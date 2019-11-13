/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.storage.zfs

import io.titandata.exception.CommandException
import io.titandata.models.CommitStatus
import io.titandata.models.RepositoryVolumeStatus
import io.titandata.storage.StorageProvider
import io.titandata.util.CommandExecutor
import org.slf4j.LoggerFactory

/**
 * ZFS Storage provider. This implements all of the local repository operations on top of a ZFS
 * storage pool. The layout is pretty straightforward:
 *
 *      pool
 *      pool/data                       Namespace for all volumesets
 *      pool/data/volumeSet             Collection of volumes
 *      pool/data/volumeSet@commit      Recursive snapshot of a particular commit
 *      pool/data/volumeSet/volume      Volume within a volume set
 */
class ZfsStorageProvider(
    val poolName: String = "titan"
) : StorageProvider {

    companion object {
        val log = LoggerFactory.getLogger(ZfsStorageProvider::class.java)
    }

    internal val executor = CommandExecutor()

    /**
     * Create a new volume set. This is simply an empty placeholder volumeset.
     */
    override fun createVolumeSet(volumeSet: String) {
        executor.exec("zfs", "create", "$poolName/data/$volumeSet")
    }

    /**
     * Clone a volumeset from an existing commit. We create the plain dataset that will contain the volumes.
     */
    override fun cloneVolumeSet(
        sourceVolumeSet: String,
        sourceCommit: String,
        newVolumeSet: String
    ) {
        createVolumeSet(newVolumeSet)
    }

    /**
     * Clone an individual volume. Here's where we actually do the work of cloning the volume, and then making sure
     * we pass back an updated config with the proper mountpoint.
     */
    override fun cloneVolume(sourceVolumeSet: String, sourceCommit: String, newVolumeSet: String, volumeName: String): Map<String, Any> {
        executor.exec("zfs", "clone", "$poolName/data/$sourceVolumeSet/$volumeName@$sourceCommit",
                "$poolName/data/$newVolumeSet/$volumeName")
        return mapOf("mountpoint" to "/var/lib/$poolName/mnt/$newVolumeSet/$volumeName")
    }

    /**
     * Delete a volume set. This should only be invoked from the reaper after all volumes have been deleted.
     */
    override fun deleteVolumeSet(volumeSet: String) {
        try {
            executor.exec("zfs", "destroy", "$poolName/data/$volumeSet")
        } catch (e: CommandException) {
            // Deletion should be idempotent, ignore errors if this object doens't exist
            if (!e.output.contains("dataset does not exist")) {
                throw e
            }
        }

        // Try to delete the directory, but it may not exist if no volumes have been created
        try {
            executor.exec("rm", "-rf", "/var/lib/$poolName/mnt/$volumeSet")
        } catch (e: CommandException) {
            if (!e.output.contains("No such file or directory")) {
                throw e
            }
        }
    }

    override fun getVolumeStatus(volumeSet: String, volume: String): RepositoryVolumeStatus {
        val output = executor.exec("zfs", "list", "-pHo",
                "logicalreferenced,referenced", "$poolName/data/$volumeSet/$volume")
        val regex = "^([^\t]+)\t([^\t]+)$".toRegex()
        val result = regex.find(output.trim())
        val volumeLogical = result!!.groupValues.get(1).toLong()
        val volumeActual = result.groupValues.get(2).toLong()
        return RepositoryVolumeStatus(
                name = volume,
                logicalSize = volumeLogical,
                actualSize = volumeActual
        )
    }

    /**
     * Create a new commit. Since we keep all volumes underneath the volume set, we can just do a recursive snapshot
     * of the set.
     */
    override fun createCommit(volumeSet: String, commitId: String, volumeNames: List<String>) {
        executor.exec("zfs", "snapshot", "-r", "$poolName/data/$volumeSet@$commitId")
    }

    /**
     * Fetch size information about a commit.
     */
    override fun getCommitStatus(volumeSet: String, commitId: String, volumeNames: List<String>): CommitStatus {
        var logicalSize = 0L
        var actualSize = 0L
        var uniqueSize = 0L

        for (volume in volumeNames) {
            val output = executor.exec("zfs", "list", "-Hpo", "logicalreferenced,referenced,used", "-t",
                    "snapshot", "$poolName/data/$volumeSet/$volume@$commitId")
            val regex = "^(.*)\t(.*)\t(.*)$".toRegex(RegexOption.MULTILINE)
            val result = regex.find(output) ?: continue
            logicalSize += result.groupValues.get(1).toLong()
            actualSize += result.groupValues.get(2).toLong()
            uniqueSize += result.groupValues.get(3).toLong()
        }

        return CommitStatus(logicalSize = logicalSize, actualSize = actualSize, uniqueSize = uniqueSize)
    }

    /**
     * Delete a commit. The reaper will have ensured that any clones have been deleted prior to invoking this. We
     * can just recursively delete the snapshot at the level of the volume set.
     */
    override fun deleteCommit(volumeSet: String, commitId: String, volumeNames: List<String>) {
        try {
            executor.exec("zfs", "destroy", "-r", "$poolName/data/$volumeSet@$commitId")
        } catch (e: CommandException) {
            // Deletion should be idempotent, ignore errors if this object doens't exist
            if (!e.output.contains("could not find any snapshots to destroy")) {
                throw e
            }
        }
    }

    /**
     * Create a new volume. Not much to do here, simply create a new dataset within the volume set.
     */
    override fun createVolume(volumeSet: String, volumeName: String): Map<String, Any> {
        executor.exec("zfs", "create", "$poolName/data/$volumeSet/$volumeName")
        return mapOf("mountpoint" to "/var/lib/$poolName/mnt/$volumeSet/$volumeName")
    }

    /**
     * Delete a volume. This should only be called when the volume has been unmounted, and the
     * repository is about to be destroyed. It is invalid to continue to use a repository
     * that has had volumes removed in the middle of its lifecycle.
     */
    override fun deleteVolume(volumeSet: String, volumeName: String, config: Map<String, Any>) {
        try {
            executor.exec("zfs", "destroy", "$poolName/data/$volumeSet/$volumeName")
        } catch (e: CommandException) {
            // Deletion should be idempotent, ignore errors if this object doens't exist
            if (!e.output.contains("dataset does not exist")) {
                throw e
            }
        }
        // A partially constructed volume will not have any config
        if (config.containsKey("mountpoint")) {
            executor.exec("rmdir", config["mountpoint"] as String)
        }
    }

    /**
     * For the local storage provider, activating a volume is equivalent to mountint it.
     */
    override fun activateVolume(volumeSet: String, volumeName: String, config: Map<String, Any>) {
        executor.exec("mkdir", "-p", config["mountpoint"] as String)
        executor.exec("mount", "-t", "zfs", "$poolName/data/$volumeSet/$volumeName",
                config["mountpoint"] as String)
    }

    /**
     * When unmounting volumes, we make sure that it is idempotent (ignoring cases where it's already
     * unmounted), and also take the opportunity to dump file usage (via lsof) if we get an EBUSY
     * error.
     */
    override fun deactivateVolume(volumeSet: String, volumeName: String, config: Map<String, Any>) {
        try {
            executor.exec("umount", config["mountpoint"] as String)
        } catch (e: CommandException) {
            if ("not mounted" in e.output) {
                return // Ignore
            } else if ("target is busy" in e.output) {
                try {
                    log.info(executor.exec("lsof"))
                } catch (ex: CommandException) {
                    // Ignore
                }
            }
            throw e
        }
    }
}
