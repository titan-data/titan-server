/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.storage.zfs

import com.google.gson.GsonBuilder
import com.google.gson.JsonSyntaxException
import com.google.gson.reflect.TypeToken
import io.titandata.exception.CommandException
import io.titandata.exception.InvalidStateException
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Commit
import io.titandata.models.CommitStatus
import io.titandata.models.Operation
import io.titandata.models.Repository
import io.titandata.models.RepositoryStatus
import io.titandata.models.RepositoryVolumeStatus
import io.titandata.models.Volume
import io.titandata.serialization.ModelTypeAdapters
import io.titandata.storage.OperationData
import io.titandata.storage.StorageProvider
import io.titandata.util.CommandExecutor
import org.slf4j.LoggerFactory

/**
 * ZFS Storage provider. This implements all of the local repository operations on top of a ZFS
 * storage pool. The layout is pretty straightforward:
 *
 *      pool
 *      pool/volumeSet              Collection of volumes
 *      pool/volumeSet@commit       Recursive snapshot of a particular commit
 *      pool/volumeSet/volume       Volume within a volume set
 *
 */
class ZfsStorageProvider(
    val poolName: String = "titan",
    val timestampProperty: String = "timestamp"
) : StorageProvider {

    companion object {
        val log = LoggerFactory.getLogger(ZfsStorageProvider::class.java)
    }

    internal val executor = CommandExecutor()

    /*
     * Create a new volume set. This is simply an empty placeholder volumeset.
     */
    override fun createVolumeSet(volumeSet: String) {
        executor.exec("zfs", "create", "$poolName/data/$volumeSet")
    }

    /*
     * Clone a volumeset from an existing commit. We create the plain volume set, and then go about cloning each
     * volume from the old volumeset into the new space.
     */
    override fun cloneVolumeSet(sourceVolumeSet: String, sourceCommit: String, newVolumeSet: String,
                                volumeNames: List<String>) {
        createVolumeSet(newVolumeSet)
        for (vol in volumeNames) {
            executor.exec("zfs", "clone", "$poolName/data/$sourceVolumeSet/$vol@$sourceCommit",
                    "$poolName/data/$newVolumeSet/$vol")

        }
    }

    /*
     * Delete a volume set. This should only be invoked from the reaper after all volumes have been deleted.
     */
    override fun deleteVolumeSet(volumeSet: String) {
        executor.exec("zfs", "destroy", "$poolName/data/$volumeSet")

        // Try to delete the directory, but it may not exist if no volumes have been created
        try {
            executor.exec("rm", "-rf", "/var/lib/$poolName/$volumeSet")
        } catch (e: CommandException) {
            if (!e.output.contains("No such file or directory")) {
                throw e
            }
        }
    }

    override fun getVolumeStatus(volumeSet: String, volume: String): RepositoryVolumeStatus {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    /*
     * Create a new commit. Since we keep all volumes underneath the volume set, we can just do a recursive snapshot
     * of the set.
     */
    override fun createCommit(volumeSet: String, commitId: String, volumeNames: List<String>) {
        executor.exec("zfs", "snapshot", "-r", "$poolName/data/$volumeSet@$commitId")
    }

    override fun getCommitStatus(volumeSet: String, commitId: String): CommitStatus {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    /*
     * Delete a commit. The reaper will have ensured that any clones have been deleted prior to invoking this. We
     * can just recursively delete the snapshot at the level of the volume set.
     */
    override fun deleteCommit(volumeSet: String, commitId: String, volumeNames: List<String>) {
        executor.exec("zfs", "destroy", "-rd", "$poolName/data/$volumeSet@$commitId")
    }

    /*
     * Create a new volume. Not much to do here, simply create a new dataset within the volume set.
     */
    override fun createVolume(volumeSet: String, volumeName: String) {
        executor.exec("zfs", "create", "$poolName/data/$volumeSet/$volumeName")
    }

    override fun deleteVolume(volumeSet: String, volumeName: String) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getVolumeMountpoint(volumeSet: String, volumeName: String): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun mountVolume(volumeSet: String, volumeName: String) {
        executor.exec("mkdir", "-p", "/var/lib/$poolName/$volumeSet/$volumeName")
        executor.exec("mount", "-t", "zfs", "$poolName/data/$volumeSet/$volumeName",
                "/var/lib/$poolName/$volumeSet/$volumeName")
    }

    /*
     * When unmounting volumes, we make sure that it is idempotent (ignoring cases where it's already
     * unmounted), and also take the opportunity to dump file usage (via lsof) if we get an EBUSY
     * error.
     */
    override fun unmountVolume(volumeSet: String, volumeName: String) {
        try {
            executor.exec("umount", "/var/lib/$poolName/$volumeSet/$volumeName")
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
