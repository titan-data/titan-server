/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.storage.zfs

import io.titandata.exception.CommandException
import io.titandata.models.Volume

/**
 * Every repo can have any number of named volumes beneath it. These volumes must be created
 * and destroyed prior to any commits or other operations. Removing a volume during the lifetime
 * of a repository will have undefined behavior.
 */
class ZfsVolumeManager(val provider: ZfsStorageProvider) {

    private val INITIAL_COMMIT = provider.INITIAL_COMMIT
    private val METADATA_PROP = provider.METADATA_PROP
    private val poolName = provider.poolName

    /**
     * Helper method to do common validation and return guid.
     */
    fun validate(repo: String, name: String) {
        provider.validateRepositoryName(repo)
        provider.validateVolumeName(name)
    }

    /**
     * Create a new volume. This will simply create a dataset with the given name within the
     * current active dataset, storing any metadata properties along with it.
     */
    fun createVolume(repo: String, volumeSet: String, volume: Volume) {
        validate(repo, volume.name)

        try {
            val json = provider.gson.toJson(volume.properties)
            provider.executor.exec("zfs", "create", "-o", "$METADATA_PROP=$json",
                    "$poolName/repo/$repo/$volumeSet/${volume.name}")
            // Always create an initial snapshot (see createRepository for why)
            provider.executor.exec("zfs", "snapshot",
                    "$poolName/repo/$repo/$volumeSet/${volume.name}@$INITIAL_COMMIT")
            // Create the mountpoint
            val mountpoint = provider.getMountpoint(repo, volume.name)
            provider.executor.exec("mkdir", "-p", mountpoint)
        } catch (e: CommandException) {
            provider.checkVolumeExists(e, volume.name)
            throw e
        }
    }

    /**
     * Delete a volume. This should only be called when the volume has been unmounted, and the
     * repository is about to be destroyed. It is invalid to continue to use a repository
     * that has had volumes removed in the middle of its lifecycle.
     */
    fun deleteVolume(repo: String, volumeSet: String, name: String) {
        validate(repo, name)

        try {
            provider.executor.exec("zfs", "destroy", "-R",
                    "$poolName/repo/$repo/$volumeSet/$name")
            provider.executor.exec("rmdir", provider.getMountpoint(repo, name))
        } catch (e: CommandException) {
            provider.checkNoSuchVolume(e, name)
            throw e
        }
    }

    /**
     * Mount a volume. This is always mounted in a predictable location, independent of what
     * GUID may be active.
     */
    fun mountVolume(repo: String, volumeSet: String, volume: Volume) {
        validate(repo, volume.name)
        provider.executor.exec("mount", "-t", "zfs", "$poolName/repo/$repo/$volumeSet/${volume.name}",
                provider.getMountpoint(repo, volume.name))
    }

    /**
     * Unmount a volume. We treat the operation as idempotent, ignoring errors if the filesystem
     * is not currently mounted. This insulates us against potential inconsistencies between
     * the CLI and server.
     */
    fun unmountVolume(repo: String, name: String) {
        validate(repo, name)
        provider.safeUnmount(provider.getMountpoint(repo, name))
    }
}
