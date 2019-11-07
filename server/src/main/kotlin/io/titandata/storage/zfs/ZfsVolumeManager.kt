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

    private val poolName = provider.poolName

    /**
     * Delete a volume. This should only be called when the volume has been unmounted, and the
     * repository is about to be destroyed. It is invalid to continue to use a repository
     * that has had volumes removed in the middle of its lifecycle.
     */
    fun deleteVolume(repo: String, volumeSet: String, name: String) {
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
     * Unmount a volume. We treat the operation as idempotent, ignoring errors if the filesystem
     * is not currently mounted. This insulates us against potential inconsistencies between
     * the CLI and server.
     */
    fun unmountVolume(repo: String, name: String) {
        provider.safeUnmount(provider.getMountpoint(repo, name))
    }
}
