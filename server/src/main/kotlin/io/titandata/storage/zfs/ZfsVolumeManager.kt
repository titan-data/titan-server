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
    fun getGuid(repo: String, name: String): String {
        provider.validateRepositoryName(repo)
        provider.validateVolumeName(name)
        return provider.getActive(repo)
    }

    /**
     * Create a new volume. This will simply create a dataset with the given name within the
     * current active dataset, storing any metadata properties along with it.
     */
    fun createVolume(repo: String, name: String, properties: Map<String, Any>): Volume {
        val guid = getGuid(repo, name)

        try {
            val json = provider.gson.toJson(properties)
            provider.executor.exec("zfs", "create", "-o", "$METADATA_PROP=$json",
                    "$poolName/repo/$repo/$guid/$name")
            // Always create an initial snapshot (see createRepository for why)
            provider.executor.exec("zfs", "snapshot",
                    "$poolName/repo/$repo/$guid/$name@$INITIAL_COMMIT")
            // Create the mountpoint
            val mountpoint = provider.getMountpoint(repo, name)
            provider.executor.exec("mkdir", "-p", mountpoint)
            return Volume(name = name, properties = properties,
                    mountpoint = mountpoint, status = mapOf<String, Any>())
        } catch (e: CommandException) {
            provider.checkVolumeExists(e, name)
            throw e
        }
    }

    /**
     * Delete a volume. This should only be called when the volume has been unmounted, and the
     * repository is about to be destroyed. It is invalid to continue to use a repository
     * that has had volumes removed in the middle of its lifecycle.
     */
    fun deleteVolume(repo: String, name: String) {
        val guid = getGuid(repo, name)

        try {
            provider.executor.exec("zfs", "destroy", "-R",
                    "$poolName/repo/$repo/$guid/$name")
            provider.executor.exec("rmdir", provider.getMountpoint(repo, name))
        } catch (e: CommandException) {
            provider.checkNoSuchVolume(e, name)
            throw e
        }
    }

    /**
     * Get info about a volume. This does a lookup to make sure that it exists, and get any
     * local properties, returning the result.
     */
    fun getVolume(repo: String, name: String): Volume {
        val guid = getGuid(repo, name)

        try {
            val output = provider.executor.exec("zfs", "list", "-Ho", METADATA_PROP,
                    "$poolName/repo/$repo/$guid/$name")
            return Volume(name = name, mountpoint = provider.getMountpoint(repo, name),
                    properties = provider.parseMetadata(output), status = mapOf<String, Any>())
        } catch (e: CommandException) {
            provider.checkNoSuchVolume(e, name)
            throw e
        }
    }

    /**
     * Mount a volume. This is always mounted in a predictable location, independent of what
     * GUID may be active.
     */
    fun mountVolume(repo: String, name: String): Volume {
        val guid = getGuid(repo, name)
        val volume = getVolume(repo, name)

        provider.executor.exec("mount", "-t", "zfs", "$poolName/repo/$repo/$guid/$name",
                provider.getMountpoint(repo, name))
        return volume
    }

    /**
     * Unmount a volume. We treat the operation as idempotent, ignoring errors if the filesystem
     * is not currently mounted. This insulates us against potential inconsistencies between
     * the CLI and server.
     */
    fun unmountVolume(repo: String, name: String) {
        getGuid(repo, name)
        provider.safeUnmount(provider.getMountpoint(repo, name))
    }

    /**
     * List all volumes within a given repository. We simply get the current active dataset,
     * and then list any immediate descendants of it, along with their metadata.
     */
    fun listVolumes(repo: String): List<Volume> {
        provider.validateRepositoryName(repo)
        val guid = provider.getActive(repo)

        val output = provider.executor.exec("zfs", "list", "-Ho", "name,$METADATA_PROP",
                "-r", "$poolName/repo/$repo/$guid")
        val volumes = mutableListOf<Volume>()
        val regex = "$poolName/repo/$repo/$guid/([^/\t]+)\t(.*)$".toRegex()
        for (line in output.lines()) {
            var result = regex.find(line)
            if (result != null) {
                val volName = result.groupValues.get(1)
                val metadata = result.groupValues.get(2)
                if (!volName.startsWith("_")) {
                    volumes.add(Volume(name = volName, properties = provider.parseMetadata(metadata),
                            mountpoint = provider.getMountpoint(repo, volName),
                            status = mapOf<String, Any>()))
                }
            }
        }

        return volumes
    }
}
