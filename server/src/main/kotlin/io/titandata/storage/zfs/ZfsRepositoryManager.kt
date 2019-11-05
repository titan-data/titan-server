/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.storage.zfs

import io.titandata.exception.CommandException
import io.titandata.models.Repository
import io.titandata.models.RepositoryStatus
import io.titandata.models.RepositoryVolumeStatus

/**
 * All repositories are placed under the "<pool>/repo" dataset. This is done to provide additional
 * ZFS namespace for titan (such as deathrow for cleaning commits). Within each repo, there can be
 * any number of child datasets, each named by a GUID, that can contain snapshots. These are
 * known as "volumesets" managed by the orchestrator layer. Within each volumeset there can be any
 * number of named volumes, and snapshots are taken at the root of each repo dataset. The result is a
 * hierarchy as such:
 *
 *      (pool)
 *      (pool)/repo
 *      (pool)/repo/(name)
 *      (pool)/repo/(name)/(guid)
 *      (pool)/repo/(name)/(guid)@(commit)
 *      (pool)/repo/(name)/(guid)/(volume)@(commit)
 *
 * With the exception of repo-wide operations (create, delete, log, etc), every operation will
 * first look up the current active dataset, and then perform the requisite actions (snapshot,
 * mount, unmount, etc) on that dataset.
 *
 * The storage provider also provides metadata storage by setting JSON string properties at both
 * the repo and snapshot level. This metadata is entirely under the control of the consumer,
 * with the exception of the 'timestampProperty' parameter. When creating a new commit, the
 * provider will fill in the timestamp with the exact creation time to ensure the most
 * accurate timestamp. This is stored in metadata because when we pull a commit down from a remote
 * server, we want to use the timestamp in the metadata, not when the snapshot was created.
 */
class ZfsRepositoryManager(val provider: ZfsStorageProvider) {

    private val poolName = provider.poolName
    private val METADATA_PROP = provider.METADATA_PROP
    private val INITIAL_COMMIT = provider.INITIAL_COMMIT

    /**
     * Parse the name and metadata for a repository, based on 'zfs list' output. This is invoked
     * when listing and fetching repositories. It assumes that there are only two fields being
     * listed, the name and the metadata. It will parse the metadata as JSON, throwing an error
     * if it's in an invalid format.
     *
     * To keep the listRepositories() code simple, this returns null when given just a pool name.
     * This is because 'zfs list -d 1' just limits the depth, and will still print the dataset
     * itself (the pool in this case).
     */
    fun parseRepository(line: String): Repository? {
        // Metadata must be last since it can contain tabs
        val regex = "^$poolName/repo/([^/\t]+)\t(.*)$".toRegex(RegexOption.MULTILINE)
        val result = regex.find(line) ?: return null
        val name = result.groupValues.get(1)
        val metadata = result.groupValues.get(2)

        return Repository(name = name, properties = provider.parseMetadata(metadata))
    }

    /**
     * Create a new repository. This involves three steps:
     *
     *  1. Create a new root repo dataset
     *  2. Create a new dataset with the given volumeSet GUID beneath the root repo
     *  3. Create a default snapshot named 'initial'
     *
     * The third step simplifies subsequent operations by always having a point to clone from,
     * even when no commits exist, so that we don't have to special case creating new datsets.
     */
    fun createRepository(repo: Repository, volumeSet: String) {
        provider.validateRepositoryName(repo.name)

        try {
            val json = provider.gson.toJson(repo.properties)
            val name = repo.name
            provider.executor.exec("zfs", "create", "-o", "mountpoint=legacy",
                    "-o", "$METADATA_PROP=$json", "$poolName/repo/$name")
            provider.executor.exec("zfs", "create", "$poolName/repo/$name/$volumeSet")

            // We always create a snapshot knows as "initial" that can be used to clone an empty
            // dataset. This keeps other code simple to avoid having to special-case an initial
            // clone
            provider.executor.exec("zfs", "snapshot", "-o", "$METADATA_PROP={}",
                    "$poolName/repo/$name/$volumeSet@$INITIAL_COMMIT")
        } catch (e: CommandException) {
            provider.checkRepositoryExists(e, repo.name)
        }
    }

    /**
     * Get the status of a repository. This fetches a few additional pieces of information about
     * the repository:
     *
     *      logicalSize     Equivalent to 'logicalUsed'. This includes the size of any and all snapshots.
     *
     *      actutalSize     Equivalent to 'used'. Includes the size of any and all snapshots.
     *
     *      checkedOutFrom  If this is a clone, then this is the commit portion of the clone origin.
     *
     *      lastCommit      Last committed change. This may or may not be the same as the last commit on the current
     *                      head filesystem, so we need to list all commits and fetch the latest one across the
     *                      entire repository.
     *
     *      volumeStatus    Status for each volume. This reports status for the current head of each volume, including:
     *
     *                      logicalSize     Equivalent to 'logicalrefererenced'. This is only the referenced data
     *                                      because it doesn't include snapshots (which maake no sense fo the
     *                                      current head filesystem.
     *
     *                      actualSize      Equivalent to 'referenced'. Like 'logicalSize', we exclude snapshots.
     *
     *                      properties      The client-controlled properties of the volume. This includes, for example,
     *                                      the path of the volume within the container.
     */
    fun getRepositoryStatus(name: String, volumeSet: String): RepositoryStatus {
        provider.validateRepositoryName(name)
        // This is not particularly efficient, but we don't have a better way
        val commits = provider.commitManager.listCommits(name, null)
        val latest = commits.getOrNull(0)?.id

        // Get the size from the dataset itself
        val fields = provider.executor.exec("zfs", "list", "-pHo", "logicalused,used",
                "$poolName/repo/$name/$volumeSet").lines().get(0).split("\t")
        val logicalSize = fields[0].toLong()
        val actualSize = fields[1].toLong()

        var sourceCommit: String? = null
        // Check to see if we have a previous snapshot on this GUID. If so, that's the source commit
        var lastSnap = provider.getLatestSnapshot("$poolName/repo/$name/$volumeSet")
        if (lastSnap != INITIAL_COMMIT) {
            sourceCommit = lastSnap
        }

        // If there are no commits on this GUID, then the source commit is our origin
        if (sourceCommit == null) {
            val origins = provider.executor.exec("zfs", "list", "-Ho", "origin", "-r",
                    "$poolName/repo/$name/$volumeSet")
            for (line in origins.lines()) {
                if (line.contains("@")) {
                    val snapshot = line.trim().substringAfterLast("@")
                    if (snapshot != INITIAL_COMMIT) {
                        sourceCommit = snapshot
                        break
                    }
                }
            }
        }

        val volumes = mutableListOf<RepositoryVolumeStatus>()
        val volumeOutput = provider.executor.exec("zfs", "list", "-d", "1", "-pHo",
                "name,logicalreferenced,referenced,$METADATA_PROP", "$poolName/repo/$name/$volumeSet")
        val regex = "^$poolName/repo/$name/$volumeSet/([^/\t]+)\t([^\t]+)\t([^\t]+)\t(.*)$".toRegex()
        for (line in volumeOutput.lines()) {
            val result = regex.find(line)
            if (result != null) {
                val volumeName = result.groupValues.get(1)
                val volumeLogical = result.groupValues.get(2).toLong()
                val volumeActual = result.groupValues.get(3).toLong()
                val metadataString = result.groupValues.get(4)
                volumes.add(RepositoryVolumeStatus(
                        name = volumeName,
                        logicalSize = volumeLogical,
                        actualSize = volumeActual,
                        properties = provider.parseMetadata(metadataString)
                ))
            }
        }

        return RepositoryStatus(
                logicalSize = logicalSize,
                actualSize = actualSize,
                sourceCommit = sourceCommit,
                lastCommit = latest,
                volumeStatus = volumes
        )
    }

    /**
     * Delete a repository. We use the '-R' flag to 'zfs destroy' to destroy all clones in the
     * appropriate order.
     */
    fun deleteRepository(name: String) {
        provider.validateRepositoryName(name)
        try {
            provider.executor.exec("zfs", "destroy", "-R", "$poolName/repo/$name")
        } catch (e: CommandException) {
            provider.checkNoSuchRepository(e, name)
            throw e
        }

        // Try to delete the directory, but it may not exist if no volumes have been created
        try {
            provider.executor.exec("rm", "-rf", provider.getMountpoint(name))
        } catch (e: CommandException) {
            if (!e.output.contains("No such file or directory")) {
                throw e
            }
        }
    }
}
