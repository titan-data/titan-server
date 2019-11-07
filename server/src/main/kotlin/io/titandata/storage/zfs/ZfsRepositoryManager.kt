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

    /**
     * Get the status of a repository. This fetches a few additional pieces of information about
     * the repository:
     *
     *      logicalSize     Equivalent to 'logicalUsed'. This includes the size of any and all snapshots.
     *
     *      actutalSize     Equivalent to 'used'. Includes the size of any and all snapshots.
     *
     *      sourceCommit    The commit lineage from which this commit is derived. This is either the previous commit
     *                      in the volumeSet, the origin of the volumeSet if there is no commit, or null if this is
     *                      the first commit in the repository.
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
        // Get the size from the dataset itself
        val fields = provider.executor.exec("zfs", "list", "-pHo", "logicalused,used",
                "$poolName/repo/$name/$volumeSet").lines().get(0).split("\t")
        val logicalSize = fields[0].toLong()
        val actualSize = fields[1].toLong()

        val volumes = mutableListOf<RepositoryVolumeStatus>()
        val volumeOutput = provider.executor.exec("zfs", "list", "-d", "1", "-pHo",
                "name,logicalreferenced,referenced", "$poolName/repo/$name/$volumeSet")
        val regex = "^$poolName/repo/$name/$volumeSet/([^/\t]+)\t([^\t]+)\t([^\t]+)$".toRegex()
        for (line in volumeOutput.lines()) {
            val result = regex.find(line)
            if (result != null) {
                val volumeName = result.groupValues.get(1)
                val volumeLogical = result.groupValues.get(2).toLong()
                val volumeActual = result.groupValues.get(3).toLong()
                volumes.add(RepositoryVolumeStatus(
                        name = volumeName,
                        logicalSize = volumeLogical,
                        actualSize = volumeActual
                ))
            }
        }

        return RepositoryStatus(
                logicalSize = logicalSize,
                actualSize = actualSize,
                volumeStatus = volumes
        )
    }

}
