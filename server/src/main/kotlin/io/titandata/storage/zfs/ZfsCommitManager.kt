/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.storage.zfs

import io.titandata.exception.CommandException
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Commit
import io.titandata.models.CommitStatus
import io.titandata.util.TagFilter
import java.time.Instant
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

/**
 * Commits are really just recursive snapshots of a ZFS dataset tree. Because each repository
 * can have multiple GUIDs within it from various checkout, pull, or push operations, the
 * set of commits is really the union of all snapshots one level beneath the repository. For
 * example, you could have:
 *
 *      pool/repo/name/guid1@hash1
 *      pool/repo/name/guid1@hash2
 *      pool/repo/name/guid2@hash3
 *
 * And to the consumer, this would look like simply three commits (hash1, hash2, and hash3)
 * within the repo. The association between commits and repo GUID datasets is entirely hidden
 * within the provider.
 *
 * The result is not particularly efficient, requiring listing all snapshots beneath the repo and
 * then finding the one that matches the given a commit id. In the event we support repos with
 * large numbers of commits, this may need to be refactored to include a cache or other mechanism
 * to accelerate calls.
 */
class ZfsCommitManager(val provider: ZfsStorageProvider) {

    /**
     * Get additional status about a commit, in this case the commit size. We map the following values to their
     * ZFS counterparts:
     *
     *      logicalSize -> logicalreferenced     The amount of data referenced by the snapshot, independent of
     *                                           compression.
     *
     *      actualSize -> referenced             The amount of compressed data reference by the snapshot. This data
     *                                           could be shared with other ZFS snapshots or datasets.
     *
     *      uniqueSize -> used                   The amount of data uniquely held by this snapshot. This is the
     *                                           amount of space that would be recovered should the commit be deleted.
     *
     * Since our commits are recursive snaphots, we have to sum up the space ourselves.
     */
    fun getCommitStatus(repo: String, volumeSet: String, id: String): CommitStatus {
        val output = provider.executor.exec("zfs", "list", "-Hpo", "name,logicalreferenced,referenced,used", "-t",
                "snapshot", "-r", "$poolName/repo/$repo/$volumeSet")

        var logicalSize = 0L
        var actualSize = 0L
        var uniqueSize = 0L

        val regex = "^$poolName/repo/$repo/$volumeSet/.*@$id\t(.*)\t(.*)\t(.*)$".toRegex(RegexOption.MULTILINE)
        for (line in output.lines()) {
            val result = regex.find(line) ?: continue
            logicalSize += result.groupValues.get(1).toLong()
            actualSize += result.groupValues.get(2).toLong()
            uniqueSize += result.groupValues.get(3).toLong()
        }

        return CommitStatus(logicalSize = logicalSize, actualSize = actualSize, uniqueSize = uniqueSize)
    }

    /**
     * Checkout the given commit. This will do the following:
     *
     *  1. Find the source guid for the given commit
     *  2. Clone the dataset and all volumes into a new GUID
     *  3. Set the active guid for the repo to point to the new clone
     *  4. Cleanup the previous active guid
     *
     * To cleanup the guid we do one of two things. First, if the guid has no active snapshots, then we can simply
     * blow it away. Otherwise, we rollback to the most recent commit as we no longer need the head filesystem data.
     */
    fun checkoutCommit(repo: String, prevVolumeSet: String, newVolumeSet: String, commitVolumeSet: String, commit: String) {
        provider.cloneCommit(repo, commitVolumeSet, commit, newVolumeSet)

        val snap = provider.getLatestSnapshot("$poolName/repo/$repo/$prevVolumeSet")

        if (snap == null) {
            // If there were no active snapshots (and hence commits) on the previous active GUID, destroy it now.
            provider.executor.exec("zfs", "destroy", "-r", "$poolName/repo/$repo/$prevVolumeSet")
        } else {
            // Now iterate over all children and rollback
            val datasets = provider.executor.exec("zfs", "list", "-Ho", "name", "-r", "$poolName/repo/$repo/$prevVolumeSet")
            for (dataset in datasets.split("\n")) {
                if (dataset.trim() != "") {
                    provider.executor.exec("zfs", "rollback", "${dataset.trim()}@$snap")
                }
            }
        }
    }
}
