/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.storage.zfs

import io.titandata.exception.CommandException
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Commit
import io.titandata.models.CommitStatus
import java.time.Instant
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

    private val poolName = provider.poolName
    private val timestampProperty = provider.timestampProperty
    private val METADATA_PROP = provider.METADATA_PROP
    private val ACTIVE_PROP = provider.ACTIVE_PROP
    private val INITIAL_COMMIT = provider.INITIAL_COMMIT

    /**
     * Create a new commit via ZFS snapshot. Both the commit ID and properties must be specified.
     * This will automatically set the 'timestampProperty' in the metadata to the creation time
     * of the snapshot, ensuring the most accurate representation of the commit timestamp. It
     * will return a copy of the Commit object with this new property set.
     */
    fun createCommit(repo: String, commit: Commit): Commit {
        provider.validateRepositoryName(repo)
        provider.validateCommitName(commit.id)

        val active = provider.getActive(repo)
        val json = provider.gson.toJson(commit.properties)
        val dataset = "$poolName/repo/$repo/$active@${commit.id}"

        // Check to see if the commit exists, even on a different active dataset
        if (provider.getCommitGuid(repo, commit.id) != null) {
            throw ObjectExistsException("commit '${commit.id}' already exists")
        }

        try {
            provider.executor.exec("zfs", "snapshot", "-r", "-o", "$METADATA_PROP=$json", dataset)

            // Go back and fetch the creation time, adding it to the new object
            val creation = provider.executor.exec("zfs", "list", "-Hpo", "creation", dataset).trim()
            val ts = DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochSecond(creation.toLong()))

            val newProperties = HashMap(commit.properties)
            newProperties[timestampProperty] = ts
            val newJson = provider.gson.toJson(newProperties)

            // And now set the metadata again, with the new timestamp property included
            provider.executor.exec("zfs", "set", "$METADATA_PROP=$newJson", dataset)

            return Commit(id = commit.id, properties = newProperties)
        } catch (e: CommandException) {
            // getActive() will have already detected if the repo exists, so this error must be
            // a missing commit
            provider.checkCommitExists(e, commit.id)
            throw e
        }
    }

    /**
     * Fetch a single commit. Uses getCommitGuid() to determine the guid of the dataset of which
     * its a part, and then gets the metadata associated with it.
     */
    fun getCommit(repo: String, id: String): Commit {
        val guid = provider.getCommitGuid(repo, id)
        guid ?: throw NoSuchObjectException("no such commit '$id' in repository '$repo'")
        val output = provider.executor.exec("zfs", "list", "-Ho",
                "io.titan-data:metadata", "$poolName/repo/$repo/$guid@$id")
        val deferDestroy = output.substringBefore("\t")
        if (deferDestroy == "on") {
            throw NoSuchObjectException("")
        }
        return Commit(id = id, properties = provider.parseMetadata(output))
    }

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
    fun getCommitStatus(repo: String, id: String): CommitStatus {
        val guid = provider.getCommitGuid(repo, id)
        guid ?: throw NoSuchObjectException("no such commit '$id' in repository '$repo'")

        val output = provider.executor.exec("zfs", "list", "-Hpo", "name,logicalreferenced,referenced,used", "-t",
                "snapshot", "-r", "$poolName/repo/$repo/$guid")

        var logicalSize = 0L
        var actualSize = 0L
        var uniqueSize = 0L

        val regex = "^$poolName/repo/$repo/$guid/.*@$id\t(.*)\t(.*)\t(.*)$".toRegex(RegexOption.MULTILINE)
        for (line in output.lines()) {
            val result = regex.find(line) ?: continue
            logicalSize += result.groupValues.get(1).toLong()
            actualSize += result.groupValues.get(2).toLong()
            uniqueSize += result.groupValues.get(3).toLong()
        }

        return CommitStatus(logicalSize = logicalSize, actualSize = actualSize, uniqueSize = uniqueSize)
    }

    /**
     * Parse the name and metadata for a commit, based on 'zfs list' output. Similar to
     * parseRepository(), this is invoked when listing commits. It assumes that there
     * are only two fields being listed, the name and the metadata. It will parse the metadata as
     * JSON, throwing an error if it's in an invalid format. If 'defer_destroy' is set, then it
     * means that the snapshot was destroyed while clones were active, and we should ignore it
     * in the commit log.
     */
    fun parseCommit(line: String): Commit? {
        // Metadata must be last since it can contain tabs
        val regex = "^$poolName/repo/[^/]+/[^/]+@([^\t]+)\toff\t(.*)$".toRegex(RegexOption.MULTILINE)
        val result = regex.find(line) ?: return null
        val id = result.groupValues.get(1)
        val metadata = result.groupValues.get(2)
        return Commit(id = id, properties = provider.parseMetadata(metadata))
    }

    /**
     * List all commits. This operates by listing all snapshots beneath a repo, along with metadata.
     * It then invokes parseCommit() to identify only those that have the right number of
     * components (since the "-d" option just limits the depth, and will include the root of the
     * list command as well.
     */
    fun listCommits(repo: String): List<Commit> {
        provider.validateRepositoryName(repo)
        try {
            val output = provider.executor.exec("zfs", "list", "-Ho",
                    "name,defer_destroy,$METADATA_PROP", "-t", "snapshot", "-d", "2",
                    "$poolName/repo/$repo")

            val commits = mutableListOf<Commit>()
            for (line in output.lines()) {
                if (line != "") {
                    val commit = parseCommit(line)
                    if (commit != null && commit.id != INITIAL_COMMIT) {
                        commits.add(commit)
                    }
                }
            }
            return commits
        } catch (e: CommandException) {
            provider.checkNoSuchRepository(e, repo)
            throw e
        }
    }

    /**
     * Delete a commit. Unlike git, our commit have no dependency on ordering, and each can be
     * pulled or pushed independently. While they may share storage, they can be independently
     * discarded to free up storage.
     */
    fun deleteCommit(repo: String, commit: String) {
        provider.validateRepositoryName(repo)
        provider.validateCommitName(commit)
        val guid = provider.getCommitGuid(repo, commit)
        guid ?: throw NoSuchObjectException("no such commit '$commit' in repository '$repo'")

        provider.executor.exec("zfs", "destroy", "-rd", "$poolName/repo/$repo/$guid@$commit")

        // If there are no more commits for this GUID, and this is not the active GUID for
        // the repo, then delete the entire GUID.
        val active = provider.getActive(repo)
        if (active != guid) {
            val output = provider.executor.exec("zfs", "list", "-H", "-t", "snapshot", "-d", "1",
                    "$poolName/repo/$repo/$guid").trim()
            if (output == "") {
                // There were no snapshots, so we can destroy the entire guid. We should be able
                // to use '-r' or '-R' since there are no snapshots, but we use '-r' just to be
                // safe and not accidentally destroy clones should something go horribly wrong.
                provider.executor.exec("zfs", "destroy", "-r", "$poolName/repo/$repo/$guid")
            }
        }
    }

    /**
     * Checkout the given commit. This will do the following:
     *
     *  1. Find the source guid for the given commit
     *  2. Clone the dataset and all volumes into a new GUID
     *  3. Set the active guid for the repo to point to the new clone
     *
     * The exception is if the hash is the latest snapshot for the given GUID. If so, we will
     * rollback to the previous state instead of cloning a new copy. This keeps storage sprawl
     * down for cases where the user is repeatedly checking out a previous commit and not
     * making any subsequent commits.
     *
     * TODO rollback the previous filesystem to discard any head state
     */
    fun checkoutCommit(repo: String, commit: String) {
        provider.validateRepositoryName(repo)
        provider.validateCommitName(commit)
        val guid = provider.getCommitGuid(repo, commit)
        guid ?: throw NoSuchObjectException("no such commit '$commit' in repository '$repo'")

        val newGuid = provider.generator.get()

        // TODO roll back if this is the last snapshot of a guid
        provider.cloneCommit(repo, guid, commit, newGuid)
        provider.executor.exec("zfs", "set", "$ACTIVE_PROP=$newGuid", "$poolName/repo/$repo")
    }
}
