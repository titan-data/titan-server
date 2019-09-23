/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.storage.zfs

import com.google.gson.JsonSyntaxException
import io.titandata.exception.CommandException
import io.titandata.exception.InvalidStateException
import io.titandata.exception.NoSuchObjectException
import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.storage.OperationData

/**
 * Start a new operation. Operations are full clones from a local commit ID (ideally one
 * with the least amount of delta). These are then used as the source (or destination) for
 * a rsync() command. When complete, they are either discarded (for a push), or snapshotted
 * as a new commit (for pull). In order to support this, all metadata about the operation
 * is stored with the operation itself, such that we can restart the titan server and
 * automatically resume any in-flight operations. To do this, we serialize the entire
 * Operation object to the 'io.titan-data:operation' property, which we can then use
 * to differentiate these datasets from other GUIDs within the repository.
 *
 * The actual storage and consumption of this information is driven through the OperationProvider
 * and OperationExecutor classes.
 */
@Suppress("UNUSED_PARAMETER")
class ZfsOperationManager(val provider: ZfsStorageProvider) {

    private val poolName = provider.poolName
    private val INITIAL_COMMIT = provider.INITIAL_COMMIT
    private val OPERATION_PROP = provider.OPERATION_PROP
    private val METADATA_PROP = provider.METADATA_PROP

    /**
     * To create an operation, we simply clone the original commit into a dataset with an ID that
     * matches that of the operation. We then serialize the operation data to a property and set
     * it on the dataset, so that we can distinguish it as an operation from other datasets
     * that might exist.
     */
    fun createOperation(repo: String, data: OperationData, localCommit: String?) {
        val commit = localCommit ?: INITIAL_COMMIT
        provider.validateName(data.operation.id, ZfsStorageProvider.ObjectType.OPERATION)
        provider.validateCommitName(data.operation.commitId)
        val guid = provider.getCommitGuid(repo, commit, true)
        guid ?: throw NoSuchObjectException("no such commit '$commit' in repository '$repo'")
        val id = data.operation.id
        provider.cloneCommit(repo, guid, commit, id)
        val json = provider.gson.toJson(data)
        provider.executor.exec("zfs", "set", "$OPERATION_PROP=$json",
                "$poolName/repo/$repo/$id")
    }

    /**
     * Parse a single line. This looks for any three-level datasets that doesn't have a "-" for
     * the operation state.
     */
    fun parseOperation(line: String): OperationData? {
        // Metadata must be last since it can contain tabs
        val regex = "^$poolName/repo/[^/]+/[^/]+\t(.*)$".toRegex(RegexOption.MULTILINE)
        val result = regex.find(line) ?: return null
        val json = result.groupValues.get(1)
        if (json == "-") {
            return null
        }
        try {
            return provider.gson.fromJson(json, OperationData::class.java)
        } catch (e: JsonSyntaxException) {
            throw InvalidStateException("operation metadata must be valid JSON")
        }
    }

    /**
     * Operations are simply all of the datasets directly beneath the repository that have
     * serialized operations data associated with them.
     */
    fun listOperations(repo: String): List<OperationData> {
        try {
            val output = provider.executor.exec("zfs", "list", "-Ho", "name,$OPERATION_PROP",
                    "-d", "1", "$poolName/repo/$repo")
            val operations = mutableListOf<OperationData>()
            for (line in output.lines()) {
                if (line != "") {
                    val operation = parseOperation(line)
                    if (operation != null) {
                        operations.add(operation)
                    }
                }
            }
            return operations
        } catch (e: CommandException) {
            provider.checkNoSuchRepository(e, repo)
            throw e
        }
    }

    fun getOperation(repo: String, id: String): OperationData {
        // Call this separately so we can distinguish no such repo from no such operation
        provider.getRepository(repo)
        try {
            val output = provider.executor.exec("zfs", "list", "-Ho", "name,$OPERATION_PROP",
                    "$poolName/repo/$repo/$id")
            return parseOperation(output) ?: throw NoSuchObjectException("no such operation '$id' in repository '$repo'")
        } catch (e: CommandException) {
            provider.checkNoSuchObject(e, id, ZfsStorageProvider.ObjectType.OPERATION)
            throw e
        }
    }

    /**
     * Commit an operation. Like a normal commit, it takes a recursive snapshot of the dataset,
     * and sets the metadata property. Unlike the normal commit process, we don't take the
     * timestamp from the created dataset, but assume that it's set in the provided commit data.
     * This will also clear the io.titan-data:operation property, at which point it will
     * stop showing up as an operation.
     */
    fun commitOperation(repo: String, id: String, commit: Commit) {
        getOperation(repo, id)
        val json = provider.gson.toJson(commit.properties)
        val dataset = "$poolName/repo/$repo/$id"

        provider.executor.exec("zfs", "snapshot", "-r", "-o", "$METADATA_PROP=$json",
                "$dataset@${commit.id}")
        provider.executor.exec("zfs", "inherit", OPERATION_PROP, dataset)
    }

    /**
     * Discard an operation. This just deletes the entire operations repository.
     */
    fun discardOperation(repo: String, id: String) {
        getOperation(repo, id)
        provider.executor.exec("zfs", "destroy", "-r", "$poolName/repo/$repo/$id")
    }

    /**
     * Update on-disk operation state. Read current metadata, modify the state, and write it
     * back.
     */
    fun updateOperationState(repo: String, id: String, state: Operation.State) {
        val op = getOperation(repo, id)
        op.operation.state = state
        val json = provider.gson.toJson(op)

        provider.executor.exec("zfs", "set", "$OPERATION_PROP=$json", "$poolName/repo/$repo/$id")
    }

    /**
     * Mount volumes. We create a mountpoint using the operation ID "/pool/<operation-id>", and
     * then mount all volumes within it based on their name ("/v0"). We return the base mountpoint
     * so that the caller can then access the data within those volumes.
     */
    fun mountOperationVolumes(repo: String, id: String): String {
        val op = getOperation(repo, id)
        // We use the operation ID as the mountpoint so it can be mounted in parallel
        val base = provider.getMountpoint(op.operation.id)
        for (v in provider.listVolumes(repo)) {
            val mountpoint = "$base/${v.name}"
            provider.executor.exec("mkdir", "-p", mountpoint)
            provider.executor.exec("mount", "-t", "zfs",
                    "$poolName/repo/$repo/${op.operation.id}/${v.name}", mountpoint)
        }
        return "$base"
    }

    /**
     * Unmount volumes.
     */
    fun unmountOperationVolumes(repo: String, id: String) {
        val op = getOperation(repo, id)
        // We use the operation ID as the mountpoint so it can be mounted in parallel
        val base = provider.getMountpoint(op.operation.id)
        for (v in provider.listVolumes(repo)) {
            val mountpoint = "$base/${v.name}"
            provider.safeUnmount(mountpoint)
        }
    }

    fun createOperationScratch(repo: String, id: String): String {
        // TODO reserve "_" in volume names
        val dataset = "$poolName/repo/$repo/$id/_scratch"
        provider.executor.exec("zfs", "create", dataset)

        val base = provider.getMountpoint(id)
        val mountpoint = "$base/_scratch"
        provider.executor.exec("mkdir", "-p", mountpoint)
        provider.executor.exec("mount", "-t", "zfs",
                "$poolName/repo/$repo/$id/_scratch", mountpoint)
        return mountpoint
    }

    fun destroyOperationScratch(repo: String, id: String) {
        val dataset = "$poolName/repo/$repo/$id/_scratch"
        val base = provider.getMountpoint(id)
        val mountpoint = "$base/_scratch"

        provider.safeUnmount(mountpoint)
        provider.executor.exec("zfs", "destroy", dataset)
    }
}
