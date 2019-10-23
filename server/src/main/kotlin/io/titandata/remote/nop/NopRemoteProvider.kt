/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.nop

import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Volume
import io.titandata.operation.OperationExecutor
import io.titandata.remote.BaseRemoteProvider

/**
 * The nop (No-operation) is a special provider used for internal testing to make it easier to
 * test local workflows without having to mock out an external remote provider. As its name implies,
 * this will simply ignore any operations. Pushing and pulling will always succeed, though listing
 * remotes will always return an empty list.
 */
class NopRemoteProvider : BaseRemoteProvider() {
    override fun listCommits(remote: Remote, params: RemoteParameters, tags: List<String>?): List<Commit> {
        return listOf()
    }

    override fun getCommit(remote: Remote, commitId: String, params: RemoteParameters): Commit {
        return Commit(id = commitId, properties = mapOf())
    }

    override fun validateOperation(
        remote: Remote,
        commitId: String,
        opType: Operation.Type,
        params: RemoteParameters
    ) {
        // All operations always succeed
    }

    override fun syncVolume(operation: OperationExecutor, data: Any?, volume: Volume, basePath: String, scratchPath: String) {
        operation.addProgress(ProgressEntry(ProgressEntry.Type.START, "Running operation"))
        val request = operation.params as NopParameters
        if (request.delay != 0) {
            Thread.sleep(request.delay * 1000L)
        }
        operation.addProgress(ProgressEntry(ProgressEntry.Type.END))
    }
}
