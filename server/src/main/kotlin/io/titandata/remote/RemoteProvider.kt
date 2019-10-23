/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote

import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Volume
import io.titandata.operation.OperationExecutor

interface RemoteProvider {
    fun listCommits(remote: Remote, params: RemoteParameters, tags: List<String>?): List<Commit>
    fun getCommit(remote: Remote, commitId: String, params: RemoteParameters): Commit

    fun validateOperation(remote: Remote, commitId: String, opType: Operation.Type, params: RemoteParameters)

    fun startOperation(operation: OperationExecutor): Any?
    fun endOperation(operation: OperationExecutor, data: Any?)
    fun failOperation(operation: OperationExecutor, data: Any?)
    fun pushVolume(operation: OperationExecutor, data: Any?, volume: Volume, basePath: String, scratchPath: String)
    fun pullVolume(operation: OperationExecutor, data: Any?, volume: Volume, basePath: String, scratchPath: String)
    fun pushMetadata(operation: OperationExecutor, data: Any?, commit: Commit, isUpdate: Boolean)
}
