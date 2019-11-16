/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote

import io.titandata.models.Commit
import io.titandata.models.Volume
import io.titandata.operation.OperationExecutor

interface RemoteProvider {
    fun startOperation(operation: OperationExecutor): Any?
    fun endOperation(operation: OperationExecutor, data: Any?)
    fun failOperation(operation: OperationExecutor, data: Any?)
    fun pushVolume(operation: OperationExecutor, data: Any?, volume: Volume, path: String, scratchPath: String)
    fun pullVolume(operation: OperationExecutor, data: Any?, volume: Volume, path: String, scratchPath: String)
    fun pushMetadata(operation: OperationExecutor, data: Any?, commit: Commit, isUpdate: Boolean)
}
