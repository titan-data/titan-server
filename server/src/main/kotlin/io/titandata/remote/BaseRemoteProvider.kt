/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote

import io.titandata.models.Commit
import io.titandata.models.Volume
import io.titandata.operation.OperationExecutor

abstract class BaseRemoteProvider : RemoteProvider {

    fun getVolumeDesc(vol: Volume): String {
        return vol.properties.get("path")?.toString() ?: vol.name
    }

    override fun startOperation(operation: OperationExecutor): Any? {
        return null
    }

    override fun endOperation(operation: OperationExecutor, data: Any?) {
        // Do nothing
    }

    override fun failOperation(operation: OperationExecutor, data: Any?) {
        // Do nothing
    }

    open fun syncVolume(operation: OperationExecutor, data: Any?, volume: Volume, path: String, scratchPath: String) {
        // Do nothing
    }

    override fun pushVolume(operation: OperationExecutor, data: Any?, volume: Volume, path: String, scratchPath: String) {
        syncVolume(operation, data, volume, path, scratchPath)
    }

    override fun pullVolume(operation: OperationExecutor, data: Any?, volume: Volume, path: String, scratchPath: String) {
        syncVolume(operation, data, volume, path, scratchPath)
    }

    override fun pushMetadata(operation: OperationExecutor, data: Any?, commit: Commit, isUpdate: Boolean) {
        // Do nothing
    }
}
