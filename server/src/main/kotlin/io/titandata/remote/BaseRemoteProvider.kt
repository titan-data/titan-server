/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote

import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Volume
import io.titandata.operation.OperationExecutor

abstract class BaseRemoteProvider : RemoteProvider {
    override fun validateOperation(
        remote: Remote,
        commitId: String,
        opType: Operation.Type,
        params: RemoteParameters,
        metadataOnly: Boolean
    ) {
        if (opType == Operation.Type.PULL) {
            getCommit(remote, commitId, params)
        } else {
            try {
                getCommit(remote, commitId, params)
                if (!metadataOnly) {
                    throw ObjectExistsException("commit $commitId exists in remote '${remote.name}'")
                }
            } catch (e: NoSuchObjectException) {
                if (metadataOnly) {
                    throw e
                }
            }
        }
    }

    fun getVolumeDesc(vol: Volume): String {
        return vol.properties?.get("path")?.toString() ?: vol.name
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
