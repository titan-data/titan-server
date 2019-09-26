/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote

import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Operation
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters

abstract class BaseRemoteProvider : RemoteProvider {
    override fun validateOperation(
        remote: Remote,
        commitId: String,
        opType: Operation.Type,
        params: RemoteParameters
    ) {
        if (opType == Operation.Type.PULL) {
            getCommit(remote, commitId, params)
        } else {
            try {
                getCommit(remote, commitId, params)
                throw ObjectExistsException("commit $commitId exists in remote '${remote.name}'")
            } catch (e: NoSuchObjectException) {
                // Ignore
            }
        }
    }
}
