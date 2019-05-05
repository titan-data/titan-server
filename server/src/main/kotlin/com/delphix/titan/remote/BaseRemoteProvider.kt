/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan.remote

import com.delphix.titan.exception.NoSuchObjectException
import com.delphix.titan.exception.ObjectExistsException
import com.delphix.titan.models.Operation
import com.delphix.titan.models.Remote
import com.delphix.titan.models.RemoteParameters

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
