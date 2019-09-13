/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan.remote

import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import com.delphix.titan.operation.OperationExecutor

interface RemoteProvider {
    fun listCommits(remote: Remote, params: RemoteParameters): List<Commit>
    fun getCommit(remote: Remote, commitId: String, params: RemoteParameters): Commit
    fun runOperation(operation: OperationExecutor)
    fun validateOperation(remote: Remote, commitId: String, opType: Operation.Type, params: RemoteParameters)
}
