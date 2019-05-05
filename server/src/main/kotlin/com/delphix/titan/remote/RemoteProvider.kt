/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan.remote

import com.delphix.titan.models.Commit
import com.delphix.titan.models.Operation
import com.delphix.titan.models.Remote
import com.delphix.titan.models.RemoteParameters
import com.delphix.titan.operation.OperationExecutor

interface RemoteProvider {
    fun listCommits(remote: Remote, params: RemoteParameters): List<Commit>
    fun getCommit(remote: Remote, commitId: String, params: RemoteParameters): Commit
    fun runOperation(operation: OperationExecutor)
    fun validateOperation(remote: Remote, commitId: String, opType: Operation.Type, params: RemoteParameters)
}
