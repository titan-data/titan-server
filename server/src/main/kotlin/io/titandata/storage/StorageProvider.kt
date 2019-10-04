/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.storage

import io.titandata.models.Commit
import io.titandata.models.CommitStatus
import io.titandata.models.Operation
import io.titandata.models.Remote
import io.titandata.models.Repository
import io.titandata.models.Volume

interface StorageProvider {

    fun createRepository(repo: Repository)
    fun listRepositories(): List<Repository>
    fun getRepository(name: String): Repository
    fun updateRepository(name: String, repo: Repository)
    fun deleteRepository(name: String)

    fun getRemotes(repo: String): List<Remote>
    fun updateRemotes(repo: String, remotes: List<Remote>)

    fun createCommit(repo: String, commit: Commit): Commit
    fun getCommit(repo: String, id: String): Commit
    fun getCommitStatus(repo: String, id: String): CommitStatus
    fun listCommits(repo: String): List<Commit>
    fun deleteCommit(repo: String, commit: String)
    fun checkoutCommit(repo: String, commit: String)

    fun createOperation(repo: String, operation: OperationData, localCommit: String? = null)
    fun listOperations(repo: String): List<OperationData>
    fun getOperation(repo: String, id: String): OperationData
    fun commitOperation(repo: String, id: String, commit: Commit)
    fun discardOperation(repo: String, id: String)
    fun updateOperationState(repo: String, id: String, state: Operation.State)
    fun mountOperationVolumes(repo: String, id: String): String
    fun unmountOperationVolumes(repo: String, id: String)
    fun createOperationScratch(repo: String, id: String): String
    fun destroyOperationScratch(repo: String, id: String)

    fun createVolume(repo: String, name: String, properties: Map<String, Any>): Volume
    fun deleteVolume(repo: String, name: String)
    fun getVolume(repo: String, name: String): Volume
    fun mountVolume(repo: String, name: String): Volume
    fun unmountVolume(repo: String, name: String)
    fun listVolumes(repo: String): List<Volume>
}
