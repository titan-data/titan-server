/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.storage

import io.titandata.models.Commit
import io.titandata.models.CommitStatus
import io.titandata.models.Operation
import io.titandata.models.Repository
import io.titandata.models.RepositoryStatus
import io.titandata.models.Volume

interface StorageProvider {

    fun load()

    fun createRepository(repo: Repository, volumeSet: String)
    fun getRepositoryStatus(name: String, volumeSet: String): RepositoryStatus
    fun deleteRepository(name: String)

    fun createCommit(repo: String, volumeSet: String, commit: Commit): Commit
    fun getCommit(repo: String, id: String): Commit
    fun getCommitStatus(repo: String, id: String): CommitStatus
    fun listCommits(repo: String, tags: List<String>?): List<Commit>
    fun deleteCommit(repo: String, activeVolumeSet: String, commit: String)
    fun checkoutCommit(repo: String, prevVolumeSet: String, newVolumeSet: String, commit: String)
    fun updateCommit(repo: String, commit: Commit)

    fun createOperation(repo: String, operation: OperationData, localCommit: String? = null)
    fun listOperations(repo: String): List<OperationData>
    fun getOperation(repo: String, id: String): OperationData
    fun commitOperation(repo: String, id: String, commit: Commit)
    fun discardOperation(repo: String, id: String)
    fun updateOperationState(repo: String, id: String, state: Operation.State)
    fun mountOperationVolumes(repo: String, id: String, volumes: List<Volume>): String
    fun unmountOperationVolumes(repo: String, id: String, volumes: List<Volume>)
    fun createOperationScratch(repo: String, id: String): String
    fun destroyOperationScratch(repo: String, id: String)

    fun createVolume(repo: String, volumeSet: String, volume: Volume)
    fun deleteVolume(repo: String, volumeSet: String, name: String)
    fun getVolumeMountpoint(repo: String, volumeName: String) : String
    fun mountVolume(repo: String, volumeSet: String, volume: Volume)
    fun unmountVolume(repo: String, name: String)
}
