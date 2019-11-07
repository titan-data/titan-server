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

    fun createVolumeSet(volumeSet: String)
    fun cloneVolumeSet(sourceVolumeSet: String, sourceCommit: String, newVolumeSet: String)
    fun deleteVolumeSet(volumeSet: String)
    fun getVolumeStatus(volumeSet: String, volume: String)
    fun inactiveVolumeSet(volumeSet: String)

    fun createCommit(volumeSet: String, commitId: String, volumeNames : List<String>)
    fun getCommitStatus(volumeSet: String, commitId: String): CommitStatus
    fun deleteCommit(volumeSet: String, commitId: String, volumeNames: List<String>)

    fun createVolume(volumeSet: String, volumeName: Volume)
    fun deleteVolume(volumeSet: String, volumeName: String)
    fun getVolumeMountpoint(volumeSet: String, volumeName: String): String
    fun mountVolume(volumeSet: String, volumeName: Volume)
    fun unmountVolume(volumeSet: String, volumeName: String)
}
