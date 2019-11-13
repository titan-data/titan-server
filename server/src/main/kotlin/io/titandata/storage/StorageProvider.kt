/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.storage

import io.titandata.models.CommitStatus
import io.titandata.models.RepositoryVolumeStatus

interface StorageProvider {
    fun createVolumeSet(volumeSet: String)
    fun cloneVolumeSet(sourceVolumeSet: String, sourceCommit: String, newVolumeSet: String)
    fun deleteVolumeSet(volumeSet: String)
    fun getVolumeStatus(volumeSet: String, volume: String): RepositoryVolumeStatus

    fun createCommit(volumeSet: String, commitId: String, volumeNames: List<String>)
    fun getCommitStatus(volumeSet: String, commitId: String, volumeNames: List<String>): CommitStatus
    fun deleteCommit(volumeSet: String, commitId: String, volumeNames: List<String>)

    fun createVolume(volumeSet: String, volumeName: String): Map<String, Any>
    fun cloneVolume(sourceVolumeSet: String, sourceCommit: String, newVolumeSet: String, volumeName: String): Map<String, Any>
    fun deleteVolume(volumeSet: String, volumeName: String, config: Map<String, Any>)
    fun activateVolume(volumeSet: String, volumeName: String, config: Map<String, Any>)
    fun deactivateVolume(volumeSet: String, volumeName: String, config: Map<String, Any>)
}
