/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.context

import io.titandata.models.CommitStatus
import io.titandata.models.VolumeStatus

interface RuntimeContext {
    fun createVolumeSet(volumeSet: String)
    fun cloneVolumeSet(sourceVolumeSet: String, sourceCommit: String, newVolumeSet: String)
    fun deleteVolumeSet(volumeSet: String)
    fun getVolumeStatus(volumeSet: String, volume: String): VolumeStatus
    fun deleteVolumeSetCommit(volumeSet: String, commitId: String)
    fun commitVolumeSet(volumeSet: String, commitId: String)

    fun getCommitStatus(volumeSet: String, commitId: String, volumeNames: List<String>): CommitStatus

    fun createVolume(volumeSet: String, volumeName: String): Map<String, Any>
    fun cloneVolume(sourceVolumeSet: String, sourceCommit: String, newVolumeSet: String, volumeName: String, sourceConfig: Map<String, Any>): Map<String, Any>
    fun commitVolume(volumeSet: String, commitId: String, volumeName: String, config: Map<String, Any>)
    fun deleteVolume(volumeSet: String, volumeName: String, config: Map<String, Any>)
    fun activateVolume(volumeSet: String, volumeName: String, config: Map<String, Any>)
    fun deactivateVolume(volumeSet: String, volumeName: String, config: Map<String, Any>)
    fun deleteVolumeCommit(volumeSet: String, commitId: String, volumeName: String)
}
