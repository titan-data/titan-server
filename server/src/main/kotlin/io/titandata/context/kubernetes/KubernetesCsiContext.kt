package io.titandata.context.kubernetes

import io.titandata.context.RuntimeContext
import io.titandata.models.CommitStatus
import io.titandata.models.RepositoryVolumeStatus

class KubernetesCsiContext : RuntimeContext {
    override fun createVolumeSet(volumeSet: String) {
        TODO("not implemented")
    }

    override fun cloneVolumeSet(sourceVolumeSet: String, sourceCommit: String, newVolumeSet: String) {
        TODO("not implemented")
    }

    override fun deleteVolumeSet(volumeSet: String) {
        TODO("not implemented")
    }

    override fun getVolumeStatus(volumeSet: String, volume: String): RepositoryVolumeStatus {
        TODO("not implemented")
    }

    override fun createCommit(volumeSet: String, commitId: String, volumeNames: List<String>) {
        TODO("not implemented")
    }

    override fun getCommitStatus(volumeSet: String, commitId: String, volumeNames: List<String>): CommitStatus {
        TODO("not implemented")
    }

    override fun deleteCommit(volumeSet: String, commitId: String, volumeNames: List<String>) {
        TODO("not implemented")
    }

    override fun createVolume(volumeSet: String, volumeName: String): Map<String, Any> {
        TODO("not implemented")
    }

    override fun cloneVolume(sourceVolumeSet: String, sourceCommit: String, newVolumeSet: String, volumeName: String): Map<String, Any> {
        TODO("not implemented")
    }

    override fun deleteVolume(volumeSet: String, volumeName: String, config: Map<String, Any>) {
        TODO("not implemented")
    }

    override fun activateVolume(volumeSet: String, volumeName: String, config: Map<String, Any>) {
        TODO("not implemented")
    }

    override fun deactivateVolume(volumeSet: String, volumeName: String, config: Map<String, Any>) {
        TODO("not implemented")
    }
}
