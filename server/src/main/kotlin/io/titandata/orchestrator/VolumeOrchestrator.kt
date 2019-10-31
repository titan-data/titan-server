package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.models.Volume

class VolumeOrchestrator(val providers: ProviderModule) {

    fun createVolume(repo: String, name: String, properties: Map<String, Any>): Volume {
        return providers.storage.createVolume(repo, name, properties)
    }

    fun deleteVolume(repo: String, name: String) {
        providers.storage.deleteVolume(repo, name)
    }

    fun getVolume(repo: String, name: String): Volume {
        return providers.storage.getVolume(repo, name)
    }

    fun mountVolume(repo: String, name: String): Volume {
        return providers.storage.mountVolume(repo, name)
    }

    fun unmountVolume(repo: String, name: String) {
        providers.storage.unmountVolume(repo, name)
    }

    fun listVolumes(repo: String): List<Volume> {
        return providers.storage.listVolumes(repo)
    }
}
