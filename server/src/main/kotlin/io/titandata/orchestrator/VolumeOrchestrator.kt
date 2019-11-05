package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.models.Volume
import org.jetbrains.exposed.sql.transactions.transaction

class VolumeOrchestrator(val providers: ProviderModule) {

    private fun getVolumeSet(repo: String) : String {
        return transaction {
            providers.metadata.getActiveVolumeSet(repo)
        }
    }

    fun createVolume(repo: String, name: String, properties: Map<String, Any>): Volume {
        return providers.storage.createVolume(repo, getVolumeSet(repo), name, properties)
    }

    fun deleteVolume(repo: String, name: String) {
        providers.storage.deleteVolume(repo, getVolumeSet(repo), name)
    }

    fun getVolume(repo: String, name: String): Volume {
        return providers.storage.getVolume(repo, getVolumeSet(repo), name)
    }

    fun mountVolume(repo: String, name: String): Volume {
        return providers.storage.mountVolume(repo, getVolumeSet(repo), name)
    }

    fun unmountVolume(repo: String, name: String) {
        providers.storage.unmountVolume(repo, name)
    }

    fun listVolumes(repo: String): List<Volume> {
        return providers.storage.listVolumes(getVolumeSet(repo), repo)
    }
}
