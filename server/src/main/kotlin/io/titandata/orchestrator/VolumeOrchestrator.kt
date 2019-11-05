package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.models.Volume
import org.jetbrains.exposed.sql.transactions.transaction

class VolumeOrchestrator(val providers: ProviderModule) {

    private fun getVolumeSet(repo: String): String {
        return transaction {
            providers.metadata.getActiveVolumeSet(repo)
        }
    }

    /*
     * We need to create two volume definitions: one that is generic, and one that is unique to the current context.
     * Things like 'mountpoint' would only exist in the latter. For now, to minimize disruption as we go through
     * the metadata transition, we simply supplment the volume definition we get from the storage provider.
     */
    fun convertVolume(repo: String, volume: Volume) : Volume {
        return Volume(
                name = "$repo/${volume.name}",
                properties = volume.properties,
                mountpoint = providers.storage.getVolumeMountpoint(repo, volume.name),
                status = mapOf<String, Any>()
        )
    }

    fun createVolume(repo: String, name: String, properties: Map<String, Any>): Volume {
        val vol = Volume(name = name, properties = properties)
        val vs = transaction {
            val vs = providers.metadata.getActiveVolumeSet(repo)
            providers.metadata.createVolume(vs, vol)
            vs
        }
        providers.storage.createVolume(repo, vs, vol)
        return convertVolume(repo, vol)
    }

    fun deleteVolume(repo: String, name: String) {
        transaction {
            val vs = providers.metadata.getActiveVolumeSet(repo)
            providers.metadata.deleteVolume(vs, name)
        }
        providers.storage.deleteVolume(repo, getVolumeSet(repo), name)
    }

    fun getVolume(repo: String, name: String): Volume {
        val vol = transaction {
            val vs = providers.metadata.getActiveVolumeSet(repo)
            providers.metadata.getVolume(vs, name)
        }
        return convertVolume(repo, vol)
    }

    fun mountVolume(repo: String, name: String) {
        val vs = transaction {
            providers.metadata.getActiveVolumeSet(repo)
        }
        val vol = transaction {
            providers.metadata.getVolume(vs, name)
        }
        providers.storage.mountVolume(repo, vs, vol)
    }

    fun unmountVolume(repo: String, name: String) {
        providers.storage.unmountVolume(repo, name)
    }

    fun listVolumes(repo: String) : List<Volume> {
        return transaction {
            val vs = providers.metadata.getActiveVolumeSet(repo)
            providers.metadata.listVolumes(vs).map { convertVolume(repo, it) }
        }
    }

    fun listAllVolumes(): List<Volume> {
        return transaction {
            val result = mutableListOf<Volume>()
            for (repo in providers.metadata.listRepositories()) {
                val vs = providers.metadata.getActiveVolumeSet(repo.name)
                result.addAll(providers.metadata.listVolumes(vs).map { convertVolume(repo.name, it) })
            }
            result
        }
    }
}
