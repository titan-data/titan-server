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
    fun convertVolume(repo: String, volume: Volume): Volume {
        return Volume(
                name = "$repo/${volume.name}",
                properties = volume.properties,
                mountpoint = providers.storage.getVolumeMountpoint(repo, volume.name),
                status = mapOf<String, Any>()
        )
    }

    fun createVolume(repo: String, name: String, properties: Map<String, Any>): Volume {
        NameUtil.validateRepoName(repo)
        NameUtil.validateVolumeName(name)

        val vol = Volume(name = name, properties = properties)
        val vs = transaction {
            val vs = providers.metadata.getActiveVolumeSet(repo)
            providers.metadata.createVolume(vs, vol)
            vs
        }
        providers.storage.createVolume(vs, vol.name)
        return convertVolume(repo, vol)
    }

    fun deleteVolume(repo: String, name: String) {
        NameUtil.validateRepoName(repo)
        NameUtil.validateVolumeName(name)

        transaction {
            val vs = providers.metadata.getActiveVolumeSet(repo)
            providers.metadata.markVolumeDeleting(vs, name)
        }
        providers.reaper.signal()
    }

    fun getVolume(repo: String, name: String): Volume {
        NameUtil.validateRepoName(repo)
        NameUtil.validateVolumeName(name)
        val vol = transaction {
            val vs = providers.metadata.getActiveVolumeSet(repo)
            providers.metadata.getVolume(vs, name)
        }
        return convertVolume(repo, vol)
    }

    fun mountVolume(repo: String, name: String) {
        NameUtil.validateRepoName(repo)
        NameUtil.validateVolumeName(name)
        val (vs, vol) = transaction {
            val vs = providers.metadata.getActiveVolumeSet(repo)
            Pair(vs, providers.metadata.getVolume(vs, name))
        }
        providers.storage.mountVolume(vs, vol.name)
    }

    fun unmountVolume(repo: String, name: String) {
        NameUtil.validateRepoName(repo)
        NameUtil.validateVolumeName(name)
        providers.storage.unmountVolume(repo, name)
    }

    fun listVolumes(repo: String): List<Volume> {
        NameUtil.validateRepoName(repo)
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
