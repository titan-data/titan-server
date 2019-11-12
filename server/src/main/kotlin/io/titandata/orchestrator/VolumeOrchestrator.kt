package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.models.docker.DockerVolume
import org.jetbrains.exposed.sql.transactions.transaction

class VolumeOrchestrator(val providers: ProviderModule) {

    fun createVolume(repo: String, volume: Volume): Volume {
        NameUtil.validateVolumeName(volume.name)
        providers.repositories.getRepository(repo)

        val vs = transaction {
            val vs = providers.metadata.getActiveVolumeSet(repo)
            providers.metadata.createVolume(vs, volume)
            vs
        }
        val config = providers.storage.createVolume(vs, volume.name)
        return transaction {
            providers.metadata.updateVolumeConfig(vs, volume.name, config)
            providers.metadata.getVolume(vs, volume.name)
        }
    }

    fun deleteVolume(repo: String, name: String) {
        NameUtil.validateVolumeName(name)
        providers.repositories.getRepository(repo)

        transaction {
            val vs = providers.metadata.getActiveVolumeSet(repo)
            providers.metadata.markVolumeDeleting(vs, name)
        }
        providers.reaper.signal()
    }

    fun getVolume(repo: String, name: String): DockerVolume {
        NameUtil.validateVolumeName(name)
        providers.repositories.getRepository(repo)

        return transaction {
            val vs = providers.metadata.getActiveVolumeSet(repo)
            providers.metadata.getVolume(vs, name)
        }
    }

    fun activateVolume(repo: String, name: String) {
        NameUtil.validateVolumeName(name)
        providers.repositories.getRepository(repo)

        val (vs, volume) = transaction {
            val vs = providers.metadata.getActiveVolumeSet(repo)
            Pair(vs, providers.metadata.getVolume(vs, name))
        }
        providers.storage.activateVolume(vs, name, volume.config)
    }

    fun deactivateVolume(repo: String, name: String) {
        NameUtil.validateVolumeName(name)
        providers.repositories.getRepository(repo)
        val (vs, volume) = transaction {
            val vs = providers.metadata.getActiveVolumeSet(repo)
            Pair(vs, providers.metadata.getVolume(vs, name))
        }
        providers.storage.deactivateVolume(vs, name, volume.config)
    }

    fun listVolumes(repo: String): List<Volume> {
        return transaction {
            val vs = providers.metadata.getActiveVolumeSet(repo)
            providers.metadata.listVolumes(vs)
        }
    }
}
