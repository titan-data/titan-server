package io.titandata.orchestrator

import io.titandata.ServiceLocator
import io.titandata.models.Volume
import io.titandata.models.VolumeStatus
import org.jetbrains.exposed.sql.transactions.transaction

class VolumeOrchestrator(val services: ServiceLocator) {

    fun createVolume(repo: String, volume: Volume): Volume {
        NameUtil.validateVolumeName(volume.name)
        services.repositories.getRepository(repo)

        val vs = transaction {
            val vs = services.metadata.getActiveVolumeSet(repo)
            services.metadata.createVolume(vs, volume)
            vs
        }
        val config = services.context.createVolume(vs, volume.name)
        return transaction {
            services.metadata.updateVolumeConfig(vs, volume.name, config)
            services.metadata.getVolume(vs, volume.name)
        }
    }

    fun deleteVolume(repo: String, name: String) {
        NameUtil.validateVolumeName(name)
        services.repositories.getRepository(repo)

        transaction {
            val vs = services.metadata.getActiveVolumeSet(repo)
            services.metadata.markVolumeDeleting(vs, name)
        }
        services.reaper.signal()
    }

    fun getVolume(repo: String, name: String): Volume {
        NameUtil.validateVolumeName(name)
        services.repositories.getRepository(repo)

        return transaction {
            val vs = services.metadata.getActiveVolumeSet(repo)
            services.metadata.getVolume(vs, name)
        }
    }

    fun getVolumeStatus(repo: String, name: String): VolumeStatus {
        val vol = getVolume(repo, name)
        val vs = transaction {
            services.metadata.getActiveVolumeSet(repo)
        }
        val rawStatus = services.context.getVolumeStatus(vs, name, vol.config)
        return VolumeStatus(
                name = vol.name,
                logicalSize = rawStatus.logicalSize,
                actualSize = rawStatus.actualSize,
                properties = vol.properties,
                ready = rawStatus.ready,
                error = rawStatus.error
        )
    }

    fun activateVolume(repo: String, name: String) {
        NameUtil.validateVolumeName(name)
        services.repositories.getRepository(repo)

        val (vs, volume) = transaction {
            val vs = services.metadata.getActiveVolumeSet(repo)
            Pair(vs, services.metadata.getVolume(vs, name))
        }
        services.context.activateVolume(vs, name, volume.config)
    }

    fun deactivateVolume(repo: String, name: String) {
        NameUtil.validateVolumeName(name)
        services.repositories.getRepository(repo)
        val (vs, volume) = transaction {
            val vs = services.metadata.getActiveVolumeSet(repo)
            Pair(vs, services.metadata.getVolume(vs, name))
        }
        services.context.deactivateVolume(vs, name, volume.config)
    }

    fun listVolumes(repo: String): List<Volume> {
        return transaction {
            val vs = services.metadata.getActiveVolumeSet(repo)
            services.metadata.listVolumes(vs)
        }
    }
}
