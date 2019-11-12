package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.models.docker.DockerVolume
import org.jetbrains.exposed.sql.transactions.transaction

class VolumeOrchestrator(val providers: ProviderModule) {

    /*
     * We need to create two volume definitions: one that is generic, and one that is unique to the current context.
     * Things like 'mountpoint' would only exist in the latter. For now, to minimize disruption as we go through
     * the metadata transition, we simply supplment the volume definition we get from the storage provider.
     */
    fun convertVolume(repo: String, volumeSet: String, volume: DockerVolume): DockerVolume {
        return DockerVolume(
                name = "$repo/${volume.name}",
                properties = volume.properties,
                mountpoint = providers.storage.getVolumeMountpoint(volumeSet, volume.name),
                status = mapOf<String, Any>()
        )
    }

    fun createVolume(repo: String, name: String, properties: Map<String, Any>): DockerVolume {
        NameUtil.validateVolumeName(name)
        providers.repositories.getRepository(repo)

        val vol = DockerVolume(name = name, properties = properties)
        val vs = transaction {
            val vs = providers.metadata.getActiveVolumeSet(repo)
            providers.metadata.createVolume(vs, vol)
            vs
        }
        providers.storage.createVolume(vs, vol.name)
        return convertVolume(repo, vs, vol)
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

        val (vs, vol) = transaction {
            val vs = providers.metadata.getActiveVolumeSet(repo)
            Pair(vs, providers.metadata.getVolume(vs, name))
        }
        return convertVolume(repo, vs, vol)
    }

    fun mountVolume(repo: String, name: String) {
        NameUtil.validateVolumeName(name)
        providers.repositories.getRepository(repo)

        val vs = transaction {
            val vs = providers.metadata.getActiveVolumeSet(repo)
            providers.metadata.getVolume(vs, name)
            vs
        }
        providers.storage.mountVolume(vs, name)
    }

    fun unmountVolume(repo: String, name: String) {
        NameUtil.validateVolumeName(name)
        providers.repositories.getRepository(repo)
        val vs = transaction {
            val vs = providers.metadata.getActiveVolumeSet(repo)
            providers.metadata.getVolume(vs, name)
            vs
        }
        providers.storage.unmountVolume(vs, name)
    }

    fun listAllVolumes(): List<DockerVolume> {
        return transaction {
            val result = mutableListOf<DockerVolume>()
            for (repo in providers.metadata.listRepositories()) {
                val vs = providers.metadata.getActiveVolumeSet(repo.name)
                result.addAll(providers.metadata.listVolumes(vs).map { convertVolume(repo.name, vs, it) })
            }
            result
        }
    }
}
