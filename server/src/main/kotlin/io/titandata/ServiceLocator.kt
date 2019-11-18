package io.titandata

import com.google.gson.GsonBuilder
import io.titandata.context.RuntimeContext
import io.titandata.metadata.MetadataProvider
import io.titandata.orchestrator.CommitOrchestrator
import io.titandata.orchestrator.OperationOrchestrator
import io.titandata.orchestrator.Reaper
import io.titandata.orchestrator.RemoteOrchestrator
import io.titandata.orchestrator.RepositoryOrchestrator
import io.titandata.orchestrator.VolumeOrchestrator
import io.titandata.remote.RemoteServer
import java.util.ServiceLoader

class ServiceLocator(val context: RuntimeContext, inMemory: Boolean = true) {
    private val loader = ServiceLoader.load(RemoteServer::class.java)
    private val remoteProviders: MutableMap<String, RemoteServer>

    init {
        val providers = mutableMapOf<String, RemoteServer>()
        loader.forEach {
            if (it != null) {
                providers[it.getProvider()] = it
            }
        }
        remoteProviders = providers
    }

    val metadata = MetadataProvider(inMemory)
    val commits = CommitOrchestrator(this)
    val repositories = RepositoryOrchestrator(this)
    val operations = OperationOrchestrator(this)
    val volumes = VolumeOrchestrator(this)
    val remotes = RemoteOrchestrator(this)
    val reaper = Reaper(this)

    val gson = GsonBuilder().create()

    // Get a remote provider by name
    fun remoteProvider(type: String): RemoteServer {
        return remoteProviders[type]
                ?: throw IllegalArgumentException("unknown remote provider '$type'")
    }

    // For testing purposes
    fun setRemoteProvider(type: String, server: RemoteServer) {
        remoteProviders[type] = server
    }
}
