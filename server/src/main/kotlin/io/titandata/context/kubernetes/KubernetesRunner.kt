package io.titandata.context.kubernetes

import com.google.gson.GsonBuilder
import io.titandata.models.ProgressEntry
import io.titandata.remote.RemoteOperation
import io.titandata.remote.RemoteOperationType
import io.titandata.remote.RemoteProgress
import io.titandata.remote.RemoteServer
import java.io.FileReader
import java.util.ServiceLoader

/*
 * This main entry point is invoked in the context of a new pod via KubernetesCsiContext in order to perform push
 * and pull operations.
 *
 * The runner is passed the base path to the directory tree with the data and configuration. This should look like:
 *
 *      /var/titan/
 *          _secret/config      Operation configuration (serialized KubernetesParameters)
 *          _scratch/           Scratch space
 *          <volume>/           One or more volumes
 *
 * Progress reporting is sent to the console as a JSON data (KubernetesProgress). The KubernetesCsiContext driver will
 * ignore any data
 */

/**
 * This parameters object is passed (in JSON form) from the main KubernetesCsiContext to the KubernetesRunner executing
 * in a separate pod. It's basically an alternate form of the RemoteOperation data class that doesn't have the
 * unserializable callback within it.
 */
data class KubernetesOperation(
    val remoteType: String,
    val remote: Map<String, Any>,
    val parameters: Map<String, Any>,
    val operationId: String,
    val commitId: String,
    val commit: Map<String, Any>?,
    val type: RemoteOperationType,
    val scratchVolume: String,
    val volumes: List<String>,
    val volumeDescriptions: List<String>
)

class KubernetesRunner() {
    private val loader = ServiceLoader.load(RemoteServer::class.java)
    private val remoteProviders: MutableMap<String, RemoteServer>
    private val gson = GsonBuilder().create()
    var progressId = 1

    init {
        val providers = mutableMapOf<String, RemoteServer>()
        loader.forEach {
            if (it != null) {
                providers[it.getProvider()] = it
            }
        }
        remoteProviders = providers
    }

    val updateProgress = fun(type: RemoteProgress, message: String?, percent: Int?) {
        val progressType = when (type) {
            RemoteProgress.START -> ProgressEntry.Type.START
            RemoteProgress.END -> ProgressEntry.Type.END
            RemoteProgress.PROGRESS -> ProgressEntry.Type.PROGRESS
            RemoteProgress.MESSAGE -> ProgressEntry.Type.MESSAGE
        }
        val progress = ProgressEntry(id = progressId++, type = progressType, message = message, percent = percent)
        println(gson.toJson(progress))
    }

    fun runOperation(basePath: String, params: KubernetesOperation) {
        val operation = RemoteOperation(
                updateProgress = updateProgress,
                type = params.type,
                commit = params.commit,
                commitId = params.commitId,
                operationId = params.operationId,
                remote = params.remote,
                parameters = params.parameters
        )

        val remote = remoteProviders[params.remoteType] ?: error("unknown remote type '${params.remoteType}'")
        val scratchPath = params.scratchVolume
        val data = remote.syncDataStart(operation)
        var success = false
        try {
            for ((idx, volume) in params.volumes.withIndex()) {
                remote.syncDataVolume(operation, data, volume, params.volumeDescriptions[idx], "$basePath/$volume",
                        "$basePath/$scratchPath")
            }
            success = true
        } finally {
            remote.syncDataEnd(operation, data, success)
        }
    }
}

fun main(args: Array<String>) {
    val gson = GsonBuilder().create()

    val basePath = System.getProperty("basePath") ?: error("missing basePath property")
    val operation = gson.fromJson(FileReader("$basePath/_secret/config"), KubernetesOperation::class.java)

    val runner = KubernetesRunner()
    try {
        runner.runOperation(basePath, operation)
        println(gson.toJson(ProgressEntry(id = runner.progressId++, type = ProgressEntry.Type.COMPLETE)))
    } catch (t: Throwable) {
        println(gson.toJson(ProgressEntry(id = runner.progressId++, type = ProgressEntry.Type.ERROR, message = t.message)))
        throw t
    }
}
