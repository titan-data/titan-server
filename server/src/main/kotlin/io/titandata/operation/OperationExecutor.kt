/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.operation

import io.titandata.ServiceLocator
import io.titandata.metadata.table.Tags.commit
import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Volume
import io.titandata.remote.RemoteOperation
import io.titandata.remote.RemoteOperationType
import io.titandata.remote.RemoteProgress
import io.titandata.remote.RemoteServer
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory

/*
 * The operation executor is responsible for managing the asynchronous execution of a single operation. Each operation
 * is divided into a few phases:
 *
 *      1. Setup          Gather up all the necessary metdata required for the operation. The result of this stage is
 *      2. Data Sync      Prepare all the volumes and invoke the remote provider-specific functionality to push or pull
 *                        data. This invokes a context-specific method, and could spawn an entirely separate process,
 *                        such as running in s separate pod in kubernetes. This phase is skipped for metadata-only
 *                        operations.
 *      3. Metadata Sync  Sync all metadata, creating the local commit (for pull) or invoking the remote provider
 *                        to push commits. This always executes in the context of the server.
 *      4. Completion     Clean up any intermediate objects, and handle any exceptions.
 *
 * This all runs in a separate thread, so these operations can take longer periods of time.
 */
class OperationExecutor(
    val services: ServiceLocator,
    val operation: Operation,
    val repo: String,
    val remote: Remote,
    val params: RemoteParameters,
    val metadataOnly: Boolean = false,
    val completionCallback: ((operationId: String) -> Unit)? = null
) : Runnable {

    companion object {
        val log = LoggerFactory.getLogger(OperationExecutor::class.java)
    }

    internal var thread: Thread? = null

    val updateProgress = fun(type: RemoteProgress, message: String?, percent: Int?) {
        transaction {
            val progressType = when (type) {
                RemoteProgress.START -> ProgressEntry.Type.START
                RemoteProgress.END -> ProgressEntry.Type.END
                RemoteProgress.PROGRESS -> ProgressEntry.Type.PROGRESS
                RemoteProgress.MESSAGE -> ProgressEntry.Type.MESSAGE
            }
            services.metadata.addProgressEntry(operation.id, ProgressEntry(progressType, message, percent))
        }
    }

    internal fun setup(): RemoteOperation {
        log.info("starting ${operation.type} operation ${operation.id}")

        val commit = if (operation.type == Operation.Type.PUSH) {
            services.commits.getCommit(repo, operation.commitId)
        } else {
            services.remotes.getRemoteCommit(repo, remote.name, params, operation.commitId)
        }

        return RemoteOperation(
                updateProgress = updateProgress,
                remote = remote.properties,
                parameters = params.properties,
                operationId = operation.id,
                commitId = operation.commitId,
                commit = commit.properties,
                type = if (operation.type == Operation.Type.PUSH) { RemoteOperationType.PUSH } else { RemoteOperationType.PULL }
        )
    }

    internal fun syncData(provider: RemoteServer, operation: RemoteOperation) {
        val data = provider.syncDataStart(operation)

        var success = false
        try {
            val operationId = operation.operationId
            val volumes = transaction {
                val vols = services.metadata.listVolumes(operationId)
                services.metadata.createVolume(operationId, Volume("_scratch"))
                vols
            }
            val scratchConfig = services.context.createVolume(operationId, "_scratch")
            transaction {
                services.metadata.updateVolumeConfig(operationId, "_scratch", scratchConfig)
            }
            try {
                services.context.activateVolume(operationId, "_scratch", scratchConfig)
                val scratch = scratchConfig["mountpoint"] as String
                for (volume in volumes) {
                    services.context.activateVolume(operationId, volume.name, volume.config)
                    val mountpoint = volume.config["mountpoint"] as String
                    try {
                        provider.syncDataVolume(operation, data, volume.name, getVolumeDesc(volume), mountpoint, scratch)
                    } finally {
                        services.context.deactivateVolume(operationId, volume.name, volume.config)
                    }
                }
                success = true
            } finally {
                services.context.deactivateVolume(operationId, "_scratch", scratchConfig)
                services.context.deleteVolume(operationId, "_scratch", scratchConfig)
                transaction {
                    services.metadata.deleteVolume(operationId, "_scratch")
                }
            }
        } finally {
            provider.syncDataEnd(operation, data, success)
        }
    }

    internal fun syncMetadata(provider: RemoteServer, operation: RemoteOperation) {
        if (operation.type == RemoteOperationType.PUSH) {
            provider.pushMetadata(operation, operation.commit!!, metadataOnly)
        } else {
            val commit = Commit(operation.commitId, operation.commit!!)
            if (metadataOnly) {
                services.commits.updateCommit(repo, commit)
            } else {
                services.commits.createCommit(repo, commit)
            }
        }
    }

    override fun run() {
        val provider = services.remoteProvider(remote.provider)
        try {
            val operationData = setup()
            if (!metadataOnly) {
                syncData(provider, operationData)
            }
            syncMetadata(provider, operationData)
            transaction {
                services.metadata.addProgressEntry(operation.id, ProgressEntry(ProgressEntry.Type.COMPLETE))
            }
        } catch (t: InterruptedException) {
            transaction {
                services.metadata.addProgressEntry(operation.id, ProgressEntry(ProgressEntry.Type.ABORT))
            }
            log.info("${operation.type} operation ${operation.id} interrupted", t)
        } catch (t: Throwable) {
            transaction {
                services.metadata.addProgressEntry(operation.id, ProgressEntry(ProgressEntry.Type.FAILED, t.message))
            }
            log.error("${operation.type} operation ${operation.id} failed", t)
        } finally {
            completionCallback?.invoke(operation.id)
        }
    }

    private fun getVolumeDesc(vol: Volume): String {
        return vol.properties.get("path")?.toString() ?: vol.name
    }

    fun start() {
        thread = Thread(this)
        thread?.start()
    }

    fun join() {
        thread?.join()
    }

    fun abort() {
        thread?.interrupt()
    }
}
