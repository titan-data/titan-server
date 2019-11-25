/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.operation

import io.titandata.ServiceLocator
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
 * The operation executor is responsible for managing the execution of a single operation. Every
 * operation will have exactly one executor associated with it, and will be the conduit to manage
 * asynchronous operations, progress, and more.
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
    private var commit: Commit? = null

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

    override fun run() {
        val provider = services.remoteProvider(remote.provider)

        var success = false
        var operationData: RemoteOperation? = null
        try {
            log.info("starting ${operation.type} operation ${operation.id}")

            if (operation.type == Operation.Type.PULL) {
                commit = services.remotes.getRemoteCommit(repo, remote.name, params, operation.commitId)
            }

            val localCommit = if (operation.type == Operation.Type.PUSH) {
                services.commits.getCommit(repo, operation.commitId)
            } else {
                null
            }

            operationData = RemoteOperation(
                    updateProgress = updateProgress,
                    remote = remote.properties,
                    parameters = params.properties,
                    operationId = operation.id,
                    commitId = operation.commitId,
                    commit = localCommit?.properties,
                    type = if (operation.type == Operation.Type.PUSH) { RemoteOperationType.PUSH } else { RemoteOperationType.PULL },
                    data = null
            )

            provider.startOperation(operationData)

            if (!metadataOnly) {
                syncData(provider, operationData)
            } else if (operation.type == Operation.Type.PULL) {
                services.commits.updateCommit(repo, commit!!)
            }

            if (localCommit != null) {
                provider.pushMetadata(operationData, localCommit.properties, metadataOnly)
            }

            success = true
            provider.endOperation(operationData, true)

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
            try {
                val op = services.operations.getOperation(operation.id)
                if (op.state == Operation.State.COMPLETE &&
                        op.type == Operation.Type.PULL && !metadataOnly) {
                    // It shouldn't be possible for commit to be null here, or else it would've failed
                    services.commits.createCommit(repo, commit!!, operation.id)
                }
                // If an operation fails, then we don't explicitly delete it but wait for the last progress to be consumed
            } catch (t: Throwable) {
                log.error("finalization of ${operation.type} failed", t)
            } finally {
                if (!success && operationData != null) {
                    provider.endOperation(operationData, success)
                }
                completionCallback?.invoke(operation.id)
            }
        }
    }

    private fun getVolumeDesc(vol: Volume): String {
        return vol.properties.get("path")?.toString() ?: vol.name
    }

    internal fun syncData(provider: RemoteServer, data: RemoteOperation) {
        val operationId = operation.id
            val volumes = transaction {
                val vols = services.metadata.listVolumes(operationId)
                services.metadata.createVolume(operation.id, Volume("_scratch"))
                vols
            }
        val scratchConfig = services.context.createVolume(operation.id, "_scratch")
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
                    provider.syncVolume(data, volume.name, getVolumeDesc(volume), mountpoint, scratch)
                } finally {
                    services.context.deactivateVolume(operationId, volume.name, volume.config)
                }
            }
        } finally {
            services.context.deactivateVolume(operationId, "_scratch", scratchConfig)
            services.context.deleteVolume(operationId, "_scratch", scratchConfig)
            transaction {
                services.metadata.deleteVolume(operationId, "_scratch")
            }
        }
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
