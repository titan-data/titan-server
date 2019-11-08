/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.operation

import io.titandata.ProviderModule
import io.titandata.exception.NoSuchObjectException
import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.remote.RemoteProvider
import io.titandata.storage.OperationData
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory

/*
 * The operation executor is responsible for managing the execution of a single operation. Every
 * operation will have exactly one executor associated with it, and will be the conduit to manage
 * asynchronous operations, progress, and more.
 */
class OperationExecutor(
    val providers: ProviderModule,
    val operation: Operation,
    val repo: String,
    val remote: Remote,
    val params: RemoteParameters,
    val isResume: Boolean = false,
    val metadataOnly: Boolean = false
) : Runnable {

    companion object {
        val log = LoggerFactory.getLogger(OperationExecutor::class.java)
    }

    private var thread: Thread? = null
    private var lastProgress: Int = 0
    private var commit: Commit? = null

    override fun run() {
        val provider = providers.remote(remote.provider)
        var operationData: Any? = null
        try {
            log.info("starting ${operation.type} operation ${operation.id}")

            if (operation.type == Operation.Type.PULL) {
                commit = provider.getCommit(remote, operation.commitId, params)
            }

            operationData = provider.startOperation(this)

            if (!metadataOnly) {
                syncData(provider, operationData)
            } else if (operation.type == Operation.Type.PULL) {
                providers.commits.updateCommit(repo, commit!!)
            }

            if (operation.type == Operation.Type.PUSH) {
                val localCommit = providers.commits.getCommit(repo, operation.commitId)
                provider.pushMetadata(this, operationData, localCommit, metadataOnly)
            }

            provider.endOperation(this, operationData)

            addProgress(ProgressEntry(ProgressEntry.Type.COMPLETE))
        } catch (t: InterruptedException) {
            addProgress(ProgressEntry(ProgressEntry.Type.ABORT))
            log.info("${operation.type} operation ${operation.id} interrupted", t)
        } catch (t: Throwable) {
            addProgress(ProgressEntry(ProgressEntry.Type.FAILED, t.message))
            log.error("${operation.type} operation ${operation.id} failed", t)
        } finally {
            try {
                if (operation.state == Operation.State.COMPLETE &&
                        operation.type == Operation.Type.PULL && !metadataOnly) {
                    // It shouldn't be possible for commit to be null here, or else it would've failed
                    providers.commits.createCommit(repo, commit!!, operation.id)
                } else {
                    transaction {
                        providers.metadata.deleteOperation(operation.id)
                        providers.metadata.markVolumeSetDeleting(operation.id)
                    }
                    providers.reaper.signal()
                }
                operationData = null
            } catch (t: Throwable) {
                log.error("finalization of ${operation.type} failed", t)
            } finally {
                if (operationData != null) {
                    provider.failOperation(this, operationData)
                }
            }
        }
    }

    fun syncData(provider: RemoteProvider, data: Any?) {
        val operationId = operation.id
            val volumes = transaction {
                providers.metadata.listVolumes(operationId).filter { it.name != "_scratch" }
            }
        val scratch = providers.storage.mountVolume(operationId, "_scratch")
        try {
            for (volume in volumes) {
                val mountpoint = providers.storage.mountVolume(operationId, volume.name)
                try {
                    if (operation.type == Operation.Type.PULL) {
                        provider.pullVolume(this, data, volume, mountpoint, scratch)
                    } else {
                        provider.pushVolume(this, data, volume, mountpoint, scratch)
                    }
                } finally {
                    providers.storage.unmountVolume(operationId, volume.name)
                }
            }

        } finally {
            providers.storage.unmountVolume(operationId, "_scratch")
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

    @Synchronized
    fun getProgress(): List<ProgressEntry> {
        val ret = transaction {
            providers.metadata.listProgressEntries(operation.id, lastProgress)
        }
        if (ret.size > 0) {
            lastProgress = ret.last().id
        }
        return ret
    }

    @Synchronized
    fun addProgress(entry: ProgressEntry) {
        val operationState = when (entry.type) {
            ProgressEntry.Type.FAILED -> Operation.State.FAILED
            ProgressEntry.Type.ABORT -> Operation.State.ABORTED
            ProgressEntry.Type.COMPLETE -> Operation.State.COMPLETE
            else -> Operation.State.RUNNING
        }
        transaction {
            providers.metadata.addProgressEntry(operation.id, entry)
            providers.metadata.updateOperationState(operation.id, operationState)
        }
        operation.state = operationState
    }
}
