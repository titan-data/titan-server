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
    private var progress: MutableList<ProgressEntry> = mutableListOf()
    private var commit: Commit? = null

    override fun run() {
        val provider = providers.remote(remote.provider)
        var operationData: Any? = null
        try {
            log.info("starting ${operation.type} operation ${operation.id}")

            if (operation.type == Operation.Type.PULL) {
                commit = provider.getCommit(remote, operation.commitId, params)
            }

            if (!isResume) {
                var localCommit: String? = null
                if (operation.type == Operation.Type.PUSH) {
                    localCommit = operation.commitId
                } else {
                    /*
                     * For pulls, we want to try to find the best possible source snapshot, so that incremental pulls
                     * minimize the amount of storage changed. To do this, we rely on the CLI pushing metadat with a
                     * "source" tag that indicates the source of the commmit. We chase this chain as far as we can
                     * on the remote until we find a matching commit locally. In the event that the chain is broken on
                     * the remote (because a commit has been deleted) or that we don't have any appropriate commits
                     * locally, we simply use the latest commit and hope for the best.
                     */
                    try {
                        var remoteCommit = commit!!
                        while (localCommit == null && remoteCommit.properties.containsKey("tags")) {
                            @Suppress("UNCHECKED_CAST")
                            val tags = remoteCommit.properties["tags"] as Map<String, String>
                            if (tags.containsKey("source")) {
                                val source = tags["source"]!!
                                try {
                                    localCommit = providers.storage.getCommit(repo, source).id
                                } catch (e: NoSuchObjectException) {
                                    // Ignore local commits that don't exist and continue down chain
                                }
                                remoteCommit = provider.getCommit(remote, source, params)
                            } else {
                                break
                            }
                        }
                    } catch (e: NoSuchObjectException) {
                        // If we can't find a remote commit in the chain, then default to latest
                    }
                }

                providers.storage.createOperation(repo, OperationData(operation = operation,
                        params = params, metadataOnly = metadataOnly), localCommit)
            }

            operationData = provider.startOperation(this)

            if (!metadataOnly) {
                syncData(provider, operationData)
            } else if (operation.type == Operation.Type.PULL) {
                providers.storage.updateCommit(repo, commit!!)
            }

            if (operation.type == Operation.Type.PUSH) {
                val localCommit = providers.storage.getCommit(repo, operation.commitId)
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
                    providers.storage.commitOperation(repo, operation.id, commit!!)
                } else {
                    providers.storage.discardOperation(repo, operation.id)
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
        val scratch = providers.storage.createOperationScratch(repo, operationId)
        try {
            val volumes = providers.volumes.listVolumes(repo)
            val base = providers.storage.mountOperationVolumes(repo, operationId, volumes)
            try {
                for (volume in volumes) {
                    if (operation.type == Operation.Type.PULL) {
                        provider.pullVolume(this, data, volume, base, scratch)
                    } else {
                        provider.pushVolume(this, data, volume, base, scratch)
                    }
                }
            } finally {
                providers.storage.unmountOperationVolumes(repo, operationId, volumes)
            }
        } finally {
            providers.storage.destroyOperationScratch(repo, operationId)
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
        val ret = progress
        progress = mutableListOf()
        return ret
    }

    @Synchronized
    fun addProgress(entry: ProgressEntry) {
        progress.add(entry)
        when (entry.type) {
            ProgressEntry.Type.FAILED -> operation.state = Operation.State.FAILED
            ProgressEntry.Type.ABORT -> operation.state = Operation.State.ABORTED
            ProgressEntry.Type.COMPLETE -> operation.state = Operation.State.COMPLETE
            else -> operation.state = Operation.State.RUNNING
        }
    }
}
