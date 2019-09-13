/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan.operation

import com.delphix.titan.ProviderModule
import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import com.delphix.titan.storage.OperationData
import com.delphix.titan.util.CommandExecutor
import org.slf4j.LoggerFactory

/*
 * The operation executor is responsible for managing the execution of a single operation. Every
 * operation will have exactly one executor associated with it, and will be the conduit to manage
 * asynchronous operations, progress, and more.
 *
 * This is basically a wrapper around an asynchronous thread, with a defined channel to report
 * back progress, and default hadling of errors
 */
class OperationExecutor(
        val providers: ProviderModule,
        val operation: Operation,
        val repo: String,
        val remote: Remote,
        val params: RemoteParameters,
        val isResume: Boolean = false
) : Runnable {

    companion object {
        val log = LoggerFactory.getLogger(CommandExecutor::class.java)
    }

    private var thread: Thread? = null
    private var progress: MutableList<ProgressEntry> = mutableListOf()
    private var commit: Commit? = null

    override fun run() {
        try {
            log.info("starting ${operation.type} operation ${operation.id}")

            if (!isResume) {
                var localCommit: String? = null
                if (operation.type == Operation.Type.PUSH) {
                    localCommit = operation.commitId
                } else {
                    // TODO figure out best local commit for pulls, not just latest
                }
                providers.storage.createOperation(repo, OperationData(operation = operation,
                        params = params), localCommit)
            }

            val provider = providers.remote(remote.provider)
            if (operation.type == Operation.Type.PULL) {
                commit = provider.getCommit(remote, operation.commitId, params)
            }
            provider.runOperation(this)
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
                        operation.type == Operation.Type.PULL) {
                    // It shouldn't be possible for commit to be null here, or else it would've failed
                    providers.storage.commitOperation(repo, operation.id, commit!!)
                } else {
                    providers.storage.discardOperation(repo, operation.id)
                }
            } catch (t: Throwable) {
                log.error("finalization of ${operation.type} failed", t)
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
