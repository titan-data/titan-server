/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.operation.OperationExecutor
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory

/**
 * The operation manager is responsible for starting, stopping, and monitoring all active
 * operations. There are two basic types of operations: push and pull. In either case, we
 * create a clone that is marked as an in-progress operation. For pushes, it's simply
 * a clone of the source commit. For pulls, it's up to the caller to specify what the source
 * hash is. Ideally, this is the previous snapshot in the remote, but we can't always guarantee
 * that we have the same set of snapshots pulled locally, so we just try to make an educated case
 * about what a good source would be to minimize the amount of data transferred.
 *
 * The operation manager is built to be entirely asynchronous, so the way it works is to first
 * create the on-disk state representing the operation, and then trigger the asynchronous
 * execution of the operation. Even if the server were to restart, we can then recover from
 * failures.
 *
 * The only thing we keep in memory is progress messages. These are queued up for the CLI to
 * pull down and display to the user, but if the server restarts then we will lose any progress
 * and the CLI will have to start with progress from the resumed operation. We have a very simple
 * model of a single client, so we adopt a model of simply discarding progress entries once they've
 * been read, and marking the entire operation complete once the last progress message has been
 * read. Because we need to track on-going operations anyway, we keep a cache of all known
 * operations in memory and serve up API requests from there.
 *
 * Operation history is kept for 24 hours or until the CLI explicitly acknowledges completion.
 * In the normal case, where a user is waiting for the CLI operation to complete, this will
 * happen automatically. But in the event that the CLI command fails, or the user puts the
 * operation in the background, they can explicitly mark it completed.
 */
class OperationOrchestrator(val providers: ProviderModule) {

    companion object {
        val log = LoggerFactory.getLogger(OperationOrchestrator::class.java)
    }

    private val operationsByRepo: MutableMap<String, MutableList<OperationExecutor>> = mutableMapOf()
    private val operationsById: MutableMap<String, OperationExecutor> = mutableMapOf()

    internal fun addOperation(exec: OperationExecutor) {
        operationsById.put(exec.operation.id, exec)
        if (!operationsByRepo.contains(exec.repo)) {
            operationsByRepo[exec.repo] = mutableListOf()
        }
        operationsByRepo[exec.repo]!!.add(exec)
    }

    internal fun removeOperation(exec: OperationExecutor) {
        operationsById.remove(exec.operation.id)
        operationsByRepo.get(exec.repo)!!.remove(exec)
    }

    internal fun buildOperation(type: Operation.Type, volumeSet: String, remote: String, commitId: String): Operation {
        return Operation(
                volumeSet,
                type,
                Operation.State.RUNNING,
                remote,
                commitId
        )
    }

    internal fun createAndStartOperation(
        type: Operation.Type,
        repository: String,
        volumeSet: String,
        remote: Remote,
        commitId: String,
        params: RemoteParameters,
        metadataOnly: Boolean
    ): Operation {
        val op = buildOperation(type, volumeSet, remote.name, commitId)
        val exec = OperationExecutor(providers, op, repository, remote, params, false, metadataOnly)
        addOperation(exec)
        val message = when (type) {
            Operation.Type.PULL -> "Pulling $commitId from '${remote.name}'"
            Operation.Type.PUSH -> "Pushing $commitId to '${remote.name}'"
        }
        exec.addProgress(ProgressEntry(ProgressEntry.Type.MESSAGE, message))
        exec.start()
        return op
    }

    internal fun getExecutor(repository: String, id: String): OperationExecutor {
        val op = operationsById[id]
        if (op == null || op.repo != repository) {
            throw NoSuchObjectException("no such operation '$id' in repository '$repository'")
        }
        return op
    }

    private fun getRemote(repository: String, remote: String): Remote {
        return transaction {
            providers.metadata.getRemote(repository, remote)
        }
    }

    @Synchronized
    fun loadState() {
        log.debug("loading operation state")
        transaction {
            for (r in providers.metadata.listRepositories()) {
                for (op in providers.storage.listOperations(r.name)) {
                    val exec = OperationExecutor(providers, op.operation, r.name,
                            getRemote(r.name, op.operation.remote), op.params, true, op.metadataOnly)
                    addOperation(exec)
                    if (op.operation.state == Operation.State.RUNNING) {
                        log.info("retrying operation ${op.operation.id} after restart")
                        exec.addProgress(ProgressEntry(ProgressEntry.Type.MESSAGE,
                                "Retrying operation after restart"))
                        exec.start()
                    }
                }
            }
        }
    }

    @Synchronized
    fun clearState() {
        val values = operationsById.values
        for (exec in values) {
            exec.abort()
            exec.join()
            operationsByRepo.get(exec.repo)!!.remove(exec)
        }
        operationsById.clear()
    }

    @Synchronized
    fun getProgress(repository: String, id: String): List<ProgressEntry> {
        val exec = getExecutor(repository, id)
        val ret = exec.getProgress()
        // When you get the progress of an operation that's done, then we remove it from the history
        when (exec.operation.state) {
            Operation.State.COMPLETE -> removeOperation(exec)
            Operation.State.ABORTED -> removeOperation(exec)
            Operation.State.FAILED -> removeOperation(exec)
            else -> {}
        }
        return ret
    }

    @Synchronized
    fun listOperations(repository: String): List<Operation> {
        transaction {
            providers.metadata.getRepository(repository)
        }
        val operations = operationsByRepo[repository] ?: mutableListOf()
        return operations.map { op -> op.operation }
    }

    @Synchronized
    fun getOperation(repository: String, id: String): Operation {
        val op = getExecutor(repository, id)
        return op.operation
    }

    @Synchronized
    fun abortOperation(repository: String, id: String) {
        log.info("abort operation $id in $repository")
        getExecutor(repository, id).abort()
        // This won't actually wait for the operation or remove it, callers must consume the last
        // progress message to remove it
    }

    @Synchronized
    fun startPull(
        repository: String,
        remote: String,
        commitId: String,
        params: RemoteParameters,
        metadataOnly: Boolean = false
    ): Operation {

        log.info("pull $commitId from $remote in $repository")
        val r = getRemote(repository, remote)
        if (r.provider != params.provider) {
            throw IllegalArgumentException("operation parameters type (${params.provider}) doesn't match type of remote '$remote' (${r.provider})")
        }
        val remoteProvider = providers.remote(r.provider)
        remoteProvider.validateOperation(r, commitId, Operation.Type.PULL, params, metadataOnly)

        operationsById.values.forEach {
            if (it.repo == repository && it.operation.type == Operation.Type.PULL &&
                    it.operation.commitId == commitId && it.operation.state == Operation.State.RUNNING) {
                throw ObjectExistsException("Pull operation ${it.operation.id} already in progress for commit ${it.operation.commitId}")
            }
        }

        try {
            providers.commits.getCommit(repository, commitId)
            if (!metadataOnly) {
                throw ObjectExistsException("commit '$commitId' already exists in repository '$repository'")
            }
        } catch (e: NoSuchObjectException) {
            if (metadataOnly) {
                throw ObjectExistsException("no such commit '$commitId' in repository '$repository'")
            }
        }

        val volumeSet = transaction {
            providers.metadata.createVolumeSet(repository)
        }

        return createAndStartOperation(Operation.Type.PULL, repository, volumeSet, r, commitId, params, metadataOnly)
    }

    @Synchronized
    fun startPush(
        repository: String,
        remote: String,
        commitId: String,
        params: RemoteParameters,
        metadataOnly: Boolean = false
    ): Operation {

        log.info("push $commitId to $remote in $repository")
        val r = getRemote(repository, remote)
        if (r.provider != params.provider) {
            throw IllegalArgumentException("operation parameters type (${params.provider}) doesn't match type of remote '$remote' (${r.provider})")
        }
        providers.commits.getCommit(repository, commitId) // check commit exists
        val remoteProvider = providers.remote(r.provider)
        if (metadataOnly) {
            remoteProvider.getCommit(r, commitId, params) // for metadata only must exist in remote as well
        }

        operationsById.values.forEach {
            if (it.repo == repository && it.operation.type == Operation.Type.PUSH &&
                    it.operation.commitId == commitId && it.remote.name == remote &&
                    it.operation.state == Operation.State.RUNNING) {
                throw ObjectExistsException("Push operation ${it.operation.id} to remote $remote already in progress for commit ${it.operation.commitId}")
            }
        }

        remoteProvider.validateOperation(r, commitId, Operation.Type.PUSH, params, metadataOnly)

        val volumeSet = transaction {
            providers.metadata.createVolumeSet(repository)
        }

        return createAndStartOperation(Operation.Type.PUSH, repository, volumeSet, r, commitId, params, metadataOnly)
    }
}
