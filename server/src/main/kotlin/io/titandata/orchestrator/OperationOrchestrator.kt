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
import io.titandata.models.Volume
import io.titandata.operation.OperationExecutor
import io.titandata.storage.OperationData
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

    private val executors: MutableMap<String, OperationExecutor> = mutableMapOf()

    internal fun createAndStartOperation(
        type: Operation.Type,
        repository: String,
        remote: Remote,
        commitId: String,
        params: RemoteParameters,
        metadataOnly: Boolean
    ): Operation {
        var localCommit: String? = null
        if (type == Operation.Type.PUSH) {
            localCommit = commitId
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
                val provider = providers.remote(remote.provider)
                var remoteCommit = provider.getCommit(remote, commitId, params)
                while (localCommit == null && remoteCommit.properties.containsKey("tags")) {
                    @Suppress("UNCHECKED_CAST")
                    val tags = remoteCommit.properties["tags"] as Map<String, String>
                    if (tags.containsKey("source")) {
                        val source = tags["source"]!!
                        try {
                            localCommit = providers.commits.getCommit(repository, source).id
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

        // Create metadata
        val (volumeSet, operation) = transaction {
            val vs = providers.metadata.createVolumeSet(repository, localCommit)
            val volumes = providers.metadata.listVolumes(providers.metadata.getActiveVolumeSet(repository))
            for (v in volumes) {
                providers.metadata.createVolume(vs, v)
            }
            providers.metadata.createVolume(vs, Volume(name = "_scratch"))
            val op = Operation(id = vs, type = type, state = Operation.State.RUNNING, remote = remote.name, commitId = commitId)
            providers.metadata.createOperation(repository, vs, OperationData(
                    metadataOnly = metadataOnly,
                    params = params,
                    operation = op
            ))
            Pair(vs, op)
        }

        // Create storage
        if (!metadataOnly) {
            if (localCommit == null) {
                val volumes = transaction {
                    providers.metadata.listVolumes(volumeSet)
                }
                providers.storage.createVolumeSet(volumeSet)
                for (v in volumes) {
                    providers.storage.createVolume(volumeSet, v.name)
                }
            } else {
                val (sourceVolumeSet, volumes) = transaction {
                    val vs = providers.metadata.getCommit(repository, localCommit).first
                    Pair(vs, providers.metadata.listVolumes(vs).map { it.name })
                }
                providers.storage.cloneVolumeSet(sourceVolumeSet, localCommit, volumeSet, volumes)
                providers.storage.createVolume(volumeSet, "_scratch")
            }
        }

        // Run executor
        val exec = OperationExecutor(providers, operation, repository, remote, params, metadataOnly)
        executors.put(exec.operation.id, exec)
        val message = when (type) {
            Operation.Type.PULL -> "Pulling $commitId from '${remote.name}'"
            Operation.Type.PUSH -> "Pushing $commitId to '${remote.name}'"
        }
        exec.addProgress(ProgressEntry(ProgressEntry.Type.MESSAGE, message))
        exec.start()
        return operation
    }

    internal fun getExecutor(repository: String, id: String): OperationExecutor {
        providers.repositories.getRepository(repository)
        NameUtil.validateOperationId(id)
        val op = executors[id]
        if (op == null || op.repo != repository) {
            throw NoSuchObjectException("no such operation '$id' in repository '$repository'")
        }
        return op
    }

    @Synchronized
    fun loadState() {
        log.debug("loading operation state")
        transaction {
            for (r in providers.metadata.listRepositories()) {
                for (op in providers.metadata.listOperations(r.name)) {
                    val exec = OperationExecutor(providers, op.operation, r.name,
                            providers.remotes.getRemote(r.name, op.operation.remote), op.params, op.metadataOnly)
                    executors.put(exec.operation.id, exec)
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
        val values = executors.values
        for (exec in values) {
            exec.abort()
            exec.join()
        }
        executors.clear()
    }

    @Synchronized
    fun getProgress(repository: String, id: String): List<ProgressEntry> {
        val exec = getExecutor(repository, id)
        val ret = exec.getProgress()
        // When you get the progress of an operation that's done, then we remove it from the history
        val remove = when (exec.operation.state) {
            Operation.State.COMPLETE -> true
            Operation.State.ABORTED -> true
            Operation.State.FAILED -> true
            else -> false
        }
        if (remove) {
            transaction {
                providers.metadata.deleteOperation(id)
            }
            executors.remove(exec.operation.id)
        }
        return ret
    }

    fun listOperations(repository: String): List<Operation> {
        providers.repositories.getRepository(repository)
        return transaction {
            providers.metadata.listOperations(repository).map { it.operation }
        }
    }

    fun getOperation(repository: String, id: String): Operation {
        NameUtil.validateOperationId(id)
        providers.repositories.getRepository(repository)
        return transaction {
            val op = providers.metadata.getOperation(id).operation
            if (providers.metadata.getVolumeSetRepo(op.id) != repository) {
                throw NoSuchObjectException("no such operation '$id' in repository '$repository'")
            }
            op
        }
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
        NameUtil.validateCommitId(commitId)
        val r = providers.remotes.getRemote(repository, remote)
        if (r.provider != params.provider) {
            throw IllegalArgumentException("operation parameters type (${params.provider}) doesn't match type of remote '$remote' (${r.provider})")
        }
        val remoteProvider = providers.remote(r.provider)
        remoteProvider.validateOperation(r, commitId, Operation.Type.PULL, params, metadataOnly)

        val inProgress = transaction {
            providers.metadata.operationInProgress(repository, Operation.Type.PULL, commitId, null)
        }
        if (inProgress != null) {
            throw ObjectExistsException("Pull operation $inProgress already in progress for commit $commitId")
        }

        try {
            providers.commits.getCommit(repository, commitId)
            if (!metadataOnly) {
                throw ObjectExistsException("commit '$commitId' already exists in repository '$repository'")
            }
        } catch (e: NoSuchObjectException) {
            if (metadataOnly) {
                throw NoSuchObjectException("no such commit '$commitId' in repository '$repository'")
            }
        }

        return createAndStartOperation(Operation.Type.PULL, repository, r, commitId, params, metadataOnly)
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
        NameUtil.validateCommitId(commitId)
        val r = providers.remotes.getRemote(repository, remote)
        if (r.provider != params.provider) {
            throw IllegalArgumentException("operation parameters type (${params.provider}) doesn't match type of remote '$remote' (${r.provider})")
        }
        providers.commits.getCommit(repository, commitId) // check commit exists
        val remoteProvider = providers.remote(r.provider)
        if (metadataOnly) {
            remoteProvider.getCommit(r, commitId, params) // for metadata only must exist in remote as well
        }

        val inProgress = transaction {
            providers.metadata.operationInProgress(repository, Operation.Type.PUSH, commitId, remote)
        }
        if (inProgress != null) {
            throw ObjectExistsException("Push operation $inProgress to remote $remote already in progress for commit $commitId")
        }

        remoteProvider.validateOperation(r, commitId, Operation.Type.PUSH, params, metadataOnly)

        return createAndStartOperation(Operation.Type.PUSH, repository, r, commitId, params, metadataOnly)
    }
}
