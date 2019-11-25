/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.orchestrator

import io.titandata.ServiceLocator
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.metadata.OperationData
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Volume
import io.titandata.operation.OperationExecutor
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory

/**
 * The operation manager is responsible for starting, stopping, and monitoring all active
 * operations. There are two basic types of operations: push and pull. In either case, we
 * create a volume set that is marked as an in-progress operation. For pushes, it's simply
 * a clone of the source commit. For pulls, we try to find the closest possible commit based
 * on the commit source as recorded in the remote repository.
 *
 * Progress messages are queued up for the CLI to pull down and display to the user, but if the
 * server restarts then we will lose any progress and the CLI will have to start with progress
 * from the resumed operation. We have a very simple model of a single client, so we adopt a
 * model of simply discarding progress entries once they've been read, and marking the entire
 * operation complete once the last progress message has been read.
 */
class OperationOrchestrator(val services: ServiceLocator) {

    companion object {
        val log = LoggerFactory.getLogger(OperationOrchestrator::class.java)
    }

    private val runningOperations: MutableMap<String, OperationExecutor> = mutableMapOf()

    val operationComplete = fun(operationId: String) {
        runningOperations.remove(operationId)
        services.reaper.signal()
    }

    internal fun createAndStartOperation(
        type: Operation.Type,
        repository: String,
        remote: Remote,
        commitId: String,
        params: RemoteParameters,
        metadataOnly: Boolean
    ): Operation {
        var localCommit = findLocalCommit(type, repository, remote, params, commitId)

        val (volumeSet, operation) = createMetadata(repository, type, remote.name, commitId, metadataOnly, params, localCommit)

        if (!metadataOnly) {
            createStorage(repository, volumeSet, localCommit)
        }

        // Run executor
        val exec = OperationExecutor(services, operation, repository, remote, params, metadataOnly, operationComplete)
        runningOperations.put(exec.operation.id, exec)
        val message = when (type) {
            Operation.Type.PULL -> "Pulling $commitId from '${remote.name}'"
            Operation.Type.PUSH -> "Pushing $commitId to '${remote.name}'"
        }
        services.metadata.addProgressEntry(operation.id, ProgressEntry(ProgressEntry.Type.MESSAGE, message))
        exec.start()
        return operation
    }

    /**
     * Find the local commit for source of the volume set clone. For pushes, this is just the same as the source commit.
     *
     * For pulls, we find the best possible source snapshot, so that incremental pulls minimize the amount of storage
     * changed. To do this, we rely on the CLI pushing metadata with a "source" tag that indicates the source of the
     * commit. We chase this chain as far as we can on the remote until we find a matching commit locally. In the
     * event that the chain is broken on the remote (because a commit has been deleted) or that we don't have any
     * appropriate commits locally, we simply use the latest commit and hope for the best.
     */
    internal fun findLocalCommit(type: Operation.Type, repo: String, remote: Remote, params: RemoteParameters, commitId: String): String? {
        if (type == Operation.Type.PUSH) {
            return commitId
        } else {
            var localCommit: String? = null
            val provider = services.remoteProvider(remote.provider)
            var remoteCommit = provider.getCommit(remote.properties, params.properties, commitId)
            while (localCommit == null && remoteCommit != null && remoteCommit.containsKey("tags")) {
                @Suppress("UNCHECKED_CAST")
                val tags = remoteCommit["tags"] as Map<String, String>
                if (tags.containsKey("source")) {
                    val source = tags["source"]!!
                    try {
                        localCommit = services.commits.getCommit(repo, source).id
                    } catch (e: NoSuchObjectException) {
                        // Ignore local commits that don't exist and continue down chain
                    }
                    remoteCommit = provider.getCommit(remote.properties, params.properties, source)
                } else {
                    break
                }
            }

            if (localCommit == null) {
                localCommit = transaction {
                    services.metadata.getLastCommit(repo)
                }
            }
            return localCommit
        }
    }

    /**
     * Create the metadata associated with this operation. This consists of:
     *
     *      A new volumeset, which is also the operation ID
     *      Volumes that correspond to the volumes in the current active volume set
     *      Operation with the given configuration
     */
    internal fun createMetadata(
        repo: String,
        type: Operation.Type,
        remoteName: String,
        commitId: String,
        metadataOnly: Boolean,
        params: RemoteParameters,
        commit: String?
    ): Pair<String, Operation> {
        return transaction {
            val vs = services.metadata.createVolumeSet(repo, commit)
            val volumes = services.metadata.listVolumes(services.metadata.getActiveVolumeSet(repo))
            for (v in volumes) {
                // Intentionally create the volume with no configuration, to be filled in later after it's created by the storage layer
                services.metadata.createVolume(vs, Volume(name = v.name, properties = v.properties))
            }
            val op = Operation(id = vs, type = type, state = Operation.State.RUNNING, remote = remoteName, commitId = commitId)
            services.metadata.createOperation(repo, vs, OperationData(
                    metadataOnly = metadataOnly,
                    params = params,
                    repo = repo,
                    operation = op
            ))
            Pair(vs, op)
        }
    }

    /**
     * Create the storage associated with this operation. When there is no known common local commit, this is
     * accomplished by creating a new volume set, and volumes for each of the volumes in the current active
     * volumeset. If we do have a common local commit, then we instead clone the volume set.
     */
    internal fun createStorage(repo: String, volumeSet: String, commit: String?) {
        if (commit == null) {
            val volumes = transaction {
                val vs = services.metadata.getActiveVolumeSet(repo)
                services.metadata.listVolumes(vs)
            }
            services.context.createVolumeSet(volumeSet)
            for (v in volumes) {
                val config = services.context.createVolume(volumeSet, v.name)
                transaction {
                    services.metadata.updateVolumeConfig(volumeSet, v.name, config)
                }
            }
        } else {
            val (sourceVolumeSet, volumes) = transaction {
                val vs = services.metadata.getCommit(repo, commit).first
                Pair(vs, services.metadata.listVolumes(vs))
            }
            services.context.cloneVolumeSet(sourceVolumeSet, commit, volumeSet)
            for (v in volumes) {
                val config = services.context.cloneVolume(sourceVolumeSet, commit, volumeSet, v.name, v.config)
                transaction {
                    services.metadata.updateVolumeConfig(volumeSet, v.name, config)
                }
            }
        }
    }

    fun loadState() {
        log.debug("loading operation state")
        val operations = transaction {
            services.metadata.listOperations()
        }
        for (op in operations) {
            val exec = OperationExecutor(services, op.operation, op.repo,
                    services.remotes.getRemote(op.repo, op.operation.remote), op.params, op.metadataOnly,
                    operationComplete)
            if (op.operation.state == Operation.State.RUNNING) {
                runningOperations.put(exec.operation.id, exec)
                log.info("retrying operation ${op.operation.id} after restart")
                services.metadata.addProgressEntry(op.operation.id, ProgressEntry(ProgressEntry.Type.MESSAGE,
                        "Retrying operation after restart"))
                exec.start()
            }
        }
    }

    /**
     * This method is just for testing, and handles cases where we've mocked up some executors without a fully
     * functional completion callback.
     */
    fun clearState() {
        for (exec in runningOperations.values) {
            exec.abort()
            exec.join()
        }
        runningOperations.clear()
    }

    fun getProgress(operationId: String, lastEntryId: Int): List<ProgressEntry> {
        return transaction {
            services.metadata.listProgressEntries(operationId, lastEntryId)
        }
    }

    fun listOperations(repository: String?): List<Operation> {
        return transaction {
            if (repository != null) {
                services.repositories.getRepository(repository)
                services.metadata.listOperationsByRepository(repository).map { it.operation }
            } else {
                services.metadata.listOperations().map { it.operation }
            }
        }
    }

    fun getOperation(id: String): Operation {
        NameUtil.validateOperationId(id)
        return transaction {
            services.metadata.getOperation(id).operation
        }
    }

    fun abortOperation(id: String) {
        getOperation(id)
        runningOperations[id]?.abort()
    }

    /**
     * Start a pull operation. This will perforjm all the checks to make sure the local and remote repository are in
     * the appropriate state, and then invoke createAndStartOperation() to do the actual work of creating and
     * running the operation.
     */
    fun startPull(
        repository: String,
        remote: String,
        commitId: String,
        rawParams: RemoteParameters,
        metadataOnly: Boolean = false
    ): Operation {

        log.info("pull $commitId from $remote in $repository")
        NameUtil.validateCommitId(commitId)
        val params = services.remotes.validateParameters(rawParams)
        val r = services.remotes.getRemote(repository, remote)
        if (r.provider != params.provider) {
            throw IllegalArgumentException("operation parameters type (${params.provider}) doesn't match type of remote '$remote' (${r.provider})")
        }
        if (r.provider != "nop") {
            services.remotes.getRemoteCommit(repository, remote, params, commitId)
        }

        val inProgress = transaction {
            services.metadata.operationInProgress(repository, Operation.Type.PULL, commitId, null)
        }
        if (inProgress != null) {
            throw ObjectExistsException("Pull operation $inProgress already in progress for commit $commitId")
        }

        try {
            services.commits.getCommit(repository, commitId)
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

    /**
     * Start a push operation. This will perform all the checks to make sure the local and remote repository are in
     * the appropriate state, and then invoke createAndStartOperation() to do the actual work of creating and
     * running the operation.
     */
    fun startPush(
        repository: String,
        remote: String,
        commitId: String,
        rawParams: RemoteParameters,
        metadataOnly: Boolean = false
    ): Operation {

        log.info("push $commitId to $remote in $repository")
        NameUtil.validateCommitId(commitId)
        val params = services.remotes.validateParameters(rawParams)
        val r = services.remotes.getRemote(repository, remote)
        if (r.provider != params.provider) {
            throw IllegalArgumentException("operation parameters type (${params.provider}) doesn't match type of remote '$remote' (${r.provider})")
        }
        services.commits.getCommit(repository, commitId) // check commit exists
        val remoteProvider = services.remoteProvider(r.provider)
        val remoteCommit = remoteProvider.getCommit(r.properties, params.properties, commitId)
        if (metadataOnly && remoteCommit == null) {
            throw NoSuchObjectException("no such commit '$commitId' in remote '$remote")
        }

        val inProgress = transaction {
            services.metadata.operationInProgress(repository, Operation.Type.PUSH, commitId, remote)
        }
        if (inProgress != null) {
            throw ObjectExistsException("Push operation $inProgress to remote $remote already in progress for commit $commitId")
        }

        if (r.provider != "nop" && remoteCommit != null && !metadataOnly) {
            throw ObjectExistsException("commit $commitId exists in remote '$remote'")
        }

        return createAndStartOperation(Operation.Type.PUSH, repository, r, commitId, params, metadataOnly)
    }
}
