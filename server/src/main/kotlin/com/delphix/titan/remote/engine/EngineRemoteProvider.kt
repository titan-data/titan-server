/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan.remote.engine

import com.delphix.sdk.Delphix
import com.delphix.sdk.Http
import com.delphix.sdk.objects.AppDataContainer
import com.delphix.sdk.objects.AppDataDirectSourceConfig
import com.delphix.sdk.objects.AppDataProvisionParameters
import com.delphix.sdk.objects.AppDataSyncParameters
import com.delphix.sdk.objects.AppDataVirtualSource
import com.delphix.sdk.objects.DeleteParameters
import com.delphix.sdk.objects.Repository
import com.delphix.sdk.objects.SourceDisableParameters
import com.delphix.sdk.objects.TimeflowPointParameters
import com.delphix.sdk.objects.TimeflowPointSemantic
import com.delphix.sdk.objects.TimeflowPointSnapshot
import com.delphix.titan.ProviderModule
import com.delphix.titan.exception.InvalidStateException
import com.delphix.titan.exception.NoSuchObjectException
import com.delphix.titan.exception.RemoteException
import com.delphix.titan.models.Commit
import com.delphix.titan.models.EngineParameters
import com.delphix.titan.models.EngineRemote
import com.delphix.titan.models.Operation
import com.delphix.titan.models.ProgressEntry
import com.delphix.titan.models.Remote
import com.delphix.titan.models.RemoteParameters
import com.delphix.titan.operation.OperationExecutor
import com.delphix.titan.remote.BaseRemoteProvider
import com.delphix.titan.sync.RsyncExecutor
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import org.json.JSONObject
import org.slf4j.LoggerFactory

class EngineRemoteProvider(val providers: ProviderModule) : BaseRemoteProvider() {

    companion object {
        val log = LoggerFactory.getLogger(EngineRemoteProvider::class.java)
    }

    private fun connect(remote: Remote, params: RemoteParameters): Delphix {
        val engineRemote = remote as EngineRemote
        val engineParams = params as EngineParameters
        val engine = Delphix(Http("http://${engineRemote.address}"))
        if (engineRemote.password == null && engineParams.password == null) {
            throw IllegalArgumentException("missing password in remote parameters")
        }
        engine.login(engineRemote.username, engineParams.password ?: engineRemote.password!!)
        return engine
    }
    private fun findInResult(result: JSONObject, lambda: (JSONObject) -> Boolean): JSONObject? {
        val objects = result.getJSONArray("result")
        for (i in 0 until objects.length()) {
            val obj = objects.getJSONObject(i)
            if (lambda(obj)) {
                return obj
            }
        }
        return null
    }

    private fun findByName(result: JSONObject, name: String): JSONObject? {
        return findInResult(result) { it.getString("name") == name }
    }

    private fun findInGroup(engine: Delphix, groupName: String, name: String): JSONObject? {
        val group = findByName(engine.group().list(), groupName) ?: return null
        val groupRef = group.getString("reference")

        return findInResult(engine.container().list()) {
            it.getString("name") == name && it.getString("group") == groupRef
        }
    }

    private fun getRepository(engine: Delphix): Repository? {
        for (r in engine.repository().list()) {
            if (r.name == "Titan") {
                return r
            }
        }
        return null
    }

    private fun getEnvUser(engine: Delphix, env: JSONObject): JSONObject? {
        return findInResult(engine.environmentUser().list()) {
            it.getString("environment") == env.getString("reference")
        }
    }

    fun waitForJob(engine: Delphix, result: JSONObject) {
        val actionResult: JSONObject = engine.action().read(result.getString("action")).getJSONObject("result")
        if (actionResult.optString("state") == "COMPLETED") {
            log.debug("action ${actionResult.getString("reference")} complete")
            println("${actionResult.getString("title")}")
        } else {
            log.debug("waiting for job ${result.getString("job")}")
            var job: JSONObject = engine.job().read(result.getString("job")).getJSONObject("result")
            while (job.optString("jobState") == "RUNNING") {
                Thread.sleep(5000)
                job = engine.job().read(result.getString("job")).getJSONObject("result")
            }

            if (job.optString("jobState") != "COMPLETED") {
                throw RemoteException("engine job ${job.getString("reference")} failed")
            }
            log.debug("engine job ${job.getString("reference")} complete")
        }
    }

    fun repoExists(engine: Delphix, name: String): Boolean {
        return findInGroup(engine, "repositories", name) != null
    }

    /**
     * Adding a remote shouldn't really be performing operations on the remote. We don't have
     * the ability to report progress or do any long-running operations. But for now this lets
     * us make forward progress. Once we have a few providers we can decide how we want to deal
     * with this (or declare that al remotes must be created out of band of the CLI).
     */
    fun createRepo(engine: Delphix, executor: OperationExecutor) {
        val name = (executor.remote as EngineRemote).repository

        executor.addProgress(ProgressEntry(type = ProgressEntry.Type.START, message = "Creating remote repository"))

        val titanEnvironment = findByName(engine.sourceEnvironment().list(), "titan")
            ?: throw InvalidStateException("engine not properly configured for titan")
        val titanSource = findInGroup(engine, "master", "titan")
            ?: throw InvalidStateException("engine not properly configured for titan")
        val repositoryGroup = findByName(engine.group().list(), "repositories")
            ?: throw InvalidStateException("engine not properly configured for titan")
        val sourceRepository = getRepository(engine)
            ?: throw InvalidStateException("engine not properly configured for titan")
        val titanUser = getEnvUser(engine, titanEnvironment)
            ?: throw InvalidStateException("engine not properly configured for titan")

        val container = AppDataContainer(name = name, group = repositoryGroup.getString("reference"))
        val timeflowPoint = TimeflowPointSemantic(container = titanSource.getString("reference"),
                location = "LATEST_SNAPSHOT")
        val source = AppDataVirtualSource(name = name, additionalMountPoints = ArrayList(),
                parameters = mapOf("operationId" to name))
        val sourceConfig = AppDataDirectSourceConfig(repository = sourceRepository.reference,
                environmentUser = titanUser.getString("reference"), name = name, linkingEnabled = false,
                path = "")
        val provisionParams = AppDataProvisionParameters(container = container, timeflowPointParameters = timeflowPoint,
                source = source, sourceConfig = sourceConfig)

        log.info("provisioning VDB on ${engine.http.engineAddress}")
        log.info(provisionParams.toMap().toString())
        val response = engine.container().provision("provision", provisionParams)
        val containerRef = response.getString("result")
        waitForJob(engine, response)

        val createdSource = findInResult(engine.source().list()) {
            it.getString("container") == containerRef
        }

        log.info("disabling VDB on ${engine.http.engineAddress}")
        val disableResponse = engine.source().disable(createdSource!!.getString("reference"),
                SourceDisableParameters())
        waitForJob(engine, disableResponse)

        executor.addProgress(ProgressEntry(type = ProgressEntry.Type.END))
    }

    private fun listSnapshots(engine: Delphix, remote: Remote): List<JSONObject> {
        val name = (remote as EngineRemote).repository

        if (!repoExists(engine, name)) {
            throw NoSuchObjectException("no such repo '$name' in remote '${remote.name}'")
        }

        val ret = ArrayList<JSONObject>()
        val snapshots = engine.snapshot().list().getJSONArray("result")
        for (i in 0 until snapshots.length()) {
            val snapshot = snapshots.getJSONObject(i)
            val properties = snapshot.optJSONObject("metadata")
            if (properties != null && properties.has("repository") && properties.has("hash")) {
                if (properties.getString("repository") == name && properties.getString("hash") != "") {
                    if (!properties.has("creation")) {
                        properties.put("creation", snapshot.getString("creationTime"))
                    }
                    properties.put("reference", snapshot.getString("reference"))
                    ret.add(properties)
                }
            }
        }

        return ret.sortedByDescending { OffsetDateTime.parse(it.getString("creation"), DateTimeFormatter.ISO_DATE_TIME) }
    }

    override fun listCommits(remote: Remote, params: RemoteParameters): List<Commit> {
        val engine = connect(remote, params)
        return listSnapshots(engine, remote).map {
            Commit(id = it.getString("hash"), properties = it.getJSONObject("metadata").toMap())
        }
    }

    override fun getCommit(remote: Remote, commitId: String, params: RemoteParameters): Commit {
        // This is horribly inefficient, but all we can do until we have a better API
        val commits = listCommits(remote, params)
        return commits.find { it -> it.id == commitId }
            ?: throw NoSuchObjectException("no such commit $commitId in remote ${remote.name}")
    }

    private fun buildContainer(engine: Delphix, operation: Operation): AppDataContainer {
        val operationsGroup = findByName(engine.group().list(), "operations")
                ?: throw InvalidStateException("engine not properly configured for titan")
        return AppDataContainer(name = operation.id,
                group = operationsGroup.getString("reference"))
    }

    private fun buildSourceConfig(engine: Delphix, operation: Operation): AppDataDirectSourceConfig {
        val sourceRepository = getRepository(engine)
                ?: throw InvalidStateException("engine not properly configured for titan")
        val titanEnvironment = findByName(engine.sourceEnvironment().list(), "titan")
                ?: throw InvalidStateException("engine not properly configured for titan")
        val titanUser = getEnvUser(engine, titanEnvironment)
                ?: throw InvalidStateException("engine not properly configured for titan")
        return AppDataDirectSourceConfig(repository = sourceRepository.reference,
                environmentUser = titanUser.getString("reference"), name = operation.id, linkingEnabled = false,
                path = "")
    }

    private fun buildTimeflowPoint(engine: Delphix, operation: OperationExecutor): TimeflowPointParameters {
        val repoName = (operation.remote as EngineRemote).repository

        if (operation.operation.type == Operation.Type.PUSH) {
            val repo = findInGroup(engine, "repositories", repoName)
                ?: throw NoSuchObjectException("no such repository '$repoName' in remote '${operation.remote.name}'")
            return TimeflowPointSemantic(container = repo.getString("reference"),
                    location = "LATEST_SNAPSHOT")
        } else {
            val snapshots = listSnapshots(engine, operation.remote)
            val snapshot = snapshots.find { it -> it.getString("hash") == operation.operation.commitId }
                    ?: throw NoSuchObjectException("no such commit '${operation.operation.commitId}' in remote '${operation.remote.name}'")
            return TimeflowPointSnapshot(snapshot.getString("reference"))
        }
    }

    private fun buildSource(operation: OperationExecutor): AppDataVirtualSource {
        val remote = operation.remote as EngineRemote
        val parameters: MutableMap<String, Any> = HashMap()
        parameters["operationId"] = operation.operation.id
        parameters["operationType"] = operation.operation.type
        parameters["repository"] = remote.repository
        if (operation.operation.type == Operation.Type.PUSH) {
            val commit = providers.storage.getCommit(operation.repo, operation.operation.commitId)
            parameters["hash"] = operation.operation.commitId
            parameters["metadata"] = commit.properties
        }

        return AppDataVirtualSource(name = operation.operation.id, additionalMountPoints = ArrayList(),
                parameters = parameters)
    }

    fun getParameters(engine: Delphix, containerRef: String): JSONObject {
        val source = findInResult(engine.source().list()) {
            it.getString("container") == containerRef
        }
        val config = engine.sourceConfig().read(source!!.getString("config")).getJSONObject("result")
        val obj = config.getJSONObject("parameters")
        return obj
    }

    override fun runOperation(operation: OperationExecutor) {
        val engine = connect(operation.remote, operation.params)
        val name = (operation.remote as EngineRemote).repository

        if (!repoExists(engine, name)) {
            if (operation.operation.type == Operation.Type.PULL) {
                throw NoSuchObjectException("no such repository '$name' in remote '${operation.remote.name}")
            } else {
                createRepo(engine, operation)
            }
        }

        operation.addProgress(ProgressEntry(type = ProgressEntry.Type.START, message = "Creating remote endpoint"))
        val timeflowPoint = buildTimeflowPoint(engine, operation)

        var params = AppDataProvisionParameters(
                container = buildContainer(engine, operation.operation),
                timeflowPointParameters = timeflowPoint,
                source = buildSource(operation),
                sourceConfig = buildSourceConfig(engine, operation.operation)
                )

        var response = engine.container().provision("provision", params)
        val operationRef = response.getString("result")

        var deleteOperation = true
        try {
            waitForJob(engine, response)
            operation.addProgress(ProgressEntry(type = ProgressEntry.Type.END))

            val resultParams = getParameters(engine, operationRef)
            val sshAddress = operation.remote.address
            val sshUser = resultParams.getString("sshUser")
            val sshKey = resultParams.getString("sshKey")

            val repo = operation.repo
            val base = providers.storage.mountOperationVolumes(repo, operation.operation.id)
            try {
                for (vol in providers.storage.listVolumes(repo)) {
                    val desc = vol.properties?.get("path")?.toString() ?: vol.name
                    val localPath = "$base/${vol.name}"
                    val remotePath = "$sshUser@$sshAddress:data/${vol.name}"
                    val src = when (operation.operation.type) {
                        Operation.Type.PUSH -> localPath
                        Operation.Type.PULL -> remotePath
                    }
                    val dst = when (operation.operation.type) {
                        Operation.Type.PUSH -> remotePath
                        Operation.Type.PULL -> localPath
                    }

                    operation.addProgress(ProgressEntry(type = ProgressEntry.Type.START,
                            message = "Syncing $desc", percent = 0))
                    val rsync = RsyncExecutor(operation, 8022, null,
                            sshKey, "$src/", "$dst/", providers.commandExecutor)
                    rsync.run()
                }
            } finally {
                providers.storage.unmountOperationVolumes(repo, operation.operation.id)
            }

            operation.addProgress(ProgressEntry(type = ProgressEntry.Type.START, message = "Removing remote endpoint"))
            if (operation.operation.type == Operation.Type.PUSH) {
                response = engine.container().sync(operationRef, AppDataSyncParameters())
                waitForJob(engine, response)

                val source = findInResult(engine.source().list()) {
                    it.getString("container") == operationRef
                }
                response = engine.source().disable(source!!.getString("reference"),
                        SourceDisableParameters())
                waitForJob(engine, response)
            } else {
                response = engine.container().delete(operationRef, DeleteParameters())
                waitForJob(engine, response)
            }
            deleteOperation = false
            operation.addProgress(ProgressEntry(type = ProgressEntry.Type.END))
        } catch (t: Throwable) {
            try {
                if (deleteOperation) {
                    response = engine.container().delete(operationRef, DeleteParameters())
                    waitForJob(engine, response)
                }
            } catch (t2: Throwable) {
                log.error("error while trying to cleanup failed operation ${operation.operation.id} on remote ${operation.remote.name}", t2) // Ignore
            }
            throw t
        }
    }
}
