/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.engine

import com.delphix.sdk.Delphix
import com.delphix.sdk.Http
import com.delphix.sdk.objects.DeleteParameters
import io.kotlintest.SkipTestException
import io.kotlintest.Spec
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.titandata.EndToEndTest
import io.titandata.client.infrastructure.ClientException
import io.titandata.models.Commit
import io.titandata.models.ProgressEntry
import io.titandata.models.Repository
import io.titandata.models.VolumeCreateRequest
import io.titandata.models.VolumeMountRequest
import io.titandata.models.VolumeRequest
import io.titandata.serialization.RemoteUtil
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import org.json.JSONObject

class EngineWorkflowTest : EndToEndTest() {

    fun waitForJob(engine: Delphix, result: JSONObject) {
        val actionResult: JSONObject = engine.action().read(result.getString("action")).getJSONObject("result")
        if (actionResult.optString("state") != "COMPLETED") {
            EngineRemoteProvider.log.debug("waiting for job ${result.getString("job")}")
            var job: JSONObject = engine.job().read(result.getString("job")).getJSONObject("result")
            while (job.optString("jobState") == "RUNNING") {
                Thread.sleep(5000)
                job = engine.job().read(result.getString("job")).getJSONObject("result")
            }

            if (job.optString("jobState") != "COMPLETED") {
                throw Exception("engine job ${job.getString("reference")} failed")
            }
        }
    }

    private fun clearEngine() {
        val formatter = DateTimeFormatter.ISO_DATE_TIME
        try {
            val remote = getRemote()
            val engine = Delphix(Http("http://${remote.address}"))
            engine.login(remote.username, remote.password!!)

            val raw = engine.container().list().getJSONArray("result").map { it -> it as JSONObject }
            val databases = raw.sortedByDescending { OffsetDateTime.parse(it.getString("creationTime"), formatter) }
            for (d in databases) {
                if (d.getString("name") != "titan") {
                    val result = engine.container().delete(d.getString("reference"), DeleteParameters(force = true))
                    waitForJob(engine, result)
                }
            }
        } catch (e: Exception) {
            // Ignore
        }
    }

    override fun beforeSpec(spec: Spec) {
        dockerUtil.stopServer()
        clearEngine()
        dockerUtil.startServer()
        dockerUtil.waitForServer()
    }

    override fun afterSpec(spec: Spec) {
        dockerUtil.stopServer(ignoreExceptions = false)
        clearEngine()
    }

    private val remoteUtil = RemoteUtil()

    private fun getRemote(repo: String = "foo", name: String = "origin"): EngineRemote {
        val connection = System.getProperty("engine.connection")
                ?: throw SkipTestException("'engine.connection' must be specified with -P")
        return remoteUtil.parseUri("engine://$connection/$repo", name, mapOf()) as EngineRemote
    }

    init {
        "can connect to engine" {
            val remote = getRemote()
            val engine = Delphix(Http("http://${remote.address}"))
            engine.login(remote.username, remote.password!!)
        }

        "create new repository succeeds" {
            val repo = Repository(
                    name = "foo",
                    properties = mapOf()
            )
            val newRepo = repoApi.createRepository(repo)
            newRepo.name shouldBe "foo"
        }

        "create volume succeeds" {
            val repo = VolumeCreateRequest(
                    name = "foo/vol",
                    opts = mapOf()
            )
            val response = volumeApi.createVolume(repo)
            response.err shouldBe ""
        }

        "mount volume succeeds" {
            val response = volumeApi.mountVolume(VolumeMountRequest(name = "foo/vol", ID = "id"))
            response.mountpoint shouldBe "/var/lib/test/mnt/foo/vol"
        }

        "create and write volume file succeeds" {
            dockerUtil.writeFile("foo/vol", "testfile", "Hello")
            val result = dockerUtil.readFile("foo/vol", "testfile")
            result shouldBe "Hello\n"
        }

        "create commit succeeds" {
            val commit = commitApi.createCommit("foo", Commit(id = "id", properties = mapOf("a" to "b")))
            commit.id shouldBe "id"
            commit.properties["a"] shouldBe "b"
        }

        "add engine remote succeeds" {
            val remote = getRemote()
            remoteApi.createRemote("foo", remote)
        }

        "list remote commits returns an error" {
            val e = shouldThrow<ClientException> {
                remoteApi.listRemoteCommits("foo", "origin", EngineParameters())
            }
            e.code shouldBe "NoSuchObjectException"
        }

        "push commit succeeds" {
            val op = operationApi.push("foo", "origin", "id", EngineParameters())
            val progress = waitForOperation(op.id)
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Pushing id to 'origin'"
            progress[1].type shouldBe ProgressEntry.Type.START
            progress[1].message shouldBe "Creating remote repository"
            progress[2].type shouldBe ProgressEntry.Type.END
            progress[3].type shouldBe ProgressEntry.Type.START
            progress[3].message shouldBe "Creating remote endpoint"
            progress[4].type shouldBe ProgressEntry.Type.END
            progress[5].type shouldBe ProgressEntry.Type.START
            progress[5].message shouldBe "Syncing vol"
            progress[5].percent shouldBe 0
            var idx = progress.indexOfLast { it.type == ProgressEntry.Type.PROGRESS }
            progress[idx].percent shouldBe 100
            progress[idx + 1].type shouldBe ProgressEntry.Type.END
            progress[idx + 2].type shouldBe ProgressEntry.Type.START
            progress[idx + 2].message shouldBe "Removing remote endpoint"
            progress[idx + 3].type shouldBe ProgressEntry.Type.END
            progress[idx + 4].type shouldBe ProgressEntry.Type.COMPLETE
        }

        "list remote commits returns pushed commit" {
            val commits = remoteApi.listRemoteCommits("foo", "origin", EngineParameters())
            commits.size shouldBe 1
            commits[0].id shouldBe "id"
            commits[0].properties["a"] shouldBe "b"
        }

        "push of same commit fails" {
            val exception = shouldThrow<ClientException> {
                operationApi.push("foo", "origin", "id", EngineParameters())
            }
            exception.code shouldBe "ObjectExistsException"
        }

        "delete local commit succeeds" {
            commitApi.deleteCommit("foo", "id")
        }

        "list local commits is empty" {
            val result = commitApi.listCommits("foo")
            result.size shouldBe 0
        }

        "write new local value succeeds" {
            dockerUtil.writeFile("foo/vol", "testfile", "Goodbye")
            val result = dockerUtil.readFile("foo/vol", "testfile")
            result shouldBe "Goodbye\n"
        }

        "pull original commit succeeds" {
            val op = operationApi.pull("foo", "origin", "id", EngineParameters())
            val progress = waitForOperation(op.id)
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Pulling id from 'origin'"
            progress[1].type shouldBe ProgressEntry.Type.START
            progress[1].message shouldBe "Creating remote endpoint"
            progress[2].type shouldBe ProgressEntry.Type.END
            progress[3].type shouldBe ProgressEntry.Type.START
            progress[3].message shouldBe "Syncing vol"
            progress[3].percent shouldBe 0
            var idx = progress.indexOfLast { it.type == ProgressEntry.Type.PROGRESS }
            progress[idx].percent shouldBe 100
            progress[idx + 1].type shouldBe ProgressEntry.Type.END
            progress[idx + 2].type shouldBe ProgressEntry.Type.START
            progress[idx + 2].message shouldBe "Removing remote endpoint"
            progress[idx + 3].type shouldBe ProgressEntry.Type.END
            progress[idx + 4].type shouldBe ProgressEntry.Type.COMPLETE
        }

        "checkout commit succeeds" {
            volumeApi.unmountVolume(VolumeMountRequest(name = "foo/vol"))
            commitApi.checkoutCommit("foo", "id")
            volumeApi.mountVolume(VolumeMountRequest(name = "foo/vol"))
        }

        "original file contents are present" {
            val result = dockerUtil.readFile("foo/vol", "testfile")
            result shouldBe "Hello\n"
        }

        "remove remote succeeds" {
            remoteApi.deleteRemote("foo", "origin")
        }

        "add remote without password succeeds" {
            val defaultRemote = getRemote()
            val remote = EngineRemote(address = defaultRemote.address, username = defaultRemote.username,
                    name = "origin", repository = defaultRemote.repository)
            remoteApi.createRemote("foo", remote)
        }

        "list commits with password succeeds" {
            val commits = remoteApi.listRemoteCommits("foo", "origin", EngineParameters(password = getRemote().password))
            commits.size shouldBe 1
            commits[0].id shouldBe "id"
            commits[0].properties["a"] shouldBe "b"
        }

        "list commits without password fails" {
            val exception = shouldThrow<ClientException> {
                remoteApi.listRemoteCommits("foo", "origin", EngineParameters())
            }
            exception.code shouldBe "IllegalArgumentException"
        }

        "delete volume succeeds" {
            volumeApi.unmountVolume(VolumeMountRequest(name = "foo/vol"))
            volumeApi.removeVolume(VolumeRequest(name = "foo/vol"))
        }

        "delete repository succeeds" {
            repoApi.deleteRepository("foo")
        }
    }
}
