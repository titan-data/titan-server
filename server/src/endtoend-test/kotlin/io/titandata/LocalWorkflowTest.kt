/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata

import io.kotlintest.Spec
import io.kotlintest.matchers.types.shouldBeInstanceOf
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.titandata.client.infrastructure.ClientException
import io.titandata.models.Commit
import io.titandata.models.NopParameters
import io.titandata.models.NopRemote
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Repository
import io.titandata.models.VolumeCreateRequest
import io.titandata.models.VolumeMountRequest
import io.titandata.models.VolumeRequest
import java.time.Duration
import kotlinx.coroutines.time.delay

class LocalWorkflowTest : EndToEndTest() {

    var currentOp = Operation(id = "none", commitId = "commit", remote = "remote",
            state = Operation.State.COMPLETE, type = Operation.Type.PUSH)

    override fun beforeSpec(spec: Spec) {
        dockerUtil.stopServer()
        dockerUtil.startServer()
        dockerUtil.waitForServer()
    }

    override fun afterSpec(spec: Spec) {
        dockerUtil.stopServer(ignoreExceptions = false)
    }

    init {
        "repository list is empty" {
            val repositories = repoApi.listRepositories()
            repositories.size shouldBe 0
        }

        "create new repository succeeds" {
            val repo = Repository(
                    name = "foo",
                    properties = mapOf("a" to "b")
            )
            val newRepo = repoApi.createRepository(repo)
            newRepo.name shouldBe "foo"
            newRepo.properties["a"] shouldBe "b"
        }

        "get created repository succeeds" {
            val repo = repoApi.getRepository("foo")
            repo.name shouldBe "foo"
            repo.properties["a"] shouldBe "b"
        }

        "new repository shows up in list output" {
            val repositories = repoApi.listRepositories()
            repositories.size shouldBe 1
            val repo = repositories.get(0)
            repo.name shouldBe "foo"
            repo.properties["a"] shouldBe "b"
        }

        "create repository with duplicate name fails" {
            val repo = Repository(
                    name = "foo",
                    properties = mapOf("a" to "b")
            )
            val exception = shouldThrow<ClientException> {
                repoApi.createRepository(repo)
            }
            exception.code shouldBe "ObjectExistsException"
        }

        "create volume succeeds" {
            val repo = VolumeCreateRequest(
                    name = "foo/vol",
                    opts = mapOf("a" to "b")
            )
            val response = volumeApi.createVolume(repo)
            response.err shouldBe ""
        }

        "create volume for unknown repository fails" {
            val exception = shouldThrow<ClientException> {
                volumeApi.createVolume(VolumeCreateRequest(name = "bar/vol", opts = mapOf()))
            }
            exception.message shouldNotBe ""
        }

        "create duplicate volume fails" {
            val exception = shouldThrow<ClientException> {
                volumeApi.createVolume(VolumeCreateRequest(name = "foo/vol", opts = mapOf()))
            }
            exception.message shouldNotBe ""
        }

        "get volume capabilities returns local scope" {
            val response = volumeApi.getCapabilities()
            response.capabilities!!.scope shouldBe "local"
        }

        "plugin activate returns VolumeDriver" {
            val response = volumeApi.pluginActivate()
            response.implements.size shouldBe 1
            response.implements[0] shouldBe "VolumeDriver"
        }

        "get volume succeeds" {
            val response = volumeApi.getVolume(VolumeRequest(name = "foo/vol"))
            response.volume shouldNotBe null
            response.volume.mountpoint shouldBe "/var/lib/test/mnt/foo/vol"
            response.volume.name shouldBe "foo/vol"
            response.volume.properties shouldNotBe null
            response.volume.properties!!["a"] shouldBe "b"
        }

        "get non-existent volume fails" {
            shouldThrow<ClientException> {
                volumeApi.getVolume(VolumeRequest(name = "bar/vol"))
            }
        }

        "volume appears in volume list" {
            val response = volumeApi.listVolumes()
            response.volumes.size shouldBe 1
            response.volumes[0].name shouldBe "foo/vol"
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

        "create duplicate commit fails" {
            val exception = shouldThrow<ClientException> {
                commitApi.createCommit("foo", Commit(id = "id", properties = mapOf("a" to "b")))
            }
            exception.code shouldBe "ObjectExistsException"
        }

        "get commit succeeds" {
            val commit = commitApi.getCommit("foo", "id")
            commit.id shouldBe "id"
            commit.properties["a"] shouldBe "b"
        }

        "get non-existent commit fails" {
            val exception = shouldThrow<ClientException> {
                commitApi.getCommit("foo", "id2")
            }
            exception.code shouldBe "NoSuchObjectException"
        }

        "get commit status succeeds" {
            val status = commitApi.getCommitStatus("foo", "id")
            status.logicalSize shouldNotBe 0
            status.actualSize shouldNotBe 0
            status.uniqueSize shouldBe 0
        }

        "commit shows up in list" {
            val commits = commitApi.listCommits("foo")
            commits.size shouldBe 1
            commits[0].id shouldBe "id"
        }

        "write new local value succeeds" {
            dockerUtil.writeFile("foo/vol", "testfile", "Goodbye")
            val result = dockerUtil.readFile("foo/vol", "testfile")
            result shouldBe "Goodbye\n"
        }

        "unmount volume succeeds" {
            volumeApi.unmountVolume(VolumeMountRequest(name = "foo/vol"))
        }

        "unmount volume is idempotent" {
            volumeApi.unmountVolume(VolumeMountRequest(name = "foo/vol"))
        }

        "checkout commit and old contents are present" {
            commitApi.checkoutCommit("foo", "id")
            volumeApi.mountVolume(VolumeMountRequest(name = "foo/vol"))
            val result = dockerUtil.readFile("foo/vol", "testfile")
            result shouldBe "Hello\n"
        }

        "add remote succeeds" {
            val result = remoteApi.createRemote("foo", NopRemote(name = "a"))
            result.name shouldBe "a"
        }

        "get remote succeeds" {
            val result = remoteApi.getRemote("foo", "a")
            result.shouldBeInstanceOf<NopRemote>()
            result.name shouldBe "a"
        }

        "add duplicate remote fails" {
            val exception = shouldThrow<ClientException> {
                remoteApi.createRemote("foo", NopRemote(name = "a"))
            }
            exception.code shouldBe "ObjectExistsException"
        }

        "remote shows up in list" {
            val result = remoteApi.listRemotes("foo")
            result.size shouldBe 1
            result[0].name shouldBe "a"
        }

        "list remote commits succeeds" {
            val result = remoteApi.listRemoteCommits("foo", "a", NopParameters())
            result.size shouldBe 0
        }

        "get remote commit succeeds" {
            val result = remoteApi.getRemoteCommit("foo", "a", "hash", NopParameters())
            result.id shouldBe "hash"
        }

        "update remote name succeeds" {
            remoteApi.updateRemote("foo", "a", NopRemote(name = "b"))
            val result = remoteApi.getRemote("foo", "b")
            result.name shouldBe "b"
            result.provider shouldBe "nop"
        }

        "list of operations is empty" {
            val result = operationApi.listOperations("foo")
            result.size shouldBe 0
        }

        "push creates new operation" {
            currentOp = operationApi.push("foo", "b", "id", NopParameters())
            currentOp.commitId shouldBe "id"
            currentOp.remote shouldBe "b"
            currentOp.type shouldBe Operation.Type.PUSH
        }

        "get push operation succeeds" {
            val result = operationApi.getOperation("foo", currentOp.id)
            result.id shouldBe currentOp.id
            result.commitId shouldBe currentOp.commitId
            result.remote shouldBe currentOp.remote
            result.type shouldBe currentOp.type
        }

        "list operations shows push operation" {
            val result = operationApi.listOperations("foo")
            result.size shouldBe 1
            result[0].id shouldBe currentOp.id
        }

        "get push operation progress succeeds" {
            delay(Duration.ofMillis(500))
            val result = operationApi.getOperation("foo", currentOp.id)
            result.state shouldBe Operation.State.COMPLETE
            val progress = operationApi.getProgress("foo", currentOp.id)
            progress.size shouldBe 4
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Pushing id to 'b'"
            progress[1].type shouldBe ProgressEntry.Type.START
            progress[1].message shouldBe "Running operation"
            progress[2].type shouldBe ProgressEntry.Type.END
            progress[3].type shouldBe ProgressEntry.Type.COMPLETE
        }

        "push operation no longer exists" {
            val exception = shouldThrow<ClientException> {
                operationApi.getOperation("foo", currentOp.id)
            }
            exception.code shouldBe "NoSuchObjectException"
        }

        "push operation no longer in list of operations" {
            val result = operationApi.listOperations("foo")
            result.size shouldBe 0
        }

        "pull creates new operation" {
            currentOp = operationApi.pull("foo", "b", "id2", NopParameters())
            currentOp.commitId shouldBe "id2"
            currentOp.remote shouldBe "b"
            currentOp.type shouldBe Operation.Type.PULL
        }

        "get pull operation succeeds" {
            val result = operationApi.getOperation("foo", currentOp.id)
            result.id shouldBe currentOp.id
            result.commitId shouldBe currentOp.commitId
            result.remote shouldBe currentOp.remote
            result.type shouldBe currentOp.type
        }

        "list operations shows pull operation" {
            val result = operationApi.listOperations("foo")
            result.size shouldBe 1
            result[0].id shouldBe currentOp.id
        }

        "get pull progress succeeds" {
            delay(Duration.ofMillis(500))
            val result = operationApi.getOperation("foo", currentOp.id)
            result.state shouldBe Operation.State.COMPLETE
            val progress = operationApi.getProgress("foo", currentOp.id)
            progress.size shouldBe 4
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Pulling id2 from 'b'"
            progress[1].type shouldBe ProgressEntry.Type.START
            progress[1].message shouldBe "Running operation"
            progress[2].type shouldBe ProgressEntry.Type.END
            progress[3].type shouldBe ProgressEntry.Type.COMPLETE
        }

        "pull operation no longer exists" {
            val exception = shouldThrow<ClientException> {
                operationApi.getOperation("foo", currentOp.id)
            }
            exception.code shouldBe "NoSuchObjectException"
        }

        "pulled commit exists" {
            val commit = commitApi.getCommit("foo", "id2")
            commit.id shouldBe "id2"
        }

        "push non-existent commit fails" {
            val exception = shouldThrow<ClientException> {
                operationApi.push("foo", "b", "id3", NopParameters())
            }
            exception.code shouldBe "NoSuchObjectException"
        }

        "aborted operation is marked aborted" {
            currentOp = operationApi.push("foo", "b", "id", NopParameters(delay = 10))
            currentOp.state shouldBe Operation.State.RUNNING
            delay(Duration.ofMillis(1000))
            operationApi.deleteOperation("foo", currentOp.id)
            delay(Duration.ofMillis(1000))
            val result = operationApi.getOperation("foo", currentOp.id)
            result.state shouldBe Operation.State.ABORTED
            val progress = operationApi.getProgress("foo", currentOp.id)
            progress.size shouldBe 3
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Pushing id to 'b'"
            progress[1].type shouldBe ProgressEntry.Type.START
            progress[1].message shouldBe "Running operation"
            progress[2].type shouldBe ProgressEntry.Type.ABORT
        }

        "delete commit succeeds" {
            commitApi.deleteCommit("foo", "id")
            val exception = shouldThrow<ClientException> {
                commitApi.getCommit("foo", "id")
            }
            exception.code shouldBe "NoSuchObjectException"
        }

        "delete remote succeeds" {
            remoteApi.deleteRemote("foo", "b")
            val exception = shouldThrow<ClientException> {
                remoteApi.getRemote("foo", "b")
            }
            exception.code shouldBe "NoSuchObjectException"
        }

        "delete non-existent remote fails" {
            val exception = shouldThrow<ClientException> {
                remoteApi.deleteRemote("foo", "b")
            }
            exception.code shouldBe "NoSuchObjectException"
        }

        "delete volume succeeds" {
            dockerUtil.pathExists("/var/lib/test/mnt/foo/vol") shouldBe true
            volumeApi.unmountVolume(VolumeMountRequest(name = "foo/vol"))
            volumeApi.removeVolume(VolumeRequest(name = "foo/vol"))
        }

        "volume directory no longer exists" {
            dockerUtil.pathExists("/var/lib/test/mnt/foo/vol") shouldBe false
        }

        "delete repository succeeds" {
            repoApi.deleteRepository("foo")
        }

        "repository directory no longer exists" {
            dockerUtil.pathExists("/var/lib/test/mnt/foo") shouldBe false
        }
    }
}
