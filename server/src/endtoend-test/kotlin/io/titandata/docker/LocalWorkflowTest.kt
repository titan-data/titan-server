/*
* Copyright The Titan Project Contributors.
 */

package io.titandata.docker

import io.kotlintest.Spec
import io.kotlintest.matchers.string.shouldStartWith
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.titandata.EndToEndTest
import io.titandata.client.infrastructure.ClientException
import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Repository
import io.titandata.models.Volume
import java.time.Duration
import kotlinx.coroutines.time.delay

class LocalWorkflowTest : EndToEndTest() {

    var currentOp = Operation(id = "none", commitId = "commit", remote = "remote",
            state = Operation.State.COMPLETE, type = Operation.Type.PUSH)

    var volumeMountpoint: String? = null
    val remote = Remote("nop", "a")
    val params = remoteUtil.getParameters(remote)

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
            val response = volumeApi.createVolume("foo", Volume("vol", mapOf("a" to "b")))
            response.name shouldBe "vol"
        }

        "create volume for unknown repository fails" {
            val exception = shouldThrow<ClientException> {
                volumeApi.createVolume("bar", Volume("vol"))
            }
            exception.code shouldBe "NoSuchObjectException"
        }

        "create duplicate volume fails" {
            val exception = shouldThrow<ClientException> {
                volumeApi.createVolume("foo", Volume("vol"))
            }
            exception.code shouldBe "ObjectExistsException"
        }

        "get volume succeeds" {
            val vol = volumeApi.getVolume("foo", "vol")
            volumeMountpoint = vol.config["mountpoint"] as String
            volumeMountpoint shouldStartWith "/var/lib/test/mnt/"
            vol.name shouldBe "vol"
            vol.properties["a"] shouldBe "b"
        }

        "get non-existent volume fails" {
            val exception = shouldThrow<ClientException> {
                volumeApi.getVolume("bar", "vol")
            }
            exception.code shouldBe "NoSuchObjectException"
        }

        "volume appears in volume list" {
            val volumes = volumeApi.listVolumes("foo")
            volumes.size shouldBe 1
            volumes[0].name shouldBe "vol"
        }

        "mount volume succeeds" {
            volumeApi.activateVolume("foo", "vol")
        }

        "create and write volume file succeeds" {
            dockerUtil.writeFile("foo", "vol", "testfile", "Hello")
            val result = dockerUtil.readFile("foo", "vol", "testfile")
            result shouldBe "Hello\n"
        }

        "last commit should not be set" {
            val status = repoApi.getRepositoryStatus("foo")
            status.sourceCommit shouldBe null
            status.lastCommit shouldBe null
        }

        "create commit succeeds" {
            val commit = commitApi.createCommit("foo", Commit(id = "id",
                    properties = mapOf("tags" to mapOf("a" to "b", "c" to "d"))))
            commit.id shouldBe "id"
            getTag(commit, "a") shouldBe "b"
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
            getTag(commit, "a") shouldBe "b"
        }

        "get non-existent commit fails" {
            val exception = shouldThrow<ClientException> {
                commitApi.getCommit("foo", "id2")
            }
            exception.code shouldBe "NoSuchObjectException"
        }

        "update commit succeeds" {
            val newCommit = Commit(id = "id", properties = mapOf("tags" to mapOf("a" to "B", "c" to "d")))
            commitApi.updateCommit("foo", newCommit)
            getTag(newCommit, "a") shouldBe "B"
            val commit = commitApi.getCommit("foo", "id")
            getTag(commit, "a") shouldBe "B"
        }

        "get commit status succeeds" {
            val status = commitApi.getCommitStatus("foo", "id")
            status.logicalSize shouldNotBe 0
            status.actualSize shouldNotBe 0
            status.uniqueSize shouldBe 0
        }

        "get repository status succeeds" {
            val status = repoApi.getRepositoryStatus("foo")
            status.sourceCommit shouldBe "id"
            status.lastCommit shouldBe "id"
            status.volumeStatus.size shouldBe 1
            status.volumeStatus[0].name shouldBe "vol"
            status.volumeStatus[0].actualSize shouldNotBe 0
            status.volumeStatus[0].logicalSize shouldNotBe 0
        }

        "commit shows up in list" {
            val commits = commitApi.listCommits("foo")
            commits.size shouldBe 1
            commits[0].id shouldBe "id"
        }

        "commit not present when filtered out" {
            val commits = commitApi.listCommits("foo", listOf("a=c"))
            commits.size shouldBe 0
        }

        "commit present when part of filter" {
            val commits = commitApi.listCommits("foo", listOf("a=B"))
            commits.size shouldBe 1
            commits[0].id shouldBe "id"
        }

        "commit present when part of compound filter" {
            val commits = commitApi.listCommits("foo", listOf("a=B", "c"))
            commits.size shouldBe 1
            commits[0].id shouldBe "id"
        }

        "write new local value succeeds" {
            dockerUtil.writeFile("foo", "vol", "testfile", "Goodbye")
            val result = dockerUtil.readFile("foo", "vol", "testfile")
            result shouldBe "Goodbye\n"
        }

        "unmount volume succeeds" {
            volumeApi.deactivateVolume("foo", "vol")
        }

        "unmount volume is idempotent" {
            volumeApi.deactivateVolume("foo", "vol")
        }

        "checkout commit and old contents are present" {
            commitApi.checkoutCommit("foo", "id")
            volumeApi.activateVolume("foo", "vol")
            val result = dockerUtil.readFile("foo", "vol", "testfile")
            result shouldBe "Hello\n"
        }

        "volume is mounted at a new location" {
            val vol = volumeApi.getVolume("foo", "vol")
            vol.config["mountpoint"] shouldNotBe volumeMountpoint
            volumeMountpoint = vol.config["mountpoint"] as String
        }

        "get repository status indicates source commit" {
            val status = repoApi.getRepositoryStatus("foo")
            status.sourceCommit shouldBe "id"
            status.lastCommit shouldBe "id"
        }

        "add remote succeeds" {
            val result = remoteApi.createRemote("foo", remote)
            result.name shouldBe "a"
        }

        "get remote succeeds" {
            val result = remoteApi.getRemote("foo", "a")
            result.provider shouldBe "nop"
            result.name shouldBe "a"
        }

        "add duplicate remote fails" {
            val exception = shouldThrow<ClientException> {
                remoteApi.createRemote("foo", remote)
            }
            exception.code shouldBe "ObjectExistsException"
        }

        "remote shows up in list" {
            val result = remoteApi.listRemotes("foo")
            result.size shouldBe 1
            result[0].name shouldBe "a"
        }

        "list remote commits succeeds" {
            val result = remoteApi.listRemoteCommits("foo", "a", params)
            result.size shouldBe 0
        }

        "get remote commit succeeds" {
            val result = remoteApi.getRemoteCommit("foo", "a", "hash", params)
            result.id shouldBe "hash"
        }

        "update remote name succeeds" {
            remoteApi.updateRemote("foo", "a", Remote("nop", "b"))
            val result = remoteApi.getRemote("foo", "b")
            result.name shouldBe "b"
            result.provider shouldBe "nop"
        }

        "list of operations is empty" {
            val result = operationApi.listOperations("foo")
            result.size shouldBe 0
        }

        "push creates new operation" {
            currentOp = operationApi.push("foo", "b", "id", params)
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
            delay(Duration.ofMillis(1000))
            val result = operationApi.getOperation("foo", currentOp.id)
            result.state shouldBe Operation.State.COMPLETE
            val progress = operationApi.getProgress("foo", currentOp.id)
            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Pushing id to 'b'"
            progress[1].type shouldBe ProgressEntry.Type.COMPLETE
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
            currentOp = operationApi.pull("foo", "b", "id2", params)
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
            delay(Duration.ofMillis(1000))
            val result = operationApi.getOperation("foo", currentOp.id)
            result.state shouldBe Operation.State.COMPLETE
            val progress = operationApi.getProgress("foo", currentOp.id)
            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Pulling id2 from 'b'"
            progress[1].type shouldBe ProgressEntry.Type.COMPLETE
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

        "list commits shows two commits" {
            val commits = commitApi.listCommits("foo")
            commits.size shouldBe 2
        }

        "list commits filters out commit" {
            val commits = commitApi.listCommits("foo", listOf("a=B"))
            commits.size shouldBe 1
            commits[0].id shouldBe "id"
        }

        "push non-existent commit fails" {
            val exception = shouldThrow<ClientException> {
                operationApi.push("foo", "b", "id3", params)
            }
            exception.code shouldBe "NoSuchObjectException"
        }

        "aborted operation is marked aborted" {
            val props = params.properties.toMutableMap()
            props["delay"] = 10
            currentOp = operationApi.push("foo", "b", "id", RemoteParameters(params.provider, props))
            currentOp.state shouldBe Operation.State.RUNNING
            delay(Duration.ofMillis(1000))
            operationApi.deleteOperation("foo", currentOp.id)
            delay(Duration.ofMillis(1000))
            val result = operationApi.getOperation("foo", currentOp.id)
            result.state shouldBe Operation.State.ABORTED
            val progress = operationApi.getProgress("foo", currentOp.id)
            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Pushing id to 'b'"
            progress[1].type shouldBe ProgressEntry.Type.ABORT
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
            volumeApi.deactivateVolume("foo", "vol")
            volumeApi.deleteVolume("foo", "vol")
        }

        "delete repository succeeds" {
            repoApi.deleteRepository("foo")
        }
    }
}
