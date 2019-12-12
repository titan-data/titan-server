/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.ssh

import io.kotlintest.Spec
import io.kotlintest.TestCaseOrder
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.titandata.EndToEndTest
import io.titandata.client.infrastructure.ClientException
import io.titandata.client.infrastructure.ServerException
import io.titandata.models.Commit
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Repository
import io.titandata.models.Volume
import io.titandata.serialization.RemoteUtil

class SshWorkflowTest : EndToEndTest() {

    val params = RemoteParameters("ssh")

    override fun beforeSpec(spec: Spec) {
        dockerUtil.stopServer()
        dockerUtil.startServer()
        dockerUtil.waitForServer()

        dockerUtil.stopSsh()
        dockerUtil.startSsh()
        dockerUtil.waitForSsh()
    }

    override fun afterSpec(spec: Spec) {
        dockerUtil.stopSsh(ignoreExceptions = false)
        dockerUtil.stopServer(ignoreExceptions = false)
    }

    override fun testCaseOrder() = TestCaseOrder.Sequential

    fun getResource(name: String): String {
        return this.javaClass.getResource(name).readText()
    }

    init {

        "create new repository succeeds" {
            val repo = Repository(
                    name = "foo",
                    properties = mapOf()
            )
            val newRepo = repoApi.createRepository(repo)
            newRepo.name shouldBe "foo"
        }

        "create and mount volume succeeds" {
            volumeApi.createVolume("foo", Volume("vol"))
            volumeApi.activateVolume("foo", "vol")
        }

        "create and write volume file succeeds" {
            dockerUtil.writeFile("foo", "vol", "testfile", "Hello")
            val result = dockerUtil.readFile("foo", "vol", "testfile")
            result shouldBe "Hello\n"
        }

        "create commit succeeds" {
            val commit = commitApi.createCommit("foo", Commit(id = "id",
                    properties = mapOf("tags" to mapOf("a" to "b", "c" to "d"))))
            commit.id shouldBe "id"
            getTag(commit, "a") shouldBe "b"
            getTag(commit, "c") shouldBe "d"
        }

        "add ssh remote succeeds" {
            dockerUtil.mkdirSsh("/bar")

            val remote = RemoteUtil().parseUri("${dockerUtil.getSshUri()}/bar", "origin", mapOf())
            remote.properties["address"] shouldBe dockerUtil.getSshHost()
            remote.properties["password"] shouldBe "test"
            remote.properties["username"] shouldBe "test"
            remote.properties["port"] shouldBe 22
            remote.name shouldBe "origin"

            remoteApi.createRemote("foo", remote)
        }

        "list remote commits returns empty list" {
            val commits = remoteApi.listRemoteCommits("foo", "origin", params)
            commits.size shouldBe 0
        }

        "get non-existent remote commit fails" {
            val exception = shouldThrow<ClientException> {
                remoteApi.getRemoteCommit("foo", "origin", "id", params)
            }
            exception.code shouldBe "NoSuchObjectException"
        }

        "push commit succeeds" {
            val op = operationApi.push("foo", "origin", "id", params)
            waitForOperation(op.id)
        }

        "list remote commits returns pushed commit" {
            val commits = remoteApi.listRemoteCommits("foo", "origin", params)
            commits.size shouldBe 1
            commits[0].id shouldBe "id"
            getTag(commits[0], "a") shouldBe "b"
        }

        "list remote commits filters out commit" {
            val commits = remoteApi.listRemoteCommits("foo", "origin", params, listOf("e"))
            commits.size shouldBe 0
        }

        "list remote commits filters include commit" {
            val commits = remoteApi.listRemoteCommits("foo", "origin", params, listOf("a=b", "c=d"))
            commits.size shouldBe 1
            commits[0].id shouldBe "id"
        }

        "remote file contents is correct" {
            val content = dockerUtil.readFileSsh("/bar/id/data/vol/testfile")
            content shouldBe "Hello\n"
        }

        "push of same commit fails" {
            val exception = shouldThrow<ClientException> {
                operationApi.push("foo", "origin", "id", params)
            }
            exception.code shouldBe "ObjectExistsException"
        }

        "update commit succeeds" {
            val newCommit = Commit(id = "id", properties = mapOf("tags" to mapOf("a" to "B", "c" to "d")))
            commitApi.updateCommit("foo", newCommit)
            getTag(newCommit, "a") shouldBe "B"
            val commit = commitApi.getCommit("foo", "id")
            getTag(commit, "a") shouldBe "B"
        }

        "push commit metadata succeeds" {
            val op = operationApi.push("foo", "origin", "id", params, true)
            waitForOperation(op.id)
        }

        "remote commit metadata updated" {
            val commit = commitApi.getCommit("foo", "id")
            commit.id shouldBe "id"
            getTag(commit, "a") shouldBe "B"
            getTag(commit, "c") shouldBe "d"
        }

        "delete local commit succeeds" {
            commitApi.deleteCommit("foo", "id")
        }

        "list local commits is empty" {
            val result = commitApi.listCommits("foo")
            result.size shouldBe 0
        }

        "write new local value succeeds" {
            dockerUtil.writeFile("foo", "vol", "testfile", "Goodbye")
            val result = dockerUtil.readFile("foo", "vol", "testfile")
            result shouldBe "Goodbye\n"
        }

        "pull original commit succeeds" {
            val op = operationApi.pull("foo", "origin", "id", params)
            waitForOperation(op.id)
        }

        "pull same commit fails" {
            val exception = shouldThrow<ClientException> {
                operationApi.pull("foo", "origin", "id", params)
            }
            exception.code shouldBe "ObjectExistsException"
        }

        "pull of metadata only succeeds" {
            val op = operationApi.pull("foo", "origin", "id", params, true)
            waitForOperation(op.id)
        }

        "checkout commit succeeds" {
            volumeApi.deactivateVolume("foo", "vol")
            commitApi.checkoutCommit("foo", "id")
            volumeApi.activateVolume("foo", "vol")
        }

        "original file contents are present" {
            val result = dockerUtil.readFile("foo", "vol", "testfile")
            result shouldBe "Hello\n"
        }

        "remove remote succeeds" {
            remoteApi.deleteRemote("foo", "origin")
        }

        "add remote without password succeeds" {
            val remote = Remote("ssh", "origin", mapOf("address" to dockerUtil.getSshHost(), "username" to "test",
                    "path" to "/bar"))
            remote.properties["address"] shouldBe dockerUtil.getSshHost()
            remote.properties["password"] shouldBe null
            remote.properties["username"] shouldBe "test"
            remote.properties["port"] shouldBe null
            remote.name shouldBe "origin"

            remoteApi.createRemote("foo", remote)
        }

        "list commits with password succeeds" {
            val commits = remoteApi.listRemoteCommits("foo", "origin", RemoteParameters("ssh", mapOf("password" to "test")))
            commits.size shouldBe 1
            commits[0].id shouldBe "id"
        }

        "list commits without password fails" {
            val exception = shouldThrow<ClientException> {
                remoteApi.listRemoteCommits("foo", "origin", params)
            }
            exception.code shouldBe "IllegalArgumentException"
        }

        "list commits with incorrect password fails" {
            val exception = shouldThrow<ServerException> {
                remoteApi.listRemoteCommits("foo", "origin", RemoteParameters("ssh", mapOf("password" to "r00t")))
            }
            exception.code shouldBe "CommandException"
        }

        "copy SSH key to server succeeds" {
            val key = getResource("/id_rsa.pub")
            dockerUtil.writeFileSsh("/home/test/.ssh/authorized_keys", key)
        }

        "list commits with key succeeds" {
            val key = getResource("/id_rsa")
            val commits = remoteApi.listRemoteCommits("foo", "origin", RemoteParameters("ssh", mapOf("key" to key)))
            commits.size shouldBe 1
            commits[0].id shouldBe "id"
        }

        "pull commit with key succeeds" {
            val key = getResource("/id_rsa")
            commitApi.deleteCommit("foo", "id")
            val op = operationApi.pull("foo", "origin", "id", RemoteParameters("ssh", mapOf("key" to key)))
            waitForOperation(op.id)
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
