/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata

import io.kotlintest.Spec
import io.kotlintest.matchers.string.shouldStartWith
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.titandata.client.infrastructure.ClientException
import io.titandata.models.Commit
import io.titandata.models.Repository
import io.titandata.models.docker.DockerVolumeCreateRequest
import io.titandata.models.docker.DockerVolumeMountRequest
import io.titandata.models.docker.DockerVolumeRequest

class DockerVolumeTest : EndToEndTest() {

    val dockerVolumeApi = DockerVolumeApi(url)

    var volumeMountpoint: String? = null

    override fun beforeSpec(spec: Spec) {
        dockerUtil.stopServer()
        dockerUtil.startServer()
        dockerUtil.waitForServer()
    }

    override fun afterSpec(spec: Spec) {
        dockerUtil.stopServer(ignoreExceptions = false)
    }

    init {
        "create new repository succeeds" {
            repoApi.createRepository(Repository("foo"))
        }

        "create volume succeeds" {
            val repo = DockerVolumeCreateRequest(
                    name = "foo/vol",
                    opts = mapOf("a" to "b")
            )
            val response = dockerVolumeApi.createVolume(repo)
            response.err shouldBe ""
        }

        "create volume for unknown repository fails" {
            val exception = shouldThrow<ClientException> {
                dockerVolumeApi.createVolume(DockerVolumeCreateRequest(name = "bar/vol", opts = mapOf()))
            }
            exception.message shouldNotBe ""
        }

        "create duplicate volume fails" {
            val exception = shouldThrow<ClientException> {
                dockerVolumeApi.createVolume(DockerVolumeCreateRequest(name = "foo/vol", opts = mapOf()))
            }
            exception.message shouldNotBe ""
        }

        "get volume capabilities returns local scope" {
            val response = dockerVolumeApi.getCapabilities()
            response.capabilities!!.scope shouldBe "local"
        }

        "plugin activate returns VolumeDriver" {
            val response = dockerVolumeApi.pluginActivate()
            response.implements.size shouldBe 1
            response.implements[0] shouldBe "VolumeDriver"
        }

        "get volume succeeds" {
            val response = dockerVolumeApi.getVolume(DockerVolumeRequest(name = "foo/vol"))
            response.volume shouldNotBe null
            response.volume.mountpoint shouldStartWith "/var/lib/test/mnt/"
            response.volume.name shouldBe "foo/vol"
            response.volume.properties shouldNotBe null
            response.volume.properties["a"] shouldBe "b"
        }

        "get non-existent volume fails" {
            shouldThrow<ClientException> {
                dockerVolumeApi.getVolume(DockerVolumeRequest(name = "bar/vol"))
            }
        }

        "volume appears in volume list" {
            val response = dockerVolumeApi.listVolumes()
            response.volumes.size shouldBe 1
            response.volumes[0].name shouldBe "foo/vol"
        }

        "mount volume succeeds" {
            val response = dockerVolumeApi.mountVolume(DockerVolumeMountRequest(name = "foo/vol", ID = "id"))
            response.mountpoint shouldStartWith "/var/lib/test/mnt/"
            volumeMountpoint = response.mountpoint
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

        "write new local value succeeds" {
            dockerUtil.writeFile("foo", "vol", "testfile", "Goodbye")
            val result = dockerUtil.readFile("foo", "vol", "testfile")
            result shouldBe "Goodbye\n"
        }

        "unmount volume succeeds" {
            dockerVolumeApi.unmountVolume(DockerVolumeMountRequest(name = "foo/vol"))
        }

        "unmount volume is idempotent" {
            dockerVolumeApi.unmountVolume(DockerVolumeMountRequest(name = "foo/vol"))
        }

        "checkout commit and old contents are present" {
            commitApi.checkoutCommit("foo", "id")
            dockerVolumeApi.mountVolume(DockerVolumeMountRequest(name = "foo/vol"))
            val result = dockerUtil.readFile("foo", "vol", "testfile")
            result shouldBe "Hello\n"
        }

        "delete volume succeeds" {
            dockerVolumeApi.unmountVolume(DockerVolumeMountRequest(name = "foo/vol"))
            dockerVolumeApi.removeVolume(DockerVolumeRequest(name = "foo/vol"))
        }

        "delete repository succeeds" {
            repoApi.deleteRepository("foo")
        }
    }
}
