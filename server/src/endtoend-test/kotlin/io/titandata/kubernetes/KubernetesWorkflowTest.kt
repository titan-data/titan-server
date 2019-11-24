/*
* Copyright The Titan Project Contributors.
 */

package io.titandata.kubernetes

import io.kotlintest.Spec
import io.kotlintest.shouldBe
import io.titandata.models.Commit
import io.titandata.models.Repository
import io.titandata.models.Volume
import java.util.UUID

class KubernetesWorkflowTest : KubernetesTest() {

    private val uuid = UUID.randomUUID().toString()

    override fun beforeSpec(spec: Spec) {
        dockerUtil.stopServer()
        dockerUtil.startServer()
        dockerUtil.waitForServer()
    }

    override fun afterSpec(spec: Spec) {
        dockerUtil.stopServer(ignoreExceptions = false)
    }

    init {
        "get context returns correct configuration" {
            val context = contextApi.getContext()
            context.provider shouldBe "kubernetes-csi"
            context.properties.size shouldBe 0
        }

        "kubectl works" {
            dockerUtil.execServer("kubectl", "cluster-info")
        }

        "create repository" {
            repoApi.createRepository(Repository("foo"))
        }

        "create volume" {
            volumeApi.createVolume("foo", Volume("vol")).config
        }

        "launch pod" {
            val volumeConfig = volumeApi.getVolume("foo", "vol").config
            launchPod("$uuid-test", volumeConfig["pvc"] as String)
            waitForPod("$uuid-test")
        }

        "write data" {
            executor.exec("kubectl", "exec", "$uuid-test", "--", "sh", "-c", "echo one > /data/out; sync; sleep 1;")
        }

        "get volume status" {
            waitForVolumeApi("foo", "vol")
            val status = volumeApi.getVolumeStatus("foo", "vol")
            status.ready shouldBe true
            status.error shouldBe null
        }

        "create commit" {
            commitApi.createCommit("foo", Commit("id"))
        }

        "get commit status" {
            waitForCommitApi("foo", "id")
            val status = commitApi.getCommitStatus("foo", "id")
            status.ready shouldBe true
            status.error shouldBe null
        }

        "update data" {
            executor.exec("kubectl", "exec", "$uuid-test", "--", "sh", "-c", "echo two > /data/out; sync; sleep 1;")
        }

        "delete pod" {
            executor.exec("kubectl", "delete", "pod", "--grace-period=0", "--force", "$uuid-test")
        }

        "checkout commit" {
            commitApi.checkoutCommit("foo", "id")
        }

        "launch new pod" {
            val volumeConfig = volumeApi.getVolume("foo", "vol").config
            launchPod("$uuid-test2", volumeConfig["pvc"] as String)
            waitForPod("$uuid-test2")
        }

        "file contents are correct" {
            val output = executor.exec("kubectl", "exec", "$uuid-test2", "cat", "/data/out").trim()
            output shouldBe "one"
        }

        "delete cloned pod succeeds" {
            executor.exec("kubectl", "delete", "pod", "--grace-period=0", "--force", "$uuid-test2")
        }

        "delete commit" {
            commitApi.deleteCommit("foo", "id")
        }

        "delete volume" {
            volumeApi.deleteVolume("foo", "vol")
        }

        "delete repository" {
            repoApi.deleteRepository("foo")
        }
    }
}
