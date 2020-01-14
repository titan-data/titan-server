/*
* Copyright The Titan Project Contributors.
 */

package io.titandata.kubernetes

import com.amazonaws.regions.DefaultAwsRegionProviderChain
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.DeleteObjectsRequest
import com.amazonaws.services.s3.model.ListObjectsRequest
import io.kotlintest.SkipTestException
import io.kotlintest.Spec
import io.kotlintest.shouldBe
import io.titandata.models.Commit
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Repository
import io.titandata.models.Volume
import java.util.UUID
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider

class KubernetesWorkflowTest : KubernetesTest() {

    private val uuid = UUID.randomUUID().toString()

    init {
        "get context returns correct configuration" {
            val context = contextApi.getContext()
            context.provider shouldBe "kubernetes-csi"
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

        "add s3 remote succeeds" {
            val remote = getRemote()
            remoteApi.createRemote("foo", remote)
        }

        "push commit succeeds" {
            val op = operationApi.push("foo", "origin", "id", params)
            waitForOperation(op.id)
        }

        "delete commit" {
            commitApi.deleteCommit("foo", "id")
        }

        "pull original commit succeeds" {
            val op = operationApi.pull("foo", "origin", "id", params)
            waitForOperation(op.id)
        }

        "delete volume" {
            volumeApi.deleteVolume("foo", "vol")
        }

        "delete repository" {
            repoApi.deleteRepository("foo")
        }
    }
}
