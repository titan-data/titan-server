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

    val params = RemoteParameters("s3")

    fun clearBucket() {
        val remote = getRemote()
        try {
            val s3 = AmazonS3ClientBuilder.standard().build()

            val request = ListObjectsRequest()
                    .withBucketName(remote.properties["bucket"] as String)
                    .withPrefix(remote.properties["path"] as String)
            var objects = s3.listObjects(request)
            while (true) {
                val keys = objects.objectSummaries.map { it.key }
                if (keys.size != 0) {
                    s3.deleteObjects(DeleteObjectsRequest(remote.properties["bucket"] as String).withKeys(*keys.toTypedArray()))
                }
                if (objects.isTruncated()) {
                    objects = s3.listNextBatchOfObjects(objects)
                } else {
                    break
                }
            }
        } catch (e: Throwable) {
            // Ignore
        }
    }

    private fun getLocation(): Pair<String, String> {
        val location = System.getProperty("s3.location")
                ?: throw SkipTestException("'s3.location' must be specified with -P")
        val bucket = location.substringBefore("/")
        val path = when {
            location.contains("/") -> location.substringAfter("/")
            else -> ""
        }
        return Pair(bucket, "$path/$uuid")
    }

    private fun getRemote(): Remote {
        val (bucket, path) = getLocation()
        val creds = DefaultCredentialsProvider.create().resolveCredentials()
                ?: throw SkipTestException("Unable to determine AWS credentials")
        val region = DefaultAwsRegionProviderChain().region

        return Remote("s3", "origin", mapOf("bucket" to bucket, "path" to path, "accessKey" to creds.accessKeyId(),
                "secretKey" to creds.secretAccessKey(), "region" to region))
    }

    override fun beforeSpec(spec: Spec) {
        dockerUtil.stopServer()
        val kubernetesConfig = System.getProperty("kubernetes.config")
        if (kubernetesConfig != null) {
            dockerUtil.startServer(kubernetesConfig)
        } else {
            dockerUtil.startServer()
        }
        dockerUtil.waitForServer()
        clearBucket()
    }

    override fun afterSpec(spec: Spec) {
        dockerUtil.stopServer(ignoreExceptions = false)
    }

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
