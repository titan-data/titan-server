/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.s3

import com.amazonaws.AmazonServiceException
import com.amazonaws.regions.DefaultAwsRegionProviderChain
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.DeleteObjectsRequest
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import io.kotlintest.SkipTestException
import io.kotlintest.Spec
import io.kotlintest.TestCaseOrder
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.titandata.EndToEndTest
import io.titandata.ProviderModule
import io.titandata.client.infrastructure.ClientException
import io.titandata.client.infrastructure.ServerException
import io.titandata.models.Commit
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Repository
import io.titandata.models.Volume
import java.io.ByteArrayInputStream
import java.util.UUID
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider

class S3WorkflowTest : EndToEndTest() {

    private val guid = UUID.randomUUID().toString()

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

    override fun beforeSpec(spec: Spec) {
        dockerUtil.stopServer()
        dockerUtil.startServer()
        dockerUtil.waitForServer()
        clearBucket()
    }

    override fun afterSpec(spec: Spec) {
        dockerUtil.stopServer(ignoreExceptions = false)
        clearBucket()
    }

    override fun testCaseOrder() = TestCaseOrder.Sequential

    private fun getLocation(): Pair<String, String> {
        val location = System.getProperty("s3.location")
                ?: throw SkipTestException("'s3.location' must be specified with -P")
        val bucket = location.substringBefore("/")
        val path = when {
            location.contains("/") -> location.substringAfter("/")
            else -> ""
        }
        return Pair(bucket, "$path/$guid")
    }

    private fun getRemote(): Remote {
        val (bucket, path) = getLocation()
        val creds = DefaultCredentialsProvider.create().resolveCredentials()
                ?: throw SkipTestException("Unable to determine AWS credentials")
        val region = DefaultAwsRegionProviderChain().region

        return Remote("s3", "origin", mapOf("bucket" to bucket, "path" to path, "accessKey" to creds.accessKeyId(),
                "secretKey" to creds.secretAccessKey(), "region" to region))
    }

    init {

        "creating and deleting S3 object succeeds" {
            val remote = getRemote()
            val provider = S3RemoteProvider(ProviderModule("test"))

            val s3 = provider.getClient(remote, params)
            try {
                val (bucket, key) = provider.getPath(remote, "id")
                val metadata = ObjectMetadata()
                metadata.userMetadata = mapOf("test" to "test")
                val input = ByteArrayInputStream("Hello, world!".toByteArray())
                val request = PutObjectRequest(bucket, key, input, metadata)
                s3.putObject(request)
                s3.deleteObject(bucket, key)
            } catch (e: AmazonServiceException) {
                throw SkipTestException("S3 operation failed: ${e.message}")
            }
        }

        "create new repository succeeds" {
            repoApi.createRepository(Repository("foo"))
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

        "add s3 remote succeeds" {
            val remote = getRemote()
            remoteApi.createRemote("foo", remote)
        }

        "list remote commits returns an empty list" {
            val result = remoteApi.listRemoteCommits("foo", "origin", params)
            result.size shouldBe 0
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

        "push of same commit fails" {
            val exception = shouldThrow<ClientException> {
                operationApi.push("foo", "origin", "id", params)
            }
            exception.code shouldBe "ObjectExistsException"
        }

        "push same commit metadata succeeds" {
            val op = operationApi.push("foo", "origin", "id", params, true)
            waitForOperation(op.id)
        }

        "create second commit succeeds" {
            commitApi.createCommit("foo", Commit(id = "id2", properties = mapOf()))
        }

        "push second commit succeeds" {
            val op = operationApi.push("foo", "origin", "id2", params)
            waitForOperation(op.id)
        }

        "list remote commits records two commits" {
            val commits = remoteApi.listRemoteCommits("foo", "origin", params)
            commits.size shouldBe 2
            commits[0].id shouldBe "id2"
            commits[1].id shouldBe "id"
        }

        "list remote commits filters commits" {
            val commits = remoteApi.listRemoteCommits("foo", "origin", params, listOf("a"))
            commits.size shouldBe 1
            commits[0].id shouldBe "id"
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

        "list remote commits returns updated commits" {
            val commits = remoteApi.listRemoteCommits("foo", "origin", params)
            commits.size shouldBe 2
            commits[0].id shouldBe "id2"
            commits[1].id shouldBe "id"
            getTag(commits[1], "a") shouldBe "B"
        }

        "delete local commits succeeds" {
            commitApi.deleteCommit("foo", "id")
            commitApi.deleteCommit("foo", "id2")
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

        "add remote without keys succeeds" {
            val defaultRemote = getRemote()
            val remote = Remote("s3", "origin", mapOf("bucket" to defaultRemote.properties["bucket"]!!,
                    "path" to defaultRemote.properties["path"]!!))
            remoteApi.createRemote("foo", remote)
        }

        "list commits with keys succeeds" {
            val remote = getRemote()
            val commits = remoteApi.listRemoteCommits("foo", "origin", RemoteParameters("s3",
                    mapOf("accessKey" to remote.properties["accessKey"], "secretKey" to remote.properties["secretKey"],
                            "region" to remote.properties["region"])))
            commits.size shouldBe 2
            commits[0].id shouldBe "id2"
            commits[1].id shouldBe "id"
        }

        "list commits without keys fails" {
            val exception = shouldThrow<ClientException> {
                remoteApi.listRemoteCommits("foo", "origin", params)
            }
            exception.code shouldBe "IllegalArgumentException"
        }

        "list commits with incorrect key fails" {
            val remote = getRemote()
            val exception = shouldThrow<ServerException> {
                remoteApi.listRemoteCommits("foo", "origin", RemoteParameters("s3", mapOf(
                        "accessKey" to "foo", "secretKey" to "bar", "region" to remote.properties["region"])))
            }
            exception.code shouldBe "AmazonS3Exception"
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
