/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.s3web

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
import io.titandata.models.Commit
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Repository
import io.titandata.models.Volume
import io.titandata.remote.s3.S3RemoteProvider
import java.io.ByteArrayInputStream
import java.util.UUID
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider

class S3WebWorkflowTest : EndToEndTest() {

    private val guid = UUID.randomUUID().toString()

    val s3params = RemoteParameters("s3")
    val s3webParams = RemoteParameters("s3web")

    fun clearBucket() {
        val remote = getS3Remote()
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

    private fun getS3Remote(): Remote {
        val (bucket, path) = getLocation()
        val creds = DefaultCredentialsProvider.create().resolveCredentials()
                ?: throw SkipTestException("Unable to determine AWS credentials")
        val region = DefaultAwsRegionProviderChain().region

        return Remote("s3", "origin", mapOf("bucket" to bucket, "path" to path, "accessKey" to creds.accessKeyId(),
                "secretKey" to creds.secretAccessKey(), "region" to region))
    }

    private fun getS3WebRemote(): Remote {
        val (bucket, path) = getLocation()

        return Remote("s3web", "web", mapOf("url" to "http://$bucket.s3.amazonaws.com/$path"))
    }

    init {

        "creating and accessing S3 object succeeds" {
            val remote = getS3Remote()
            val provider = S3RemoteProvider(ProviderModule("test"))

            val s3 = provider.getClient(remote, s3params)
            try {
                val (bucket, key) = provider.getPath(remote, "id")
                val metadata = ObjectMetadata()
                metadata.userMetadata = mapOf("test" to "test")
                val input = ByteArrayInputStream("Hello, world!".toByteArray())
                val request = PutObjectRequest(bucket, key, input, metadata)
                s3.putObject(request)

                val webRemote = getS3WebRemote()
                val webProvider = S3WebRemoteProvider(ProviderModule("test"))
                val body = webProvider.getFile(webRemote, "id").body!!.string()
                body shouldBe "Hello, world!"

                s3.deleteObject(bucket, key)
            } catch (e: AmazonServiceException) {
                throw SkipTestException("S3 operation failed: ${e.message}")
            }
        }

        "create new repository succeeds" {
            val repo = repoApi.createRepository(Repository("foo"))
            repo.name shouldBe "foo"
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
            val remote = getS3Remote()
            remoteApi.createRemote("foo", remote)
        }

        "add s3 web remote succeeds" {
            val remote = getS3WebRemote()
            remoteApi.createRemote("foo", remote)
        }

        "list remote commits returns an empty list" {
            val result = remoteApi.listRemoteCommits("foo", "web", s3webParams)
            result.size shouldBe 0
        }

        "push commit succeeds" {
            val op = operationApi.push("foo", "origin", "id", s3params)
            waitForOperation(op.id)
        }

        "list remote commits returns pushed commit" {
            val commits = remoteApi.listRemoteCommits("foo", "web", s3webParams)
            commits.size shouldBe 1
            commits[0].id shouldBe "id"
            getTag(commits[0], "a") shouldBe "b"
        }

        "list remote commits filters out commit" {
            val commits = remoteApi.listRemoteCommits("foo", "web", s3webParams, listOf("e"))
            commits.size shouldBe 0
        }

        "list remote commits filters include commit" {
            val commits = remoteApi.listRemoteCommits("foo", "web", s3webParams, listOf("a=b", "c=d"))
            commits.size shouldBe 1
            commits[0].id shouldBe "id"
        }

        "push of same commit fails" {
            val exception = shouldThrow<ClientException> {
                operationApi.push("foo", "origin", "id", s3params)
            }
            exception.code shouldBe "ObjectExistsException"
        }

        "create second commit succeeds" {
            commitApi.createCommit("foo", Commit(id = "id2", properties = mapOf()))
        }

        "push to web fails" {
            val exception = shouldThrow<ClientException> {
                operationApi.push("foo", "web", "id2", s3params)
            }
            exception.code shouldBe "IllegalArgumentException"
        }

        "push second commit succeeds" {
            val op = operationApi.push("foo", "origin", "id2", s3params)
            waitForOperation(op.id)
        }

        "list remote commits records two commits" {
            val commits = remoteApi.listRemoteCommits("foo", "web", s3webParams)
            commits.size shouldBe 2
            commits[0].id shouldBe "id2"
            commits[1].id shouldBe "id"
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
            val op = operationApi.pull("foo", "web", "id", s3webParams)
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
            remoteApi.deleteRemote("foo", "web")
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
