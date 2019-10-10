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
import io.titandata.models.Repository
import io.titandata.models.VolumeCreateRequest
import io.titandata.models.VolumeMountRequest
import io.titandata.models.VolumeRequest
import io.titandata.util.GuidGenerator
import java.io.ByteArrayInputStream
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider

class S3WorkflowTest : EndToEndTest() {

    private val guid = GuidGenerator().get()

    fun clearBucket() {
        val remote = getRemote()
        try {
            val s3 = AmazonS3ClientBuilder.standard().build()

            val request = ListObjectsRequest()
                    .withBucketName(remote.bucket)
                    .withPrefix(remote.path)
            var objects = s3.listObjects(request)
            while (true) {
                val keys = objects.objectSummaries.map { it.key }
                if (keys.size != 0) {
                    s3.deleteObjects(DeleteObjectsRequest(remote.bucket).withKeys(*keys.toTypedArray()))
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

    private fun getRemote(): S3Remote {
        val (bucket, path) = getLocation()
        val creds = DefaultCredentialsProvider.create().resolveCredentials()
                ?: throw SkipTestException("Unable to determine AWS credentials")
        val region = DefaultAwsRegionProviderChain().region

        return S3Remote(name = "origin", bucket = bucket, path = path, accessKey = creds.accessKeyId(),
                secretKey = creds.secretAccessKey(), region = region)
    }

    init {

        "creating and deleting S3 object succeeds" {
            val remote = getRemote()
            val provider = S3RemoteProvider(ProviderModule("test"))

            val s3 = provider.getClient(remote, S3Parameters())
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

        "add s3 remote succeeds" {
            val remote = getRemote()
            remoteApi.createRemote("foo", remote)
        }

        "list remote commits returns an empty list" {
            val result = remoteApi.listRemoteCommits("foo", "origin", S3Parameters())
            result.size shouldBe 0
        }

        "push commit succeeds" {
            val op = operationApi.push("foo", "origin", "id", S3Parameters())
            waitForOperation(op.id)
        }

        "list remote commits returns pushed commit" {
            val commits = remoteApi.listRemoteCommits("foo", "origin", S3Parameters())
            commits.size shouldBe 1
            commits[0].id shouldBe "id"
            commits[0].properties["a"] shouldBe "b"
        }

        "push of same commit fails" {
            val exception = shouldThrow<ClientException> {
                operationApi.push("foo", "origin", "id", S3Parameters())
            }
            exception.code shouldBe "ObjectExistsException"
        }

        "create second commit succeeds" {
            commitApi.createCommit("foo", Commit(id = "id2", properties = mapOf()))
        }

        "push second commit succeeds" {
            val op = operationApi.push("foo", "origin", "id2", S3Parameters())
            waitForOperation(op.id)
        }

        "list remote commits records two commits" {
            val commits = remoteApi.listRemoteCommits("foo", "origin", S3Parameters())
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
            dockerUtil.writeFile("foo/vol", "testfile", "Goodbye")
            val result = dockerUtil.readFile("foo/vol", "testfile")
            result shouldBe "Goodbye\n"
        }

        "pull original commit succeeds" {
            val op = operationApi.pull("foo", "origin", "id", S3Parameters())
            waitForOperation(op.id)
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

        "add remote without keys succeeds" {
            val defaultRemote = getRemote()
            val remote = S3Remote(name = "origin", bucket = defaultRemote.bucket, path = defaultRemote.path)
            remoteApi.createRemote("foo", remote)
        }

        "list commits with keys succeeds" {
            val remote = getRemote()
            val commits = remoteApi.listRemoteCommits("foo", "origin", S3Parameters(accessKey = remote.accessKey,
                    secretKey = remote.secretKey, region = remote.region))
            commits.size shouldBe 2
            commits[0].id shouldBe "id2"
            commits[1].id shouldBe "id"
        }

        "list commits without keys fails" {
            val exception = shouldThrow<ClientException> {
                remoteApi.listRemoteCommits("foo", "origin", S3Parameters())
            }
            exception.code shouldBe "IllegalArgumentException"
        }

        "list commits with incorrect key fails" {
            val remote = getRemote()
            val exception = shouldThrow<ServerException> {
                remoteApi.listRemoteCommits("foo", "origin", S3Parameters(accessKey = "foo", secretKey = "bar",
                        region = remote.region))
            }
            exception.code shouldBe "AmazonS3Exception"
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
