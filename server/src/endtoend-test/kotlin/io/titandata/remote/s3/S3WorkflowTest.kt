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
import io.titandata.client.infrastructure.ClientException
import io.titandata.client.infrastructure.ServerException
import io.titandata.models.Commit
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Repository
import io.titandata.models.Volume
import io.titandata.remote.s3.server.S3RemoteServer
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


        "add remote without keys succeeds" {
            val defaultRemote = getRemote()
            val remote = Remote("s3", "origin", mapOf("bucket" to defaultRemote.properties["bucket"]!!,
                    "path" to defaultRemote.properties["path"]!!))
            remoteApi.createRemote("foo", remote)
        }

        "list commits with keys succeeds" {
            val remote = getRemote()
            val commits = remoteApi.listRemoteCommits("foo", "origin", RemoteParameters("s3",
                    mapOf("accessKey" to remote.properties["accessKey"]!!, "secretKey" to remote.properties["secretKey"]!!,
                            "region" to remote.properties["region"]!!)))
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
                        "accessKey" to "foo", "secretKey" to "bar", "region" to remote.properties["region"]!!)))
            }
            exception.code shouldBe "AmazonS3Exception"
        }

    }
}
