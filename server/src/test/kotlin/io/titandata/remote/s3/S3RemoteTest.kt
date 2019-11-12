/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.s3

import com.google.gson.GsonBuilder
import io.kotlintest.extensions.system.OverrideMode
import io.kotlintest.extensions.system.withEnvironment
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.titandata.models.Remote
import io.titandata.serialization.RemoteUtil

class S3RemoteTest : StringSpec() {

    val gson = GsonBuilder().create()
    val remoteUtil = RemoteUtil()

    fun parse(uri: String, map: Map<String, String>? = null): Remote {
        return remoteUtil.parseUri(uri, "name", map ?: mapOf())
    }

    init {
        "parsing full S3 URI succeeds" {
            val result = parse("s3://bucket/object/path")
            result.provider shouldBe "s3"
            result.name shouldBe "name"
            result.properties["bucket"] shouldBe "bucket"
            result.properties["path"] shouldBe "object/path"
            result.properties["accessKey"] shouldBe null
            result.properties["secretKey"] shouldBe null
        }

        "parsing S3 without path succeeds" {
            val result = parse("s3://bucket")
            result.provider shouldBe "s3"
            result.name shouldBe "name"
            result.properties["bucket"] shouldBe "bucket"
            result.properties["path"] shouldBe null
            result.properties["accessKey"] shouldBe null
            result.properties["secretKey"] shouldBe null
        }

        "specifying an invalid property fails" {
            shouldThrow<IllegalArgumentException> {
                parse("s3://bucket/path", mapOf("foo" to "bar"))
            }
        }

        "plain s3 provider fails" {
            shouldThrow<IllegalArgumentException> {
                parse("s3")
            }
        }

        "specifying query parameter fails" {
            shouldThrow<IllegalArgumentException> {
                parse("s3://bucket/path?query")
            }
        }

        "specifying fragment fails" {
            shouldThrow<IllegalArgumentException> {
                parse("s3://bucket/path#fragment")
            }
        }

        "specifying user fails" {
            shouldThrow<IllegalArgumentException> {
                parse("s3://user@bucket/path")
            }
        }

        "specifying port fails" {
            shouldThrow<IllegalArgumentException> {
                parse("s3://bucket:port/path")
            }
        }

        "missing bucket in s3 URI fails" {
            shouldThrow<IllegalArgumentException> {
                parse("s3:///path")
            }
        }

        "specifying key properties succeeds" {
            val result = parse("s3://bucket/object/path", mapOf("accessKey" to "ACCESS", "secretKey" to "SECRET"))
            result.properties["bucket"] shouldBe "bucket"
            result.properties["path"] shouldBe "object/path"
            result.properties["accessKey"] shouldBe "ACCESS"
            result.properties["secretKey"] shouldBe "SECRET"
        }

        "specifying access key only fails" {
            shouldThrow<IllegalArgumentException> {
                parse("s3://bucket/object/path", mapOf("accessKey" to "ACCESS"))
            }
        }

        "specifying secret key only fails" {
            shouldThrow<IllegalArgumentException> {
                parse("s3://bucket/object/path", mapOf("secretKey" to "SECRET"))
            }
        }

        "s3 remote to URI succeeds" {
            val (uri, props) = remoteUtil.toUri(Remote("s3", "name", mapOf("bucket" to "bucket", "path" to "path")))
            uri shouldBe "s3://bucket/path"
            props.size shouldBe 0
        }

        "s3 remote with keys to URI succeeds" {
            val (uri, props) = remoteUtil.toUri(Remote("s3", "name", mapOf("bucket" to "bucket", "path" to "path",
                    "accessKey" to "ACCESS", "secretKey" to "SECRET")))
            uri shouldBe "s3://bucket/path"
            props.size shouldBe 2
            props["accessKey"] shouldBe "ACCESS"
            props["secretKey"] shouldBe "*****"
        }

        "s3 remote with region to URI succeeds" {
            val (uri, props) = remoteUtil.toUri(Remote("s3", "name", mapOf("bucket" to "bucket", "path" to "path",
                    "region" to "REGION")))
            uri shouldBe "s3://bucket/path"
            props.size shouldBe 1
            props["region"] shouldBe "REGION"
        }

        "s3 get parameters succeeds" {
            val params = remoteUtil.getParameters(Remote("s3", "name", mapOf("bucket" to "bucket", "path" to "path",
                    "accessKey" to "ACCESS", "secretKey" to "SECRET", "region" to "REGION")))
            params.provider shouldBe "s3"
            params.properties["accessKey"] shouldBe "ACCESS"
            params.properties["secretKey"] shouldBe "SECRET"
            params.properties["region"] shouldBe "REGION"
        }

        "getting credentials from environment succeeds" {
            withEnvironment(mapOf("AWS_ACCESS_KEY_ID" to "accessKey", "AWS_SECRET_ACCESS_KEY" to "secretKey",
                    "AWS_REGION" to "us-west-2", "AWS_SESSION_TOKEN" to "sessionToken"), OverrideMode.SetOrOverride) {
                System.getenv("AWS_ACCESS_KEY_ID") shouldBe "accessKey"
                System.getenv("AWS_SECRET_ACCESS_KEY") shouldBe "secretKey"
                System.getenv("AWS_REGION") shouldBe "us-west-2"
                System.getenv("AWS_SESSION_TOKEN") shouldBe "sessionToken"
                val params = remoteUtil.getParameters(Remote("s3", "name", mapOf("bucket" to "bucket", "path" to "path")))
                params.properties["accessKey"] shouldBe "accessKey"
                params.properties["secretKey"] shouldBe "secretKey"
                params.properties["sessionToken"] shouldBe "sessionToken"
                params.properties["region"] shouldBe "us-west-2"
            }
        }
    }
}
