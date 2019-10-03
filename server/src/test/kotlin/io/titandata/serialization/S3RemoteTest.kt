/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.serialization

import com.google.gson.GsonBuilder
import io.kotlintest.extensions.system.withEnvironment
import io.kotlintest.matchers.types.shouldBeInstanceOf
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.S3Parameters
import io.titandata.models.S3Remote

class S3RemoteTest : StringSpec() {

    val gson = ModelTypeAdapters.configure(GsonBuilder()).create()
    val remoteUtil = RemoteUtil()

    fun parse(uri: String, map: Map<String, String>? = null): Remote {
        return remoteUtil.parseUri(uri, "name", map ?: mapOf())
    }

    init {
        "parsing full S3 URI succeeds" {
            val result = parse("s3://bucket/object/path")
            result.shouldBeInstanceOf<S3Remote>()
            val remote = result as S3Remote
            remote.name shouldBe "name"
            remote.bucket shouldBe "bucket"
            remote.path shouldBe "object/path"
            remote.accessKey shouldBe null
            remote.secretKey shouldBe null
        }

        "parsing S3 without path succeeds" {
            val result = parse("s3://bucket")
            result.shouldBeInstanceOf<S3Remote>()
            val remote = result as S3Remote
            remote.name shouldBe "name"
            remote.bucket shouldBe "bucket"
            remote.path shouldBe null
            remote.accessKey shouldBe null
            remote.secretKey shouldBe null
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
            val remote = result as S3Remote
            remote.bucket shouldBe "bucket"
            remote.path shouldBe "object/path"
            remote.accessKey shouldBe "ACCESS"
            remote.secretKey shouldBe "SECRET"
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

        "serializing a s3 remote succeeds" {
            val result = gson.toJson(S3Remote(name = "foo",
                    bucket = "bucket", path = "object"))
            result.shouldBe("{\"provider\":\"s3\",\"name\":\"foo\",\"bucket\":\"bucket\"," +
                    "\"path\":\"object\"}")
        }

        "serializing a s3 remote with keys succeeds" {
            val result = gson.toJson(S3Remote(name = "foo",
                    bucket = "bucket", path = "object", accessKey = "ACCESS", secretKey = "SECRET"))
            result.shouldBe("{\"provider\":\"s3\",\"name\":\"foo\",\"bucket\":\"bucket\"," +
                    "\"path\":\"object\",\"accessKey\":\"ACCESS\",\"secretKey\":\"SECRET\"}")
        }

        "serializing a s3 remote with region succeeds" {
            val result = gson.toJson(S3Remote(name = "foo",
                    bucket = "bucket", path = "object", region = "region"))
            result.shouldBe("{\"provider\":\"s3\",\"name\":\"foo\",\"bucket\":\"bucket\"," +
                    "\"path\":\"object\",\"region\":\"region\"}")
        }

        "deserializing a s3 remote succeeds" {
            val result = gson.fromJson("{\"provider\":\"s3\",\"name\":\"foo\",\"bucket\":\"bucket\"," +
                    "\"path\":\"object\",\"accessKey\":\"ACCESS\",\"secretKey\":\"SECRET\",\"region\":\"REGION\"}",
                    Remote::class.java)
            result.shouldBeInstanceOf<S3Remote>()
            val remote = result as S3Remote
            remote.provider shouldBe "s3"
            remote.bucket shouldBe "bucket"
            remote.path shouldBe "object"
            remote.accessKey shouldBe "ACCESS"
            remote.secretKey shouldBe "SECRET"
            remote.region shouldBe "REGION"
        }

        "serializing a s3 request succeeds" {
            val result = gson.toJson(S3Parameters(accessKey = "ACCESS", secretKey = "SECRET", region = "REGION"))
            result.shouldBe("{\"provider\":\"s3\",\"accessKey\":\"ACCESS\",\"secretKey\":\"SECRET\",\"region\":\"REGION\"}")
        }

        "deserializing a s3 request succeeds" {
            val result = gson.fromJson("{\"provider\":\"s3\",\"accessKey\":\"ACCESS\",\"secretKey\":\"SECRET\",\"region\":\"REGION\"}",
                    RemoteParameters::class.java)
            result.shouldBeInstanceOf<S3Parameters>()
            result as S3Parameters
            result.provider shouldBe "s3"
            result.accessKey shouldBe "ACCESS"
            result.secretKey shouldBe "SECRET"
            result.region shouldBe "REGION"
        }

        "s3 remote to URI succeeds" {
            val (uri, props) = remoteUtil.toUri(S3Remote(name = "name", bucket = "bucket", path = "path"))
            uri shouldBe "s3://bucket/path"
            props.size shouldBe 0
        }

        "s3 remote with keys to URI succeeds" {
            val (uri, props) = remoteUtil.toUri(S3Remote(name = "name", bucket = "bucket", path = "path",
                    accessKey = "ACCESS", secretKey = "SECRET"))
            uri shouldBe "s3://bucket/path"
            props.size shouldBe 2
            props["accessKey"] shouldBe "ACCESS"
            props["secretKey"] shouldBe "*****"
        }

        "s3 remote with region to URI succeeds" {
            val (uri, props) = remoteUtil.toUri(S3Remote(name = "name", bucket = "bucket", path = "path",
                    region = "REGION"))
            uri shouldBe "s3://bucket/path"
            props.size shouldBe 1
            props["region"] shouldBe "REGION"
        }

        "s3 get parameters succeeds" {
            val params = remoteUtil.getParameters(S3Remote(name = "name", bucket = "bucket", path = "path",
                    accessKey = "ACCESS", secretKey = "SECRET", region = "REGION"))
            params.shouldBeInstanceOf<S3Parameters>()
            params as S3Parameters
            params.provider shouldBe "s3"
            params.accessKey shouldBe "ACCESS"
            params.secretKey shouldBe "SECRET"
            params.region shouldBe "REGION"
        }

        "getting credentials from environment succeeds" {
            withEnvironment(mapOf("AWS_ACCESS_KEY_ID" to "accessKey", "AWS_SECRET_ACCESS_KEY" to "secretKey",
                    "AWS_REGION" to "us-west-2", "AWS_SESSION_TOKEN" to "sessionToken")) {
                System.getenv("AWS_ACCESS_KEY_ID") shouldBe "accessKey"
                System.getenv("AWS_SECRET_ACCESS_KEY") shouldBe "secretKey"
                System.getenv("AWS_REGION") shouldBe "us-west-2"
                System.getenv("AWS_SESSION_TOKEN") shouldBe "sessionToken"
                val params = remoteUtil.getParameters(S3Remote(name = "name", bucket = "bucket", path = "path"))
                params.shouldBeInstanceOf<S3Parameters>()
                params as S3Parameters
                params.accessKey shouldBe "accessKey"
                params.secretKey shouldBe "secretKey"
                params.sessionToken shouldBe "sessionToken"
                params.region shouldBe "us-west-2"
            }
        }
    }
}
