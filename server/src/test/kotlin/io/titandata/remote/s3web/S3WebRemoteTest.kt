/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.s3web

import com.google.gson.GsonBuilder
import io.kotlintest.matchers.types.shouldBeInstanceOf
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.titandata.models.Remote
import io.titandata.serialization.RemoteUtil

class S3WebRemoteTest : StringSpec() {

    val gson = GsonBuilder().create()
    val remoteUtil = RemoteUtil()

    fun parse(uri: String, map: Map<String, String>? = null): Remote {
        return remoteUtil.parseUri(uri, "name", map ?: mapOf())
    }

    init {
        "parsing full S3 web URI succeeds" {
            val result = parse("s3web://host/object/path")
            result.provider shouldBe "s3web"
            result.name shouldBe "name"
            result.properties["url"] shouldBe "http://host/object/path"
        }

        "parsing S3 web without path succeeds" {
            val result = parse("s3web://host")
            result.provider shouldBe "s3web"
            result.name shouldBe "name"
            result.properties["url"] shouldBe "http://host"
        }

        "specifying an invalid property fails" {
            shouldThrow<IllegalArgumentException> {
                parse("s3web://host/path", mapOf("foo" to "bar"))
            }
        }

        "plain s3web provider fails" {
            shouldThrow<IllegalArgumentException> {
                parse("s3web")
            }
        }

        "specifying query parameter fails" {
            shouldThrow<IllegalArgumentException> {
                parse("s3web://host/path?query")
            }
        }

        "specifying fragment fails" {
            shouldThrow<IllegalArgumentException> {
                parse("s3web://host/path#fragment")
            }
        }

        "specifying user fails" {
            shouldThrow<IllegalArgumentException> {
                parse("s3web://user@host/path")
            }
        }

        "specifying port succeeds" {
            val result = parse("s3web://host:1023/object/path")
            result.provider shouldBe "s3web"
            result.name shouldBe "name"
            result.properties["url"] shouldBe "http://host:1023/object/path"
        }

        "missing host in s3 web URI fails" {
            shouldThrow<IllegalArgumentException> {
                parse("s3:///path")
            }
        }

        "s3 web remote to URI succeeds" {
            val (uri, props) = remoteUtil.toUri(Remote("s3web", "name", mapOf("url" to "http://host/path")))
            uri shouldBe "s3web://host/path"
            props.size shouldBe 0
        }
    }
}
