/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.nop

import com.google.gson.GsonBuilder
import com.google.gson.JsonParseException
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.titandata.models.Remote
import io.titandata.serialization.RemoteUtil

class NopRemoteTest : StringSpec() {

    val gson = GsonBuilder().create()
    val remoteUtil = RemoteUtil()

    fun parse(uri: String, map: Map<String, String>? = null): Remote {
        return remoteUtil.parseUri(uri, "name", map ?: emptyMap())
    }

    init {
        "parsing a nop URI succeeds" {
            val result = parse("nop")
            result.provider shouldBe "nop"
            result.name shouldBe "name"
        }

        "parsing URI with missing authority fails" {
            shouldThrow<IllegalArgumentException> {
                parse("nop://")
            }
        }

        "parsing nop URI authority fails" {
            shouldThrow<IllegalArgumentException> {
                parse("nop://foo")
            }
        }

        "serializing a nop remote succeeds" {
            val result = gson.toJson(Remote("nop", "foo"))
            result.shouldBe("{\"provider\":\"nop\",\"name\":\"foo\"}")
        }

        "deserializing a nop remote succeeds" {
            val result = gson.fromJson("{\"provider\":\"nop\",\"name\":\"foo\"}", Remote::class.java)
            result.provider shouldBe "nop"
            result.name shouldBe "foo"
        }

        "deserializing an unknown provider type fails" {
            shouldThrow<JsonParseException> {
                gson.fromJson("{\"provider\":\"blah\",\"name\":\"foo\"}",
                        Remote::class.java)
            }
        }

        "converting to nop remote succeeds" {
            val (result, properties) = remoteUtil.toUri(Remote("nop", "name"))
            result shouldBe "nop"
            properties.size shouldBe 0
        }

        "getting nop parameters succeeds" {
            val result = remoteUtil.getParameters(Remote("nop", "name"))
            result.provider shouldBe "nop"
        }
    }
}
