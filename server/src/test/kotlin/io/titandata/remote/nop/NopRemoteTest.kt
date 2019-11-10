/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.nop

import com.google.gson.GsonBuilder
import com.google.gson.JsonParseException
import io.kotlintest.matchers.types.shouldBeInstanceOf
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.serialization.ModelTypeAdapters
import io.titandata.serialization.RemoteUtil

class NopRemoteTest : StringSpec() {

    val gson = ModelTypeAdapters.configure(GsonBuilder()).create()
    val remoteUtil = RemoteUtil()

    fun parse(uri: String, map: Map<String, String>? = null): Remote {
        return remoteUtil.parseUri(uri, "name", map ?: emptyMap())
    }

    init {
        "parsing a nop URI succeeds" {
            val result = parse("nop")
            result.shouldBeInstanceOf<NopRemote>()
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
            val result = gson.toJson(NopRemote(name = "foo"))
            result.shouldBe("{\"provider\":\"nop\",\"name\":\"foo\"}")
        }

        "deserializing a nop remote succeeds" {
            val result = gson.fromJson("{\"provider\":\"nop\",\"name\":\"foo\"}", Remote::class.java)
            result.shouldBeInstanceOf<NopRemote>()
            result.provider shouldBe "nop"
            result.name shouldBe "foo"
        }

        "deserializing an unknown provider type fails" {
            shouldThrow<JsonParseException> {
                gson.fromJson("{\"provider\":\"blah\",\"name\":\"foo\"}",
                        Remote::class.java)
            }
        }

        "serializing a nop request succeeds" {
            val result = gson.toJson(NopParameters())
            result.shouldBe("{\"provider\":\"nop\",\"delay\":0}")
        }

        "deserializing a nop request succeeds" {
            val result = gson.fromJson("{\"provider\":\"nop\"}", RemoteParameters::class.java)
            result.shouldBeInstanceOf<NopParameters>()
            result.provider shouldBe "nop"
        }

        "converting to nop remote succeeds" {
            val (result, properties) = remoteUtil.toUri(NopRemote(name = "name"))
            result shouldBe "nop"
            properties.size shouldBe 0
        }

        "getting nop parameters succeeds" {
            val result = remoteUtil.getParameters(NopRemote(name = "name"))
            result.shouldBeInstanceOf<NopParameters>()
            result.provider shouldBe "nop"
        }
    }
}
