/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.engine

import com.google.gson.GsonBuilder
import io.kotlintest.TestCase
import io.kotlintest.TestResult
import io.kotlintest.matchers.types.shouldBeInstanceOf
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.mockk.MockKAnnotations
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.impl.annotations.OverrideMockKs
import io.titandata.models.Remote
import io.titandata.serialization.RemoteUtil
import java.io.Console

class EngineRemoteTest : StringSpec() {

    val gson = GsonBuilder().create()
    val remoteUtil = RemoteUtil()

    fun parse(uri: String, map: Map<String, String>? = null): Remote {
        return remoteUtil.parseUri(uri, "name", map ?: mapOf())
    }

    @MockK
    lateinit var console: Console

    @InjectMockKs
    @OverrideMockKs
    var engineUtil = EngineRemoteUtil()

    override fun beforeTest(testCase: TestCase) {
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    init {
        "parsing full engine URI succeeds" {
            val result = parse("engine://user:pass@host/path")
            result.shouldBeInstanceOf<EngineRemote>()
            val remote = result as EngineRemote
            remote.name shouldBe "name"
            remote.username shouldBe "user"
            remote.password shouldBe "pass"
            remote.address shouldBe "host"
            remote.repository shouldBe "path"
        }

        "parsing engine URI without password succeeds" {
            val result = parse("engine://user@host/path")
            result.shouldBeInstanceOf<EngineRemote>()
            val remote = result as EngineRemote
            remote.name shouldBe "name"
            remote.username shouldBe "user"
            remote.password shouldBe null
            remote.address shouldBe "host"
            remote.repository shouldBe "path"
        }

        "plain engine provider fails" {
            shouldThrow<IllegalArgumentException> {
                parse("engine")
            }
        }

        "specifying engine port fails" {
            shouldThrow<IllegalArgumentException> {
                parse("engine://user:pass@host:123/path")
            }
        }

        "specifying engine query parameter fails" {
            shouldThrow<IllegalArgumentException> {
                parse("engine://user@host/path?query")
            }
        }

        "specifying engine fragment fails" {
            shouldThrow<IllegalArgumentException> {
                parse("engine://user@host/path#fragment")
            }
        }

        "missing username in engine URI fails" {
            shouldThrow<IllegalArgumentException> {
                parse("engine://host/path")
            }
        }

        "missing path in engine URI fails" {
            shouldThrow<IllegalArgumentException> {
                parse("engine://user@host")
            }
        }

        "empty path in engine URI fails" {
            shouldThrow<IllegalArgumentException> {
                parse("engine://user@host/")
            }
        }

        "missing host in engine URI fails" {
            shouldThrow<IllegalArgumentException> {
                parse("engine://user@/path")
            }
        }

        "serializing an engine remote succeeds" {
            val result = gson.toJson(EngineRemote(name = "foo",
                    address = "a", username = "u", password = "p", repository = "bar"))
            result.shouldBe("{\"provider\":\"engine\",\"name\":\"foo\",\"address\":\"a\"," +
                    "\"username\":\"u\",\"password\":\"p\",\"repository\":\"bar\"}")
        }

        "deserializing an engine remote succeeds" {
            val result = gson.fromJson("{\"provider\":\"engine\",\"name\":\"foo\",\"address\":\"a\"," +
                    "\"username\":\"u\",\"password\":\"p\"}", EngineRemote::class.java)
            result.shouldBeInstanceOf<EngineRemote>()
            result.provider shouldBe "engine"
            result.name shouldBe "foo"
            result.username shouldBe "u"
            result.password shouldBe "p"
        }

        "engine remote to URI succeeds" {
            val (result, props) = remoteUtil.toUri(EngineRemote(name = "name", address = "host", username = "user",
                    repository = "foo"))
            result shouldBe "engine://user@host/foo"
            props.size shouldBe 0
        }

        "engine remote with password to URI succeeds" {
            val (result, props) = remoteUtil.toUri(EngineRemote(name = "name", address = "host", username = "user",
                    repository = "foo", password = "pass"))
            result shouldBe "engine://user:pass@host/foo"
            props.size shouldBe 0
        }

        "get engine parameters succeeds" {
            val result = remoteUtil.getParameters(EngineRemote(name = "name", address = "host", username = "user",
                    repository = "foo", password = "pass"))
            result.properties["password"] shouldBe null
        }

        "get engine parameters prompts for password" {
            every { console.readPassword(any()) } returns "pass".toCharArray()
            val result = engineUtil.getParameters(EngineRemote(name = "name", address = "host", username = "user",
                    repository = "foo"))
            result.properties["password"] shouldBe "pass"
        }
    }
}
