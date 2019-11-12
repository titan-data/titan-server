/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.engine

import io.kotlintest.TestCase
import io.kotlintest.TestResult
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
            result.provider shouldBe "engine"
            result.name shouldBe "name"
            result.properties["username"] shouldBe "user"
            result.properties["password"] shouldBe "pass"
            result.properties["address"] shouldBe "host"
            result.properties["repository"] shouldBe "path"
        }

        "parsing engine URI without password succeeds" {
            val result = parse("engine://user@host/path")
            result.provider shouldBe "engine"
            result.name shouldBe "name"
            result.properties["username"] shouldBe "user"
            result.properties["password"] shouldBe null
            result.properties["address"] shouldBe "host"
            result.properties["repository"] shouldBe "path"
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

        "engine remote to URI succeeds" {
            val (result, props) = remoteUtil.toUri(Remote("engine", "name", mapOf("address" to "host", "username" to "user",
                    "repository" to "foo")))
            result shouldBe "engine://user@host/foo"
            props.size shouldBe 0
        }

        "engine remote with password to URI succeeds" {
            val (result, props) = remoteUtil.toUri(Remote("engine", "name", mapOf("address" to "host", "username" to "user",
                    "repository" to "foo", "password" to "pass")))
            result shouldBe "engine://user:pass@host/foo"
            props.size shouldBe 0
        }

        "get engine parameters succeeds" {
            val result = remoteUtil.getParameters(Remote("engine", "name", mapOf("address" to "host", "username" to "user",
                    "repository" to "foo", "password" to "pass")))
            result.properties["password"] shouldBe null
        }

        "get engine parameters prompts for password" {
            every { console.readPassword(any()) } returns "pass".toCharArray()
            val result = engineUtil.getParameters(Remote("engine", "name", mapOf("address" to "host", "username" to "user",
                    "repository" to "foo")))
            result.properties["password"] shouldBe "pass"
        }
    }
}
