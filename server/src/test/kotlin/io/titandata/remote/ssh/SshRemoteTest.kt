/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.ssh

import com.google.gson.GsonBuilder
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
import org.junit.rules.TemporaryFolder

class SshRemoteTest : StringSpec() {

    val gson = GsonBuilder().create()
    val remoteUtil = RemoteUtil()

    fun parse(uri: String, map: Map<String, String>? = null): Remote {
        return remoteUtil.parseUri(uri, "name", map ?: mapOf())
    }

    @MockK
    lateinit var console: Console

    @InjectMockKs
    @OverrideMockKs
    var sshUtil = SshRemoteUtil()

    override fun beforeTest(testCase: TestCase) {
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    init {
        "parsing full SSH URI succeeds" {
            val result = parse("ssh://user:pass@host:8022/path")
            result.provider shouldBe "ssh"
            result.name shouldBe "name"
            result.properties["username"] shouldBe "user"
            result.properties["password"] shouldBe "pass"
            result.properties["address"] shouldBe "host"
            result.properties["port"] shouldBe 8022
            result.properties["path"] shouldBe "/path"
            result.properties["keyFile"] shouldBe null
        }

        "parsing simple SSH URI succeeds" {
            val result = parse("ssh://user@host/path")
            result.provider shouldBe "ssh"
            result.name shouldBe "name"
            result.properties["username"] shouldBe "user"
            result.properties["password"] shouldBe null
            result.properties["address"] shouldBe "host"
            result.properties["port"] shouldBe null
            result.properties["path"] shouldBe "/path"
            result.properties["keyFile"] shouldBe null
        }

        "specifying key file in properties succeeds" {
            val result = parse("ssh://user@host/path", mapOf("keyFile" to "~/.ssh/id_dsa"))
            result.properties["keyFile"] shouldBe "~/.ssh/id_dsa"
        }

        "parsing relative path succeeds" {
            val result = parse("ssh://user@host/~/relative/path")
            result.properties["path"] shouldBe "relative/path"
        }

        "specifying password and key file fails" {
            shouldThrow<IllegalArgumentException> {
                parse("ssh://user:password@host/path", mapOf("keyFile" to "~/.ssh/id_dsa"))
            }
        }

        "specifying an invalid property fails" {
            shouldThrow<IllegalArgumentException> {
                parse("ssh://user@host/path", mapOf("foo" to "bar"))
            }
        }

        "plain ssh provider fails" {
            shouldThrow<IllegalArgumentException> {
                parse("ssh")
            }
        }

        "specifying query parameter fails" {
            shouldThrow<IllegalArgumentException> {
                parse("ssh://user@host/path?query")
            }
        }

        "specifying fragment fails" {
            shouldThrow<IllegalArgumentException> {
                parse("ssh://user@host/path#fragment")
            }
        }

        "missing username in ssh URI fails" {
            shouldThrow<IllegalArgumentException> {
                parse("ssh://host/path")
            }
        }

        "missing path in ssh URI fails" {
            shouldThrow<IllegalArgumentException> {
                parse("ssh://user@host")
            }
        }

        "missing host in ssh URI fails" {
            shouldThrow<IllegalArgumentException> {
                parse("ssh://user@/path")
            }
        }

        "basic SSH remote to URI succeeds" {
            val (uri, parameters) = remoteUtil.toUri(Remote("ssh", "name", mapOf("username" to "username", "address" to "host",
                    "path" to "/path")))
            uri shouldBe "ssh://username@host/path"
            parameters.size shouldBe 0
        }

        "SSH remote with password to URI succeeds" {
            val (uri, parameters) = remoteUtil.toUri(Remote("ssh", "name", mapOf("username" to "username", "address" to "host",
                    "path" to "/path", "password" to "pass")))
            uri shouldBe "ssh://username:*****@host/path"
            parameters.size shouldBe 0
        }

        "SSH remote with port to URI succeeds" {
            val (uri, parameters) = remoteUtil.toUri(Remote("ssh", "name", mapOf("username" to "username", "address" to "host",
                    "path" to "/path", "port" to 812)))
            uri shouldBe "ssh://username@host:812/path"
            parameters.size shouldBe 0
        }

        "SSH remote with relative path to URI succeeds" {
            val (uri, parameters) = remoteUtil.toUri(Remote("ssh", "name", mapOf("username" to "username", "address" to "host",
                    "path" to "path")))
            uri shouldBe "ssh://username@host/~/path"
            parameters.size shouldBe 0
        }

        "SSH remote with keyfile to URI succeeds" {
            val (uri, parameters) = remoteUtil.toUri(Remote("ssh", "name", mapOf("username" to "username", "address" to "host",
                    "path" to "/path", "keyFile" to "keyfile")))
            uri shouldBe "ssh://username@host/path"
            parameters.size shouldBe 1
            parameters["keyFile"] shouldBe "keyfile"
        }

        "get basic SSH get parameters succeeds" {
            val params = remoteUtil.getParameters(Remote("ssh", "name", mapOf("username" to "username", "address" to "host",
                    "path" to "/path", "password" to "pass")))
            params.provider shouldBe "ssh"
            params.properties["password"] shouldBe null
            params.properties["key"] shouldBe null
        }

        "get SSH parameters with keyfile succeeds" {
            val temporaryFolder = TemporaryFolder()
            temporaryFolder.create()
            try {
                val keyFile = temporaryFolder.newFile()
                keyFile.writeText("KEY")
                val params = remoteUtil.getParameters(Remote("ssh", "name", mapOf("username" to "username", "address" to "host",
                        "path" to "/path", "keyFile" to keyFile.absolutePath)))
                params.properties["password"] shouldBe null
                params.properties["key"] shouldBe "KEY"
            } finally {
                temporaryFolder.delete()
            }
        }

        "prompt for SSH password succeeds" {
            every { console.readPassword(any()) } returns "pass".toCharArray()
            val params = sshUtil.getParameters(mapOf("username" to "username", "address" to "host",
                    "path" to "/path"))
            params["password"] shouldBe "pass"
            params["key"] shouldBe null
        }
    }
}
