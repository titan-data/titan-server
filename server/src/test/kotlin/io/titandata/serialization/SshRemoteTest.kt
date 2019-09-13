/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package io.titandata.serialization

import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.SshParameters
import io.titandata.models.SshRemote
import io.titandata.serialization.remote.SshRemoteUtil
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
import java.io.Console
import org.junit.rules.TemporaryFolder

class SshRemoteTest : StringSpec() {

    val gson = ModelTypeAdapters.configure(GsonBuilder()).create()
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
            result.shouldBeInstanceOf<SshRemote>()
            val remote = result as SshRemote
            remote.name shouldBe "name"
            remote.username shouldBe "user"
            remote.password shouldBe "pass"
            remote.address shouldBe "host"
            remote.port shouldBe 8022
            remote.path shouldBe "/path"
            remote.keyFile shouldBe null
        }

        "parsing simple SSH URI succeeds" {
            val result = parse("ssh://user@host/path")
            result.shouldBeInstanceOf<SshRemote>()
            val remote = result as SshRemote
            remote.name shouldBe "name"
            remote.username shouldBe "user"
            remote.password shouldBe null
            remote.address shouldBe "host"
            remote.port shouldBe null
            remote.path shouldBe "/path"
            remote.keyFile shouldBe null
        }

        "specifying key file in properties succeeds" {
            val result = parse("ssh://user@host/path", mapOf("keyFile" to "~/.ssh/id_dsa"))
            result.shouldBeInstanceOf<SshRemote>()
            val remote = result as SshRemote
            remote.keyFile shouldBe "~/.ssh/id_dsa"
        }

        "parsing relative path succeeds" {
            val result = parse("ssh://user@host/~/relative/path")
            result.shouldBeInstanceOf<SshRemote>()
            val remote = result as SshRemote
            remote.path shouldBe "relative/path"
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

        "serializing a ssh remote succeeds" {
            val result = gson.toJson(SshRemote(name = "foo",
                    address = "a", username = "u", password = "p", path = "/p"))
            result.shouldBe("{\"provider\":\"ssh\",\"name\":\"foo\",\"address\":\"a\"," +
                    "\"username\":\"u\",\"password\":\"p\",\"path\":\"/p\"}")
        }

        "serializing a ssh remote with key file succeeds" {
            val result = gson.toJson(SshRemote(name = "foo",
                    address = "a", username = "u", keyFile = "p", path = "/p"))
            result.shouldBe("{\"provider\":\"ssh\",\"name\":\"foo\",\"address\":\"a\"," +
                    "\"username\":\"u\",\"keyFile\":\"p\",\"path\":\"/p\"}")
        }

        "deserializing a ssh remote succeeds" {
            val result = gson.fromJson("{\"provider\":\"ssh\",\"name\":\"foo\",\"address\":\"a\"," +
                    "\"username\":\"u\",\"password\":\"p\",\"path\":\"/p\"}", Remote::class.java)
            result.shouldBeInstanceOf<SshRemote>()
            val remote = result as SshRemote
            remote.provider shouldBe "ssh"
            remote.name shouldBe "foo"
            remote.username shouldBe "u"
            remote.password shouldBe "p"
            remote.path shouldBe "/p"
        }

        "serializing a ssh request succeeds" {
            val result = gson.toJson(SshParameters(password = "p"))
            result.shouldBe("{\"provider\":\"ssh\",\"password\":\"p\"}")
        }

        "deserializing a ssh request succeeds" {
            val result = gson.fromJson("{\"provider\":\"ssh\",\"password\":\"p\"}",
                    RemoteParameters::class.java)
            result.shouldBeInstanceOf<SshParameters>()
            val request = result as SshParameters
            request.provider shouldBe "ssh"
            request.password shouldBe "p"
        }

        "basic SSH remote to URI succeeds" {
            val (uri, parameters) = remoteUtil.toUri(SshRemote(name = "name", username = "username", address = "host",
                    path = "/path"))
            uri shouldBe "ssh://username@host/path"
            parameters.size shouldBe 0
        }

        "SSH remote with password to URI succeeds" {
            val (uri, parameters) = remoteUtil.toUri(SshRemote(name = "name", username = "username", address = "host",
                    path = "/path", password = "pass"))
            uri shouldBe "ssh://username:*****@host/path"
            parameters.size shouldBe 0
        }

        "SSH remote with port to URI succeeds" {
            val (uri, parameters) = remoteUtil.toUri(SshRemote(name = "name", username = "username", address = "host",
                    path = "/path", port = 812))
            uri shouldBe "ssh://username@host:812/path"
            parameters.size shouldBe 0
        }

        "SSH remote with relative path to URI succeeds" {
            val (uri, parameters) = remoteUtil.toUri(SshRemote(name = "name", username = "username", address = "host",
                    path = "path"))
            uri shouldBe "ssh://username@host/~/path"
            parameters.size shouldBe 0
        }

        "SSH remote with keyfile to URI succeeds" {
            val (uri, parameters) = remoteUtil.toUri(SshRemote(name = "name", username = "username", address = "host",
                    path = "/path", keyFile = "keyfile"))
            uri shouldBe "ssh://username@host/path"
            parameters.size shouldBe 1
            parameters["keyFile"] shouldBe "keyfile"
        }

        "get basic SSH get parameters succeeds" {
            val params = remoteUtil.getParameters(SshRemote(name = "name", username = "username", address = "host",
                    path = "/path", password = "pass"))
            params.shouldBeInstanceOf<SshParameters>()
            params as SshParameters
            params.provider shouldBe "ssh"
            params.password shouldBe null
            params.key shouldBe null
        }

        "get SSH parameters with keyfile succeeds" {
            val temporaryFolder = TemporaryFolder()
            temporaryFolder.create()
            try {
                val keyFile = temporaryFolder.newFile()
                keyFile.writeText("KEY")
                val params = remoteUtil.getParameters(SshRemote(name = "name", username = "username", address = "host",
                        path = "/path", keyFile = keyFile.absolutePath))
                params as SshParameters
                params.password shouldBe null
                params.key shouldBe "KEY"
            } finally {
                temporaryFolder.delete()
            }
        }

        "prompt for SSH password succeeds" {
            every { console.readPassword(any()) } returns "pass".toCharArray()
            val params = sshUtil.getParameters(SshRemote(name = "name", username = "username", address = "host",
                    path = "/path"))
            params.shouldBeInstanceOf<SshParameters>()
            params as SshParameters
            params.password shouldBe "pass"
            params.key shouldBe null
        }
    }
}
