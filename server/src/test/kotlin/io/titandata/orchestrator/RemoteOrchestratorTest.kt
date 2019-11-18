/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.orchestrator

import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.matchers.types.shouldBeInstanceOf
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.mockk.MockKAnnotations
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.OverrideMockKs
import io.mockk.impl.annotations.SpyK
import io.mockk.slot
import io.titandata.ServiceLocator
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Repository
import io.titandata.remote.nop.server.NopRemoteServer
import io.titandata.remote.ssh.server.SshRemoteServer
import org.jetbrains.exposed.sql.transactions.transaction

class RemoteOrchestratorTest : StringSpec() {

    val params = RemoteParameters("nop")

    @SpyK
    var nopProvider = NopRemoteServer()

    @SpyK
    var sshProvider = SshRemoteServer()

    @InjectMockKs
    @OverrideMockKs
    var services = ServiceLocator("test")

    override fun beforeSpec(spec: Spec) {
        services.metadata.init()
    }

    override fun beforeTest(testCase: TestCase) {
        services.metadata.clear()
        transaction {
            services.metadata.createRepository(Repository(name = "foo"))
        }
        MockKAnnotations.init(this)
        services.setRemoteProvider("nop", nopProvider)
        services.setRemoteProvider("ssh", sshProvider)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    init {
        "add remote succeeds" {
            services.remotes.addRemote("foo", Remote("nop", "origin"))
        }

        "add remote fails with invalid properties" {
            shouldThrow<IllegalArgumentException> {
                services.remotes.addRemote("foo", Remote("nop", "origin", mapOf("foo" to "bar")))
            }
        }

        "add remote with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                services.remotes.addRemote("bad/repo", Remote("nop", "origin"))
            }
        }
        "add remote with invalid name fails" {
            shouldThrow<IllegalArgumentException> {
                services.remotes.addRemote("foo", Remote("nop", "bad/remote"))
            }
        }

        "add remote to non-existent repo fails" {
            shouldThrow<NoSuchObjectException> {
                services.remotes.addRemote("bar", Remote("nop", "origin"))
            }
        }

        "get remote succeeds" {
            services.remotes.addRemote("foo", Remote("s3", "origin", mapOf("bucket" to "bucket")))
            val remote = services.remotes.getRemote("foo", "origin")
            remote.provider shouldBe "s3"
            remote.name shouldBe "origin"
            remote.properties["bucket"] shouldBe "bucket"
        }

        "get remote with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                services.remotes.getRemote("bad/repo", "origin")
            }
        }

        "get remote with invalid name fails" {
            shouldThrow<IllegalArgumentException> {
                services.remotes.getRemote("foo", "bad/remote")
            }
        }

        "get remote from non-existent repo fails" {
            shouldThrow<NoSuchObjectException> {
                services.remotes.getRemote("foo", "origin")
            }
        }

        "list remotes succeeds" {
            services.remotes.addRemote("foo", Remote("nop", "one"))
            services.remotes.addRemote("foo", Remote("nop", "two"))
            val remotes = services.remotes.listRemotes("foo").sortedBy { it.name }
            remotes.size shouldBe 2
            remotes[0].name shouldBe "one"
            remotes[1].name shouldBe "two"
        }

        "list remotes with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                services.remotes.listRemotes("bad/repo")
            }
        }

        "list remotes from non-existent repo fails" {
            shouldThrow<NoSuchObjectException> {
                services.remotes.listRemotes("bar")
            }
        }

        "remove remote succeeds" {
            services.remotes.addRemote("foo", Remote("nop", "origin"))
            services.remotes.removeRemote("foo", "origin")
            shouldThrow<NoSuchObjectException> {
                services.remotes.getRemote("foo", "origin")
            }
        }

        "remove remote with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                services.remotes.removeRemote("bad/repo", "origin")
            }
        }

        "remove remote with invalid name fails" {
            shouldThrow<IllegalArgumentException> {
                services.remotes.removeRemote("foo", "bad/remote")
            }
        }

        "remove remote from non-existent repo fails" {
            shouldThrow<NoSuchObjectException> {
                services.remotes.removeRemote("bar", "origin")
            }
        }

        "remove non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                services.remotes.removeRemote("foo", "origin")
            }
        }

        "update remote succeeds" {
            services.remotes.addRemote("foo", Remote("nop", "origin"))
            services.remotes.updateRemote("foo", "origin", Remote("s3", "origin2", mapOf("bucket" to "bucket")))
            services.remotes.listRemotes("foo").size shouldBe 1
            val remote = services.remotes.getRemote("foo", "origin2")
            remote.provider shouldBe "s3"
            remote.name shouldBe "origin2"
            remote.properties["bucket"] shouldBe "bucket"
        }

        "update remote with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                services.remotes.updateRemote("bad/repo", "origin", Remote("nop", "origin"))
            }
        }

        "update remote with invalid repo rpoerties fails" {
            services.remotes.addRemote("foo", Remote("nop", "origin"))
            shouldThrow<IllegalArgumentException> {
                services.remotes.updateRemote("foo", "origin", Remote("nop", "origin", mapOf("foo" to "bar")))
            }
        }

        "update remote with invalid name fails" {
            shouldThrow<IllegalArgumentException> {
                services.remotes.updateRemote("foo", "bad/repo", Remote("nop", "origin"))
            }
        }

        "update remote with invalid new name fails" {
            shouldThrow<IllegalArgumentException> {
                services.remotes.updateRemote("foo", "origin", Remote("nop", "bad/repo"))
            }
        }

        "update remote from non-existent repo fails" {
            shouldThrow<NoSuchObjectException> {
                services.remotes.updateRemote("bar", "origin", Remote("nop", "origin"))
            }
        }

        "update non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                services.remotes.updateRemote("foo", "origin", Remote("nop", "origin"))
            }
        }

        "update remote to existing name fails" {
            services.remotes.addRemote("foo", Remote("nop", "one"))
            services.remotes.addRemote("foo", Remote("nop", "two"))
            shouldThrow<ObjectExistsException> {
                services.remotes.updateRemote("foo", "one", Remote("nop", "two"))
            }
        }

        "list remote commits succeeds" {
            every { nopProvider.listCommits(any(), any(), any()) } returns
                    listOf("one" to emptyMap(), "two" to emptyMap())
            services.remotes.addRemote("foo", Remote("nop", "origin"))
            val result = services.remotes.listRemoteCommits("foo", "origin", params, null)
            result.size shouldBe 2
            result[0].id shouldBe "one"
            result[1].id shouldBe "two"
        }

        "list remote commits fails with invalid parameters" {
            every { nopProvider.listCommits(any(), any(), any()) } returns
                    listOf("one" to emptyMap(), "two" to emptyMap())
            services.remotes.addRemote("foo", Remote("nop", "origin"))
            shouldThrow<IllegalArgumentException> {
                services.remotes.listRemoteCommits("foo", "origin", RemoteParameters("nop", mapOf("foo" to "bar")), null)
            }
        }

        "list remote commits with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                services.remotes.listRemoteCommits("bad/repo", "origin", params, null)
            }
        }

        "list remote commits with invalid remote name fails" {
            shouldThrow<IllegalArgumentException> {
                services.remotes.listRemoteCommits("foo", "bad/remote", params, null)
            }
        }

        "list remote commits for non-existent repo fails" {
            shouldThrow<NoSuchObjectException> {
                services.remotes.listRemoteCommits("bar", "origin", params, null)
            }
        }

        "list remote commits for non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                services.remotes.listRemoteCommits("foo", "origin", params, null)
            }
        }

        "mismatched remote parameter types fail on list" {
            services.remotes.addRemote("foo", Remote("ssh", "origin", mapOf("address" to "host", "username" to "user",
                    "path" to "/path")))
            shouldThrow<IllegalArgumentException> {
                services.remotes.listRemoteCommits("foo", "origin", params, null)
            }
        }

        "list remote commits converts remote port" {
            services.remotes.addRemote("foo", Remote("ssh", "origin", mapOf("address" to "host", "username" to "user",
                    "path" to "/path", "port" to 8022)))
            val slot = slot<Map<String, Any>>()
            every { sshProvider.listCommits(capture(slot), any(), any()) } returns emptyList()
            val result = services.remotes.listRemoteCommits("foo", "origin", RemoteParameters("ssh", mapOf("password" to "pass")), null)
            result.size shouldBe 0
            slot.captured["port"] shouldBe 8022
            slot.captured["port"].shouldBeInstanceOf<Int>()
        }

        "get remote commit succeeds" {
            every { nopProvider.getCommit(any(), any(), any()) } returns emptyMap()
            services.remotes.addRemote("foo", Remote("nop", "origin"))
            val result = services.remotes.getRemoteCommit("foo", "origin", params, "id")
            result.id shouldBe "id"
        }

        "mismatched remote parameter types fail on get" {
            services.remotes.addRemote("foo", Remote("ssh", "origin", mapOf("address" to "host", "username" to "user",
                    "path" to "/path")))
            shouldThrow<IllegalArgumentException> {
                services.remotes.getRemoteCommit("foo", "origin", params, "id")
            }
        }

        "get remote commit converts remote port" {
            services.remotes.addRemote("foo", Remote("ssh", "origin", mapOf("address" to "host", "username" to "user",
                    "path" to "/path", "port" to 8022)))
            val slot = slot<Map<String, Any>>()
            every { sshProvider.getCommit(capture(slot), any(), any()) } returns emptyMap()
            val result = services.remotes.getRemoteCommit("foo", "origin", RemoteParameters("ssh", mapOf("password" to "pass")), "id")
            result.id shouldBe "id"
            slot.captured["port"] shouldBe 8022
            slot.captured["port"].shouldBeInstanceOf<Int>()
        }

        "get remote commit fails with invalid parameters" {
            every { nopProvider.getCommit(any(), any(), any()) } returns emptyMap()
            services.remotes.addRemote("foo", Remote("nop", "origin"))
            shouldThrow<IllegalArgumentException> {
                services.remotes.getRemoteCommit("foo", "origin", RemoteParameters("nop", mapOf("a" to "b")), "id")
            }
        }

        "get remote commit with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                services.remotes.getRemoteCommit("bad/repo", "origin", params, "id")
            }
        }

        "get remote commit with invalid remote name fails" {
            shouldThrow<IllegalArgumentException> {
                services.remotes.getRemoteCommit("foo", "bad/remote", params, "id")
            }
        }

        "get remote commit for non-existent repo fails" {
            shouldThrow<NoSuchObjectException> {
                services.remotes.getRemoteCommit("bar", "origin", params, "id")
            }
        }

        "get remote commit for non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                services.remotes.getRemoteCommit("foo", "origin", params, "id")
            }
        }
    }
}
