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
import io.mockk.impl.annotations.MockK
import io.mockk.impl.annotations.OverrideMockKs
import io.mockk.impl.annotations.SpyK
import io.titandata.ProviderModule
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Commit
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Repository
import io.titandata.remote.nop.NopRemoteProvider
import io.titandata.storage.zfs.ZfsStorageProvider
import org.jetbrains.exposed.sql.transactions.transaction

class RemoteOrchestratorTest : StringSpec() {

    val params = RemoteParameters("nop")

    @MockK
    lateinit var zfsStorageProvider: ZfsStorageProvider

    @SpyK
    var nopRemoteProvider = NopRemoteProvider()

    @InjectMockKs
    @OverrideMockKs
    var providers = ProviderModule("test")

    override fun beforeSpec(spec: Spec) {
        providers.metadata.init()
    }

    override fun beforeTest(testCase: TestCase) {
        providers.metadata.clear()
        transaction {
            providers.metadata.createRepository(Repository(name = "foo"))
        }
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    init {
        "add remote succeeds" {
            providers.remotes.addRemote("foo", Remote("nop", "origin"))
        }

        "add remote with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.remotes.addRemote("bad/repo", Remote("nop", "origin"))
            }
        }
        "add remote with invalid name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.remotes.addRemote("foo", Remote("nop", "bad/remote"))
            }
        }

        "add remote to non-existent repo fails" {
            shouldThrow<NoSuchObjectException> {
                providers.remotes.addRemote("bar", Remote("nop", "origin"))
            }
        }

        "get remote succeeds" {
            providers.remotes.addRemote("foo", Remote("s3", "origin", mapOf("bucket" to "bucket")))
            val remote = providers.remotes.getRemote("foo", "origin")
            remote.provider shouldBe "s3"
            remote.name shouldBe "origin"
            remote.properties["bucket"] shouldBe "bucket"
        }

        "get remote with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.remotes.getRemote("bad/repo", "origin")
            }
        }

        "get remote with invalid name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.remotes.getRemote("foo", "bad/remote")
            }
        }

        "get remote from non-existent repo fails" {
            shouldThrow<NoSuchObjectException> {
                providers.remotes.getRemote("foo", "origin")
            }
        }

        "list remotes succeeds" {
            providers.remotes.addRemote("foo", Remote("nop", "one"))
            providers.remotes.addRemote("foo", Remote("nop", "two"))
            val remotes = providers.remotes.listRemotes("foo").sortedBy { it.name }
            remotes.size shouldBe 2
            remotes[0].name shouldBe "one"
            remotes[1].name shouldBe "two"
        }

        "list remotes with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.remotes.listRemotes("bad/repo")
            }
        }

        "list remotes from non-existent repo fails" {
            shouldThrow<NoSuchObjectException> {
                providers.remotes.listRemotes("bar")
            }
        }

        "remove remote succeeds" {
            providers.remotes.addRemote("foo", Remote("nop", "origin"))
            providers.remotes.removeRemote("foo", "origin")
            shouldThrow<NoSuchObjectException> {
                providers.remotes.getRemote("foo", "origin")
            }
        }

        "remove remote with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.remotes.removeRemote("bad/repo", "origin")
            }
        }

        "remove remote with invalid name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.remotes.removeRemote("foo", "bad/remote")
            }
        }

        "remove remote from non-existent repo fails" {
            shouldThrow<NoSuchObjectException> {
                providers.remotes.removeRemote("bar", "origin")
            }
        }

        "remove non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                providers.remotes.removeRemote("foo", "origin")
            }
        }

        "update remote succeeds" {
            providers.remotes.addRemote("foo", Remote("nop", "origin"))
            providers.remotes.updateRemote("foo", "origin", Remote("s3", "origin2", mapOf("bucket" to "bucket")))
            providers.remotes.listRemotes("foo").size shouldBe 1
            val remote = providers.remotes.getRemote("foo", "origin2")
            remote.provider shouldBe "s3"
            remote.name shouldBe "origin2"
            remote.properties["bucket"] shouldBe "bucket"
        }

        "update remote with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.remotes.updateRemote("bad/repo", "origin", Remote("nop", "origin"))
            }
        }

        "update remote with invalid name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.remotes.updateRemote("foo", "bad/repo", Remote("nop", "origin"))
            }
        }

        "update remote with invalid new name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.remotes.updateRemote("foo", "origin", Remote("nop", "bad/repo"))
            }
        }

        "update remote from non-existent repo fails" {
            shouldThrow<NoSuchObjectException> {
                providers.remotes.updateRemote("bar", "origin", Remote("nop", "origin"))
            }
        }

        "update non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                providers.remotes.updateRemote("foo", "origin", Remote("nop", "origin"))
            }
        }

        "update remote to existing name fails" {
            providers.remotes.addRemote("foo", Remote("nop", "one"))
            providers.remotes.addRemote("foo", Remote("nop", "two"))
            shouldThrow<ObjectExistsException> {
                providers.remotes.updateRemote("foo", "one", Remote("nop", "two"))
            }
        }

        "list remote commits succeeds" {
            every { nopRemoteProvider.listCommits(any(), any(), any()) } returns
                    listOf(Commit(id = "one"),
                            Commit(id = "two"))
            providers.remotes.addRemote("foo", Remote("nop", "origin"))
            val result = providers.remotes.listRemoteCommits("foo", "origin", params, null)
            result.size shouldBe 2
            result[0].id shouldBe "one"
            result[1].id shouldBe "two"
        }

        "list remote commits with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.remotes.listRemoteCommits("bad/repo", "origin", params, null)
            }
        }

        "list remote commits with invalid remote name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.remotes.listRemoteCommits("foo", "bad/remote", params, null)
            }
        }

        "list remote commits for non-existent repo fails" {
            shouldThrow<NoSuchObjectException> {
                providers.remotes.listRemoteCommits("bar", "origin", params, null)
            }
        }

        "list remote commits for non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                providers.remotes.listRemoteCommits("foo", "origin", params, null)
            }
        }

        "get remote commit succeeds" {
            every { nopRemoteProvider.getCommit(any(), any(), any()) } returns
                    Commit(id = "one")
            providers.remotes.addRemote("foo", Remote("nop", "origin"))
            val result = providers.remotes.getRemoteCommit("foo", "origin", params, "id")
            result.id shouldBe "one"
        }

        "get remote commit with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.remotes.getRemoteCommit("bad/repo", "origin", params, "id")
            }
        }

        "get remote commit with invalid remote name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.remotes.getRemoteCommit("foo", "bad/remote", params, "id")
            }
        }

        "get remote commit for non-existent repo fails" {
            shouldThrow<NoSuchObjectException> {
                providers.remotes.getRemoteCommit("bar", "origin", params, "id")
            }
        }

        "get remote commit for non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                providers.remotes.getRemoteCommit("foo", "origin", params, "id")
            }
        }
    }
}
