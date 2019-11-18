/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.orchestrator

import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.matchers.maps.shouldContainKey
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.mockk.MockKAnnotations
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.impl.annotations.OverrideMockKs
import io.mockk.just
import io.mockk.verify
import io.titandata.ServiceLocator
import io.titandata.context.docker.DockerZfsContext
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Commit
import io.titandata.models.Repository
import io.titandata.models.Volume
import org.jetbrains.exposed.sql.transactions.transaction

class CommitOrchestratorTest : StringSpec() {

    @MockK
    lateinit var dockerZfsContext: DockerZfsContext

    @MockK
    lateinit var reaper: Reaper

    lateinit var vs: String

    @InjectMockKs
    @OverrideMockKs
    var services = ServiceLocator("test")

    override fun beforeSpec(spec: Spec) {
        services.metadata.init()
    }

    override fun beforeTest(testCase: TestCase) {
        services.metadata.clear()
        transaction {
            services.metadata.createRepository(Repository("foo"))
            vs = services.metadata.createVolumeSet("foo", null, true)
            services.metadata.createVolume(vs, Volume("vol1"))
            services.metadata.createVolume(vs, Volume("vol2"))
        }
        val ret = MockKAnnotations.init(this)
        every { dockerZfsContext.createCommit(any(), any(), any()) } just Runs
        return ret
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    init {
        "create commit with invalid name fails" {
            shouldThrow<IllegalArgumentException> {
                services.commits.createCommit("foo", Commit(id = "bad/commit"))
            }
        }

        "create commit with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                services.commits.createCommit("bad/repo", Commit(id = "id"))
            }
        }

        "create commit with non-existent repo fails" {
            shouldThrow<NoSuchObjectException> {
                services.commits.createCommit("bar", Commit(id = "id"))
            }
        }

        "create duplicate commit fails" {
            services.commits.createCommit("foo", Commit(id = "id"))
            shouldThrow<ObjectExistsException> {
                services.commits.createCommit("foo", Commit(id = "id"))
            }
        }

        "create commit uses current active volume set" {
            services.commits.createCommit("foo", Commit(id = "id", properties = mapOf("a" to "b")))
            val (volumeSet, commit) = transaction {
                services.metadata.getCommit("foo", "id")
            }
            volumeSet shouldBe vs
            commit.id shouldBe "id"
            verify {
                dockerZfsContext.createCommit(vs, "id", listOf("vol1", "vol2"))
            }
        }

        "create commit on explicit volume set succeeds" {
            val vs2 = transaction {
                services.metadata.createVolumeSet("foo")
            }
            services.commits.createCommit("foo", Commit(id = "id", properties = mapOf("a" to "b")), vs2)
            verify {
                dockerZfsContext.createCommit(vs2, "id", listOf())
            }
        }

        "create commit adds timestamp if not set" {
            services.commits.createCommit("foo", Commit(id = "id", properties = mapOf("a" to "b")))
            val commit = transaction {
                services.metadata.getCommit("foo", "id").second
            }
            commit.properties shouldContainKey "timestamp"
        }

        "create commit leaves timestamp intact if specified" {
            services.commits.createCommit("foo", Commit(id = "id", properties = mapOf("timestamp" to "2019-09-20T13:45:38Z")))
            val commit = transaction {
                services.metadata.getCommit("foo", "id").second
            }
            commit.properties["timestamp"] shouldBe "2019-09-20T13:45:38Z"
        }

        "get commit fails with invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                services.commits.getCommit("bad/repo", "id")
            }
        }

        "get commit fails with invalid commit id" {
            shouldThrow<IllegalArgumentException> {
                services.commits.getCommit("foo", "bad/id")
            }
        }

        "get commit fails with non-existent repo" {
            shouldThrow<NoSuchObjectException> {
                services.commits.getCommit("bar", "id")
            }
        }

        "get commit fails with non-existent commit" {
            shouldThrow<NoSuchObjectException> {
                services.commits.getCommit("foo", "nosuchid")
            }
        }

        "list commits succeeds" {
            services.commits.createCommit("foo", Commit(id = "one", properties = mapOf("timestamp" to "2019-09-20T13:45:36Z")))
            services.commits.createCommit("foo", Commit(id = "three", properties = mapOf("timestamp" to "2019-09-20T13:45:38Z")))
            services.commits.createCommit("foo", Commit(id = "two", properties = mapOf("timestamp" to "2019-09-20T13:45:37Z")))
            val commits = services.commits.listCommits("foo")
            commits.size shouldBe 3
            commits[0].id shouldBe "three"
            commits[1].id shouldBe "two"
            commits[2].id shouldBe "one"
        }

        "list commits filters via tags" {
            services.commits.createCommit("foo", Commit(id = "one", properties = mapOf("tags" to mapOf(
                    "a" to "b"
            ))))
            services.commits.createCommit("foo", Commit(id = "two", properties = mapOf("tags" to mapOf(
                    "a" to "b",
                    "c" to "d"
            ))))
            services.commits.createCommit("foo", Commit(id = "three", properties = mapOf("tags" to mapOf(
                    "a" to "c",
                    "c" to ""
            ))))
            val commits = services.commits.listCommits("foo", listOf("a=b", "c"))
            commits.size shouldBe 1
            commits[0].id shouldBe "two"
        }

        "list commits fails with invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                services.commits.listCommits("bad/repo")
            }
        }

        "list commits fails with non-existent repo" {
            shouldThrow<NoSuchObjectException> {
                services.commits.listCommits("bar")
            }
        }

        "delete commits succeeds" {
            every { reaper.signal() } just Runs
            services.commits.createCommit("foo", Commit(id = "id"))
            services.commits.deleteCommit("foo", "id")

            shouldThrow<NoSuchObjectException> {
                services.commits.getCommit("foo", "id")
            }

            verify {
                reaper.signal()
            }
        }

        "delete commit fails with invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                services.commits.deleteCommit("bad/repo", "id")
            }
        }

        "delete commit fails with invalid commit id" {
            shouldThrow<IllegalArgumentException> {
                services.commits.deleteCommit("foo", "bad/id")
            }
        }

        "delete commit fails with non-existent repo" {
            shouldThrow<NoSuchObjectException> {
                services.commits.deleteCommit("bar", "id")
            }
        }

        "delete commit fails with non-existent commit" {
            shouldThrow<NoSuchObjectException> {
                services.commits.getCommit("foo", "nosuchid")
            }
        }

        "checkout commit succeeds" {
            every { dockerZfsContext.cloneVolumeSet(any(), any(), any()) } just Runs
            every { dockerZfsContext.cloneVolume(any(), any(), any(), any()) } returns emptyMap()
            every { dockerZfsContext.createVolume(any(), any()) } returns emptyMap()
            every { reaper.signal() } just Runs
            services.commits.createCommit("foo", Commit("id"))
            services.commits.checkoutCommit("foo", "id")

            val vs2 = transaction {
                services.metadata.getActiveVolumeSet("foo")
            }
            vs2 shouldNotBe vs

            val volumes = transaction {
                services.metadata.listVolumes(vs2)
            }
            volumes.size shouldBe 2

            verify {
                reaper.signal()
                dockerZfsContext.cloneVolumeSet(vs, "id", vs2)
                dockerZfsContext.cloneVolume(vs, "id", vs2, "vol1")
                dockerZfsContext.cloneVolume(vs, "id", vs2, "vol2")
            }
        }

        "checkout commit fails with invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                services.commits.checkoutCommit("bad/repo", "id")
            }
        }

        "checkout commit fails with invalid commit id" {
            shouldThrow<IllegalArgumentException> {
                services.commits.checkoutCommit("foo", "bad/id")
            }
        }

        "checkout commit fails with non-existent repo" {
            shouldThrow<NoSuchObjectException> {
                services.commits.checkoutCommit("bar", "id")
            }
        }

        "checkout commit fails with non-existent commit" {
            shouldThrow<NoSuchObjectException> {
                services.commits.checkoutCommit("foo", "nosuchid")
            }
        }

        "update commit succeeds" {
            val orig = services.commits.createCommit("foo", Commit(id = "id", properties = mapOf("a" to "b")))
            services.commits.updateCommit("foo", Commit(id = "id", properties = mapOf("timestamp" to orig.properties["timestamp"] as String,
                    "a" to "c")))
            val commit = services.commits.getCommit("foo", "id")
            commit.properties["a"] shouldBe "c"
        }

        "update commit fails with invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                services.commits.updateCommit("bad/repo", Commit(id = "id"))
            }
        }

        "update commit fails with invalid commit id" {
            shouldThrow<IllegalArgumentException> {
                services.commits.updateCommit("foo", Commit(id = "bad/id"))
            }
        }

        "update commit fails with non-existent repo" {
            shouldThrow<NoSuchObjectException> {
                services.commits.updateCommit("bar", Commit(id = "id"))
            }
        }

        "update commit fails with non-existent commit" {
            shouldThrow<NoSuchObjectException> {
                services.commits.updateCommit("foo", Commit(id = "nosuchid"))
            }
        }
    }
}
