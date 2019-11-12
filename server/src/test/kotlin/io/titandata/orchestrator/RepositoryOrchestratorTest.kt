/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.orchestrator

import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.shouldBe
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
import io.mockk.verifyAll
import io.titandata.ProviderModule
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Commit
import io.titandata.models.Repository
import io.titandata.models.RepositoryVolumeStatus
import io.titandata.models.Volume
import io.titandata.storage.zfs.ZfsStorageProvider
import org.jetbrains.exposed.sql.transactions.transaction

class RepositoryOrchestratorTest : StringSpec() {

    @MockK
    lateinit var zfsStorageProvider: ZfsStorageProvider

    @MockK
    lateinit var reaper: Reaper

    @InjectMockKs
    @OverrideMockKs
    var providers = ProviderModule("test")

    override fun beforeSpec(spec: Spec) {
        providers.metadata.init()
    }

    override fun beforeTest(testCase: TestCase) {
        providers.metadata.clear()
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    fun createRepository() {
        every { zfsStorageProvider.createVolumeSet(any()) } just Runs
        providers.repositories.createRepository(Repository(name = "foo", properties = mapOf("a" to "b")))
    }

    init {
        "create repository succeeds" {
            createRepository()
            val vs = transaction {
                providers.metadata.getActiveVolumeSet("foo")
            }
            verifyAll {
                zfsStorageProvider.createVolumeSet(vs)
            }
        }

        "create repository with invalid name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.repositories.createRepository(Repository(name = "a".repeat(65)))
            }
        }

        "get repository succeeds" {
            createRepository()
            val repo = providers.repositories.getRepository("foo")
            repo.name shouldBe "foo"
            repo.properties["a"] shouldBe "b"
        }

        "get repository with invalid name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.repositories.getRepository("bad/repo")
            }
        }

        "get non-existent repository fails" {
            shouldThrow<NoSuchObjectException> {
                providers.repositories.getRepository("bar")
            }
        }

        "list repositories succeeds" {
            createRepository()
            providers.repositories.createRepository(Repository(name = "bar"))
            val repos = providers.repositories.listRepositories().sortedBy { it.name }
            repos.size shouldBe 2
            repos[0].name shouldBe "bar"
            repos[1].name shouldBe "foo"
        }

        "update repository succeeds" {
            createRepository()
            providers.repositories.updateRepository("foo", Repository(name = "bar", properties = mapOf("b" to "c")))
            val repo = providers.repositories.getRepository("bar")
            repo.name shouldBe "bar"
            repo.properties["b"] shouldBe "c"
            shouldThrow<NoSuchObjectException> {
                providers.repositories.getRepository("foo")
            }
        }

        "update repository with invalid source name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.repositories.updateRepository("bad/repo", Repository(name = "foo"))
            }
        }

        "update repository with invalid new name fails" {
            createRepository()
            shouldThrow<IllegalArgumentException> {
                providers.repositories.updateRepository("foo", Repository(name = "bad/repo"))
            }
        }

        "update non-existent repository fails" {
            shouldThrow<NoSuchObjectException> {
                providers.repositories.updateRepository("foo", Repository(name = "foo"))
            }
        }

        "rename to conflicting repo fails" {
            createRepository()
            providers.repositories.createRepository(Repository(name = "bar"))
            shouldThrow<ObjectExistsException> {
                providers.repositories.updateRepository("foo", Repository(name = "bar"))
            }
        }

        "delete repository succeeds" {
            every { reaper.signal() } just Runs
            createRepository()
            providers.repositories.deleteRepository("foo")
            shouldThrow<NoSuchObjectException> {
                providers.repositories.getRepository("foo")
            }
            verify {
                reaper.signal()
            }
        }

        "get repository status succeeds" {
            createRepository()
            every { zfsStorageProvider.createVolume(any(), any()) } returns emptyMap()
            providers.volumes.createVolume("foo", Volume("vol1"))
            providers.volumes.createVolume("foo", Volume("vol2"))
            every { zfsStorageProvider.createCommit(any(), any(), any()) } just Runs
            providers.commits.createCommit("foo", Commit(id = "id"))
            every { zfsStorageProvider.getVolumeStatus(any(), any()) } returns RepositoryVolumeStatus(
                name = "vol", logicalSize = 20, actualSize = 10)
            val status = providers.repositories.getRepositoryStatus("foo")
            status.lastCommit shouldBe "id"
            status.sourceCommit shouldBe "id"
            status.volumeStatus.size shouldBe 2
            val vols = status.volumeStatus.sortedBy { it.name }
            vols[0].actualSize shouldBe 10
            vols[0].actualSize shouldBe 10
            vols[0].logicalSize shouldBe 20
            vols[0].name shouldBe "vol1"
            vols[1].logicalSize shouldBe 20
            vols[1].name shouldBe "vol2"
        }
    }
}
