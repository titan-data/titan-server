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
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyAll
import io.titandata.ServiceLocator
import io.titandata.context.docker.DockerZfsContext
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Commit
import io.titandata.models.Repository
import io.titandata.models.RepositoryVolumeStatus
import io.titandata.models.Volume
import org.jetbrains.exposed.sql.transactions.transaction

class RepositoryOrchestratorTest : StringSpec() {

    @MockK
    lateinit var context: DockerZfsContext

    @MockK
    lateinit var reaper: Reaper

    @InjectMockKs
    @OverrideMockKs
    var services = ServiceLocator(mockk())

    override fun beforeSpec(spec: Spec) {
        services.metadata.init()
    }

    override fun beforeTest(testCase: TestCase) {
        services.metadata.clear()
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    fun createRepository() {
        every { context.createVolumeSet(any()) } just Runs
        services.repositories.createRepository(Repository(name = "foo", properties = mapOf("a" to "b")))
    }

    init {
        "create repository succeeds" {
            createRepository()
            val vs = transaction {
                services.metadata.getActiveVolumeSet("foo")
            }
            verifyAll {
                context.createVolumeSet(vs)
            }
        }

        "create repository with invalid name fails" {
            shouldThrow<IllegalArgumentException> {
                services.repositories.createRepository(Repository(name = "a".repeat(65)))
            }
        }

        "get repository succeeds" {
            createRepository()
            val repo = services.repositories.getRepository("foo")
            repo.name shouldBe "foo"
            repo.properties["a"] shouldBe "b"
        }

        "get repository with invalid name fails" {
            shouldThrow<IllegalArgumentException> {
                services.repositories.getRepository("bad/repo")
            }
        }

        "get non-existent repository fails" {
            shouldThrow<NoSuchObjectException> {
                services.repositories.getRepository("bar")
            }
        }

        "list repositories succeeds" {
            createRepository()
            services.repositories.createRepository(Repository(name = "bar"))
            val repos = services.repositories.listRepositories().sortedBy { it.name }
            repos.size shouldBe 2
            repos[0].name shouldBe "bar"
            repos[1].name shouldBe "foo"
        }

        "update repository succeeds" {
            createRepository()
            services.repositories.updateRepository("foo", Repository(name = "bar", properties = mapOf("b" to "c")))
            val repo = services.repositories.getRepository("bar")
            repo.name shouldBe "bar"
            repo.properties["b"] shouldBe "c"
            shouldThrow<NoSuchObjectException> {
                services.repositories.getRepository("foo")
            }
        }

        "update repository with invalid source name fails" {
            shouldThrow<IllegalArgumentException> {
                services.repositories.updateRepository("bad/repo", Repository(name = "foo"))
            }
        }

        "update repository with invalid new name fails" {
            createRepository()
            shouldThrow<IllegalArgumentException> {
                services.repositories.updateRepository("foo", Repository(name = "bad/repo"))
            }
        }

        "update non-existent repository fails" {
            shouldThrow<NoSuchObjectException> {
                services.repositories.updateRepository("foo", Repository(name = "foo"))
            }
        }

        "rename to conflicting repo fails" {
            createRepository()
            services.repositories.createRepository(Repository(name = "bar"))
            shouldThrow<ObjectExistsException> {
                services.repositories.updateRepository("foo", Repository(name = "bar"))
            }
        }

        "delete repository succeeds" {
            every { reaper.signal() } just Runs
            createRepository()
            services.repositories.deleteRepository("foo")
            shouldThrow<NoSuchObjectException> {
                services.repositories.getRepository("foo")
            }
            verify {
                reaper.signal()
            }
        }

        "get repository status succeeds" {
            createRepository()
            every { context.createVolume(any(), any()) } returns emptyMap()
            services.volumes.createVolume("foo", Volume("vol1"))
            services.volumes.createVolume("foo", Volume("vol2"))
            every { context.commitVolumeSet(any(), any()) } just Runs
            every { context.commitVolume(any(), any(), any(), any()) } just Runs
            services.commits.createCommit("foo", Commit(id = "id"))
            every { context.getVolumeStatus(any(), any()) } returns RepositoryVolumeStatus(
                name = "vol", logicalSize = 20, actualSize = 10)
            val status = services.repositories.getRepositoryStatus("foo")
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
