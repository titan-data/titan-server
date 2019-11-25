/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.orchestrator

import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.matchers.collections.shouldBeEmpty
import io.kotlintest.matchers.collections.shouldNotBeEmpty
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
import io.titandata.ServiceLocator
import io.titandata.context.docker.DockerZfsContext
import io.titandata.exception.NoSuchObjectException
import io.titandata.metadata.OperationData
import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.RemoteParameters
import io.titandata.models.Repository
import io.titandata.models.Volume
import org.jetbrains.exposed.sql.transactions.transaction

class ReaperTest : StringSpec() {

    @MockK
    lateinit var context: DockerZfsContext

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

    init {
        "delete volumes does nothing with no volumes" {
            services.reaper.reapVolumes() shouldBe false
        }

        "delete volumes does nothing with no deleting volumes" {
            val vs = transaction {
                services.metadata.createRepository(Repository("foo"))
                val vs = services.metadata.createVolumeSet("foo")
                services.metadata.createVolume(vs, Volume("volume"))
                vs
            }
            services.reaper.reapVolumes() shouldBe false
            transaction {
                services.metadata.getVolume(vs, "volume")
            }
        }

        "delete volumes deletes marked volumes" {
            val vs = transaction {
                services.metadata.createRepository(Repository("foo"))
                val vs = services.metadata.createVolumeSet("foo")
                services.metadata.createVolume(vs, Volume("volume"))
                services.metadata.markVolumeDeleting(vs, "volume")
                vs
            }
            every { context.deleteVolume(any(), any(), any()) } just Runs
            services.reaper.reapVolumes() shouldBe true
            verify {
                context.deleteVolume(vs, "volume", emptyMap())
            }
            shouldThrow<NoSuchObjectException> {
                transaction {
                    services.metadata.getVolume(vs, "volume")
                }
            }
        }

        "delete empty volume sets does nothing with no volume sets" {
            services.reaper.reapVolumeSets() shouldBe false
        }

        "delete empty volume sets does nothing with inactive volume sets" {
            transaction {
                services.metadata.createRepository(Repository("foo"))
                services.metadata.createVolumeSet("foo")
            }
            services.reaper.reapVolumeSets() shouldBe false
        }

        "delete empty volume set does nothing while commits still exist" {
            transaction {
                services.metadata.createRepository(Repository("foo"))
                val vs = services.metadata.createVolumeSet("foo")
                services.metadata.createCommit("foo", vs, Commit("id"))
                services.metadata.markVolumeSetDeleting(vs)
            }
            services.reaper.reapVolumeSets() shouldBe false
        }

        "delete empty volume succeeds with no commits" {
            val vs = transaction {
                services.metadata.createRepository(Repository("foo"))
                val vs = services.metadata.createVolumeSet("foo")
                services.metadata.createVolume(vs, Volume("volume"))
                services.metadata.markVolumeSetDeleting(vs)
                vs
            }
            every { context.deleteVolume(any(), any(), any()) } just Runs
            every { context.deleteVolumeSet(any()) } just Runs

            services.reaper.reapVolumeSets() shouldBe true

            verify {
                context.deleteVolume(vs, "volume", emptyMap())
                context.deleteVolumeSet(vs)
            }

            transaction {
                services.metadata.listDeletingVolumes().shouldBeEmpty()
                services.metadata.listDeletingVolumeSets().shouldBeEmpty()
            }
        }

        "mark empty volume sets ignores volume sets with commits" {
            transaction {
                services.metadata.createRepository(Repository("foo"))
                val vs = services.metadata.createVolumeSet("foo")
                services.metadata.createCommit("foo", vs, Commit("id"))
            }
            services.reaper.markEmptyVolumeSets()
            transaction {
                services.metadata.listDeletingVolumeSets().shouldBeEmpty()
            }
        }

        "mark empty volume sets ignores volume sets with operations" {
            transaction {
                services.metadata.createRepository(Repository("foo"))
                val vs = services.metadata.createVolumeSet("foo")
                services.metadata.createOperation("foo", vs, OperationData(
                        operation = Operation(
                                id = vs,
                                type = Operation.Type.PUSH,
                                state = Operation.State.COMPLETE,
                                remote = "origin",
                                commitId = "commit"
                        ),
                        params = RemoteParameters("nop"),
                        metadataOnly = false,
                        repo = "foo"
                ))
            }
            services.reaper.markEmptyVolumeSets()
            transaction {
                services.metadata.listDeletingVolumeSets().shouldBeEmpty()
            }
        }

        "mark empty volume sets ignores active volume sets" {
            transaction {
                services.metadata.createRepository(Repository("foo"))
                services.metadata.createVolumeSet("foo", null, true)
            }
            services.reaper.markEmptyVolumeSets()
            transaction {
                services.metadata.listDeletingVolumeSets().shouldBeEmpty()
            }
        }

        "mark empty volume sets succeeds" {
            val vs = transaction {
                services.metadata.createRepository(Repository("foo"))
                services.metadata.createVolumeSet("foo")
            }
            services.reaper.markEmptyVolumeSets()
            transaction {
                val volumeSets = services.metadata.listDeletingVolumeSets()
                volumeSets.size shouldBe 1
                volumeSets[0] shouldBe vs
            }
        }

        "reap commits does nothing with no commits" {
            services.reaper.reapCommits() shouldBe false
        }

        "reap commits does nothing with no deleting commits" {
            transaction {
                services.metadata.createRepository(Repository("foo"))
                val vs = services.metadata.createVolumeSet("foo")
                services.metadata.createCommit("foo", vs, Commit("id"))
            }
            services.reaper.reapCommits() shouldBe false
        }

        "reap commits ignores commits with clones" {
            transaction {
                services.metadata.createRepository(Repository("foo"))
                val vs = services.metadata.createVolumeSet("foo")
                services.metadata.createCommit("foo", vs, Commit("src"))
                services.metadata.createVolumeSet("foo", "src")
                services.metadata.markCommitDeleting("foo", "src")
            }
            services.reaper.reapCommits() shouldBe false
            transaction {
                services.metadata.listDeletingCommits().shouldNotBeEmpty()
            }
        }

        "reap commits succeeds" {
            val vs = transaction {
                services.metadata.createRepository(Repository("foo"))
                val vs = services.metadata.createVolumeSet("foo")
                services.metadata.createVolume(vs, Volume("volume"))
                services.metadata.createCommit("foo", vs, Commit("id"))
                services.metadata.markCommitDeleting("foo", "id")
                vs
            }
            every { context.deleteVolumeSetCommit(any(), any()) } just Runs
            every { context.deleteVolumeCommit(any(), any(), any()) } just Runs
            services.reaper.reapCommits() shouldBe true
            verify {
                context.deleteVolumeSetCommit(vs, "id")
                context.deleteVolumeCommit(vs, "id", "volume")
            }
            transaction {
                services.metadata.listDeletingCommits().shouldBeEmpty()
            }
        }
    }
}
