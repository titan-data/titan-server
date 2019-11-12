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
import io.mockk.verify
import io.titandata.ProviderModule
import io.titandata.exception.NoSuchObjectException
import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.Repository
import io.titandata.models.Volume
import io.titandata.remote.nop.NopParameters
import io.titandata.storage.OperationData
import io.titandata.storage.zfs.ZfsStorageProvider
import org.jetbrains.exposed.sql.transactions.transaction

class ReaperTest : StringSpec() {

    @MockK
    lateinit var zfsStorageProvider: ZfsStorageProvider

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

    init {
        "delete volumes does nothing with no volumes" {
            providers.reaper.reapVolumes() shouldBe false
        }

        "delete volumes does nothing with no deleting volumes" {
            val vs = transaction {
                providers.metadata.createRepository(Repository("foo"))
                val vs = providers.metadata.createVolumeSet("foo")
                providers.metadata.createVolume(vs, Volume(name = "volume"))
                vs
            }
            providers.reaper.reapVolumes() shouldBe false
            transaction {
                providers.metadata.getVolume(vs, "volume")
            }
        }

        "delete volumes deletes marked volumes" {
            val vs = transaction {
                providers.metadata.createRepository(Repository("foo"))
                val vs = providers.metadata.createVolumeSet("foo")
                providers.metadata.createVolume(vs, Volume(name = "volume"))
                providers.metadata.markVolumeDeleting(vs, "volume")
                vs
            }
            every { zfsStorageProvider.deleteVolume(any(), any()) } just Runs
            providers.reaper.reapVolumes() shouldBe true
            verify {
                zfsStorageProvider.deleteVolume(vs, "volume")
            }
            shouldThrow<NoSuchObjectException> {
                transaction {
                    providers.metadata.getVolume(vs, "volume")
                }
            }
        }

        "delete empty volume sets does nothing with no volume sets" {
            providers.reaper.reapVolumeSets() shouldBe false
        }

        "delete empty volume sets does nothing with inactive volume sets" {
            transaction {
                providers.metadata.createRepository(Repository("foo"))
                providers.metadata.createVolumeSet("foo")
            }
            providers.reaper.reapVolumeSets() shouldBe false
        }

        "delete empty volume set does nothing while commits still exist" {
            transaction {
                providers.metadata.createRepository(Repository("foo"))
                val vs = providers.metadata.createVolumeSet("foo")
                providers.metadata.createCommit("foo", vs, Commit("id"))
                providers.metadata.markVolumeSetDeleting(vs)
            }
            providers.reaper.reapVolumeSets() shouldBe false
        }

        "delete empty volume succeeds with no commits" {
            val vs = transaction {
                providers.metadata.createRepository(Repository("foo"))
                val vs = providers.metadata.createVolumeSet("foo")
                providers.metadata.createVolume(vs, Volume(name = "volume"))
                providers.metadata.markVolumeSetDeleting(vs)
                vs
            }
            every { zfsStorageProvider.deleteVolume(any(), any()) } just Runs
            every { zfsStorageProvider.deleteVolumeSet(any()) } just Runs

            providers.reaper.reapVolumeSets() shouldBe true

            verify {
                zfsStorageProvider.deleteVolume(vs, "volume")
                zfsStorageProvider.deleteVolumeSet(vs)
            }

            transaction {
                providers.metadata.listDeletingVolumes().shouldBeEmpty()
                providers.metadata.listDeletingVolumeSets().shouldBeEmpty()
            }
        }

        "mark empty volume sets ignores volume sets with commits" {
            transaction {
                providers.metadata.createRepository(Repository("foo"))
                val vs = providers.metadata.createVolumeSet("foo")
                providers.metadata.createCommit("foo", vs, Commit("id"))
            }
            providers.reaper.markEmptyVolumeSets()
            transaction {
                providers.metadata.listDeletingVolumeSets().shouldBeEmpty()
            }
        }

        "mark empty volume sets ignores volume sets with operations" {
            transaction {
                providers.metadata.createRepository(Repository("foo"))
                val vs = providers.metadata.createVolumeSet("foo")
                providers.metadata.createOperation("foo", vs, OperationData(
                        operation = Operation(
                                id = vs,
                                type = Operation.Type.PUSH,
                                state = Operation.State.COMPLETE,
                                remote = "origin",
                                commitId = "commit"
                        ),
                        params = NopParameters(),
                        metadataOnly = false
                ))
            }
            providers.reaper.markEmptyVolumeSets()
            transaction {
                providers.metadata.listDeletingVolumeSets().shouldBeEmpty()
            }
        }

        "mark empty volume sets ignores active volume sets" {
            transaction {
                providers.metadata.createRepository(Repository("foo"))
                providers.metadata.createVolumeSet("foo", null, true)
            }
            providers.reaper.markEmptyVolumeSets()
            transaction {
                providers.metadata.listDeletingVolumeSets().shouldBeEmpty()
            }
        }

        "mark empty volume sets succeeds" {
            val vs = transaction {
                providers.metadata.createRepository(Repository("foo"))
                providers.metadata.createVolumeSet("foo")
            }
            providers.reaper.markEmptyVolumeSets()
            transaction {
                val volumeSets = providers.metadata.listDeletingVolumeSets()
                volumeSets.size shouldBe 1
                volumeSets[0] shouldBe vs
            }
        }

        "reap commits does nothing with no commits" {
            providers.reaper.reapCommits() shouldBe false
        }

        "reap commits does nothing with no deleting commits" {
            transaction {
                providers.metadata.createRepository(Repository("foo"))
                val vs = providers.metadata.createVolumeSet("foo")
                providers.metadata.createCommit("foo", vs, Commit("id"))
            }
            providers.reaper.reapCommits() shouldBe false
        }

        "reap commits ignores commits with clones" {
            transaction {
                providers.metadata.createRepository(Repository("foo"))
                val vs = providers.metadata.createVolumeSet("foo")
                providers.metadata.createCommit("foo", vs, Commit("src"))
                providers.metadata.createVolumeSet("foo", "src")
                providers.metadata.markCommitDeleting("foo", "src")
            }
            providers.reaper.reapCommits() shouldBe false
            transaction {
                providers.metadata.listDeletingCommits().shouldNotBeEmpty()
            }
        }

        "reap commits succeeds" {
            val vs = transaction {
                providers.metadata.createRepository(Repository("foo"))
                val vs = providers.metadata.createVolumeSet("foo")
                providers.metadata.createVolume(vs, Volume(name = "volume"))
                providers.metadata.createCommit("foo", vs, Commit("id"))
                providers.metadata.markCommitDeleting("foo", "id")
                vs
            }
            every { zfsStorageProvider.deleteCommit(any(), any(), any()) } just Runs
            providers.reaper.reapCommits() shouldBe true
            verify {
                zfsStorageProvider.deleteCommit(vs, "id", listOf("volume"))
            }
            transaction {
                providers.metadata.listDeletingCommits().shouldBeEmpty()
            }
        }
    }
}
