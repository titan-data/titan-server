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
import io.titandata.models.Repository
import io.titandata.models.Volume
import io.titandata.storage.zfs.ZfsStorageProvider
import org.jetbrains.exposed.sql.transactions.transaction

class VolumeOrchestratorTest : StringSpec() {

    @MockK
    lateinit var zfsStorageProvider: ZfsStorageProvider

    @MockK
    lateinit var reaper: Reaper

    lateinit var vs: String

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
            vs = providers.metadata.createVolumeSet("foo", null, true)
        }
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    fun createVolume(): Volume {
        every { zfsStorageProvider.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
        return providers.volumes.createVolume("foo", Volume("vol", mapOf("a" to "b")))
    }

    init {
        "create and get volume succeeds" {
            val vol = createVolume()
            vol.name shouldBe "vol"
            vol.config["mountpoint"] shouldBe "/mountpoint"
            vol.properties["a"] shouldBe "b"
            verifyAll {
                zfsStorageProvider.createVolume(vs, "vol")
            }
        }

        "create volume with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.volumes.createVolume("bad/repo", Volume("vol"))
            }
        }

        "create volume with invalid volume name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.volumes.createVolume("repo", Volume("bad/vol"))
            }
        }

        "create volume with non-existent repo fails" {
            shouldThrow<NoSuchObjectException> {
                providers.volumes.createVolume("bar", Volume("vol"))
            }
        }

        "delete volume marks volume for deletion" {
            every { reaper.signal() } just Runs

            createVolume()

            providers.volumes.deleteVolume("foo", "vol")

            shouldThrow<NoSuchObjectException> {
                providers.volumes.getVolume("foo", "vol")
            }

            verifyAll {
                reaper.signal()
            }
        }

        "delete volume with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.volumes.deleteVolume("bad/repo", "vol")
            }
        }

        "delete volume with invalid volume name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.volumes.deleteVolume("repo", "bad/vol")
            }
        }

        "delete volume with non-existent repository fails" {
            shouldThrow<NoSuchObjectException> {
                providers.volumes.deleteVolume("bar", "vol")
            }
        }

        "delete non-existent volume fails" {
            shouldThrow<NoSuchObjectException> {
                providers.volumes.deleteVolume("foo", "vol")
            }
        }

        "get volume succeeds" {
            createVolume()
            val vol = providers.volumes.getVolume("foo", "vol")
            vol.name shouldBe "vol"
            vol.config["mountpoint"] shouldBe "/mountpoint"
            vol.properties["a"] shouldBe "b"
        }

        "get volume with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.volumes.getVolume("bad/repo", "vol")
            }
        }

        "get volume with invalid volume name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.volumes.getVolume("repo", "bad/vol")
            }
        }

        "get volume for non-existing repo fails" {
            shouldThrow<NoSuchObjectException> {
                providers.volumes.getVolume("bar", "vol")
            }
        }

        "activate volume succeeds" {
            every { zfsStorageProvider.activateVolume(any(), any(), any()) } just Runs
            createVolume()
            providers.volumes.activateVolume("foo", "vol")
            verify {
                zfsStorageProvider.activateVolume(vs, "vol", mapOf("mountpoint" to "/mountpoint"))
            }
        }

        "activate volume with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.volumes.activateVolume("bad/repo", "vol")
            }
        }

        "activate volume with invalid volume name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.volumes.activateVolume("repo", "bad/vol")
            }
        }

        "activate volume for non-existing repo fails" {
            shouldThrow<NoSuchObjectException> {
                providers.volumes.activateVolume("bar", "vol")
            }
        }

        "activate volume for non-existing volume fails" {
            shouldThrow<NoSuchObjectException> {
                providers.volumes.activateVolume("foo", "vol")
            }
        }

        "inactivate volume succeeds" {
            every { zfsStorageProvider.inactivateVolume(any(), any(), any()) } just Runs
            createVolume()
            providers.volumes.inactivateVolume("foo", "vol")
            verify {
                zfsStorageProvider.inactivateVolume(vs, "vol", mapOf("mountpoint" to "/mountpoint"))
            }
        }

        "inactivate volume with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.volumes.inactivateVolume("bad/repo", "vol")
            }
        }

        "inactivate volume with invalid volume name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.volumes.inactivateVolume("repo", "bad/vol")
            }
        }

        "inactivate volume for non-existing repo fails" {
            shouldThrow<NoSuchObjectException> {
                providers.volumes.inactivateVolume("bar", "vol")
            }
        }

        "inactivate volume for non-existing volume fails" {
            shouldThrow<NoSuchObjectException> {
                providers.volumes.inactivateVolume("foo", "vol")
            }
        }

        "list volumes succeeds" {
            createVolume()

            transaction {
                providers.metadata.createRepository(Repository(name = "bar"))
                providers.metadata.createVolumeSet("bar", null, true)
                providers.volumes.createVolume("bar", Volume("vol2"))
            }

            val volumes = providers.volumes.listVolumes("foo")
            volumes.size shouldBe 1
            volumes[0].name shouldBe "vol"
        }
    }
}
