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
import io.titandata.models.docker.DockerVolume
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

    fun createVolume(): DockerVolume {
        every { zfsStorageProvider.createVolume(any(), any()) } just Runs
        every { zfsStorageProvider.getVolumeMountpoint(any(), any()) } returns "/mountpoint"
        return providers.volumes.createVolume("foo", "vol", mapOf("a" to "b"))
    }

    init {
        "create and get volume succeeds" {
            val vol = createVolume()
            vol.name shouldBe "foo/vol"
            vol.mountpoint shouldBe "/mountpoint"
            vol.properties["a"] shouldBe "b"
            verifyAll {
                zfsStorageProvider.createVolume(vs, "vol")
                zfsStorageProvider.getVolumeMountpoint(vs, "vol")
            }
        }

        "create volume with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.volumes.createVolume("bad/repo", "vol", emptyMap())
            }
        }

        "create volume with invalid volume name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.volumes.createVolume("repo", "bad/vol", emptyMap())
            }
        }

        "create volume with non-existent repo fails" {
            shouldThrow<NoSuchObjectException> {
                providers.volumes.createVolume("bar", "vol", emptyMap())
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
            vol.name shouldBe "foo/vol"
            vol.mountpoint shouldBe "/mountpoint"
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

        "mount volume succeeds" {
            every { zfsStorageProvider.mountVolume(any(), any()) } returns ""
            createVolume()
            providers.volumes.mountVolume("foo", "vol")
            verify {
                zfsStorageProvider.mountVolume(vs, "vol")
            }
        }

        "mount volume with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.volumes.mountVolume("bad/repo", "vol")
            }
        }

        "mount volume with invalid volume name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.volumes.mountVolume("repo", "bad/vol")
            }
        }

        "mount volume for non-existing repo fails" {
            shouldThrow<NoSuchObjectException> {
                providers.volumes.mountVolume("bar", "vol")
            }
        }

        "mount volume for non-existing volume fails" {
            shouldThrow<NoSuchObjectException> {
                providers.volumes.mountVolume("foo", "vol")
            }
        }

        "unmount volume succeeds" {
            every { zfsStorageProvider.unmountVolume(any(), any()) } just Runs
            createVolume()
            providers.volumes.unmountVolume("foo", "vol")
            verify {
                zfsStorageProvider.unmountVolume(vs, "vol")
            }
        }

        "unmount volume with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.volumes.unmountVolume("bad/repo", "vol")
            }
        }

        "unmount volume with invalid volume name fails" {
            shouldThrow<IllegalArgumentException> {
                providers.volumes.unmountVolume("repo", "bad/vol")
            }
        }

        "unmount volume for non-existing repo fails" {
            shouldThrow<NoSuchObjectException> {
                providers.volumes.unmountVolume("bar", "vol")
            }
        }

        "unmount volume for non-existing volume fails" {
            shouldThrow<NoSuchObjectException> {
                providers.volumes.unmountVolume("foo", "vol")
            }
        }

        "list all volumes succeeds" {
            createVolume()

            transaction {
                providers.metadata.createRepository(Repository(name = "bar"))
                providers.metadata.createVolumeSet("bar", null, true)
                providers.volumes.createVolume("bar", "vol2", emptyMap())
            }

            val volumes = providers.volumes.listAllVolumes().sortedBy { it.name }
            volumes.size shouldBe 2
            volumes[0].name shouldBe "bar/vol2"
            volumes[1].name shouldBe "foo/vol"
        }
    }
}
