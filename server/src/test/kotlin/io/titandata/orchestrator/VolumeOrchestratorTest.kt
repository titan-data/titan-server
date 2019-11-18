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
import io.titandata.ServiceLocator
import io.titandata.context.docker.DockerZfsContext
import io.titandata.exception.NoSuchObjectException
import io.titandata.models.Repository
import io.titandata.models.Volume
import org.jetbrains.exposed.sql.transactions.transaction

class VolumeOrchestratorTest : StringSpec() {

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
            services.metadata.createRepository(Repository(name = "foo"))
            vs = services.metadata.createVolumeSet("foo", null, true)
        }
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    fun createVolume(): Volume {
        every { dockerZfsContext.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
        return services.volumes.createVolume("foo", Volume("vol", mapOf("a" to "b")))
    }

    init {
        "create and get volume succeeds" {
            val vol = createVolume()
            vol.name shouldBe "vol"
            vol.config["mountpoint"] shouldBe "/mountpoint"
            vol.properties["a"] shouldBe "b"
            verifyAll {
                dockerZfsContext.createVolume(vs, "vol")
            }
        }

        "create volume with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                services.volumes.createVolume("bad/repo", Volume("vol"))
            }
        }

        "create volume with invalid volume name fails" {
            shouldThrow<IllegalArgumentException> {
                services.volumes.createVolume("repo", Volume("bad/vol"))
            }
        }

        "create volume with non-existent repo fails" {
            shouldThrow<NoSuchObjectException> {
                services.volumes.createVolume("bar", Volume("vol"))
            }
        }

        "delete volume marks volume for deletion" {
            every { reaper.signal() } just Runs

            createVolume()

            services.volumes.deleteVolume("foo", "vol")

            shouldThrow<NoSuchObjectException> {
                services.volumes.getVolume("foo", "vol")
            }

            verifyAll {
                reaper.signal()
            }
        }

        "delete volume with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                services.volumes.deleteVolume("bad/repo", "vol")
            }
        }

        "delete volume with invalid volume name fails" {
            shouldThrow<IllegalArgumentException> {
                services.volumes.deleteVolume("repo", "bad/vol")
            }
        }

        "delete volume with non-existent repository fails" {
            shouldThrow<NoSuchObjectException> {
                services.volumes.deleteVolume("bar", "vol")
            }
        }

        "delete non-existent volume fails" {
            shouldThrow<NoSuchObjectException> {
                services.volumes.deleteVolume("foo", "vol")
            }
        }

        "get volume succeeds" {
            createVolume()
            val vol = services.volumes.getVolume("foo", "vol")
            vol.name shouldBe "vol"
            vol.config["mountpoint"] shouldBe "/mountpoint"
            vol.properties["a"] shouldBe "b"
        }

        "get volume with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                services.volumes.getVolume("bad/repo", "vol")
            }
        }

        "get volume with invalid volume name fails" {
            shouldThrow<IllegalArgumentException> {
                services.volumes.getVolume("repo", "bad/vol")
            }
        }

        "get volume for non-existing repo fails" {
            shouldThrow<NoSuchObjectException> {
                services.volumes.getVolume("bar", "vol")
            }
        }

        "activate volume succeeds" {
            every { dockerZfsContext.activateVolume(any(), any(), any()) } just Runs
            createVolume()
            services.volumes.activateVolume("foo", "vol")
            verify {
                dockerZfsContext.activateVolume(vs, "vol", mapOf("mountpoint" to "/mountpoint"))
            }
        }

        "activate volume with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                services.volumes.activateVolume("bad/repo", "vol")
            }
        }

        "activate volume with invalid volume name fails" {
            shouldThrow<IllegalArgumentException> {
                services.volumes.activateVolume("repo", "bad/vol")
            }
        }

        "activate volume for non-existing repo fails" {
            shouldThrow<NoSuchObjectException> {
                services.volumes.activateVolume("bar", "vol")
            }
        }

        "activate volume for non-existing volume fails" {
            shouldThrow<NoSuchObjectException> {
                services.volumes.activateVolume("foo", "vol")
            }
        }

        "inactivate volume succeeds" {
            every { dockerZfsContext.deactivateVolume(any(), any(), any()) } just Runs
            createVolume()
            services.volumes.deactivateVolume("foo", "vol")
            verify {
                dockerZfsContext.deactivateVolume(vs, "vol", mapOf("mountpoint" to "/mountpoint"))
            }
        }

        "inactivate volume with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                services.volumes.deactivateVolume("bad/repo", "vol")
            }
        }

        "inactivate volume with invalid volume name fails" {
            shouldThrow<IllegalArgumentException> {
                services.volumes.deactivateVolume("repo", "bad/vol")
            }
        }

        "inactivate volume for non-existing repo fails" {
            shouldThrow<NoSuchObjectException> {
                services.volumes.deactivateVolume("bar", "vol")
            }
        }

        "inactivate volume for non-existing volume fails" {
            shouldThrow<NoSuchObjectException> {
                services.volumes.deactivateVolume("foo", "vol")
            }
        }

        "list volumes succeeds" {
            createVolume()

            transaction {
                services.metadata.createRepository(Repository(name = "bar"))
                services.metadata.createVolumeSet("bar", null, true)
                services.volumes.createVolume("bar", Volume("vol2"))
            }

            val volumes = services.volumes.listVolumes("foo")
            volumes.size shouldBe 1
            volumes[0].name shouldBe "vol"
        }
    }
}
