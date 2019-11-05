/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.storage.zfs

import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.mockk.MockKAnnotations
import io.mockk.clearAllMocks
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.impl.annotations.OverrideMockKs
import io.mockk.verifySequence
import io.titandata.exception.CommandException
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.util.CommandExecutor

class ZfsVolumeTest : StringSpec() {

    @MockK
    lateinit var executor: CommandExecutor

    @InjectMockKs
    @OverrideMockKs
    var provider = ZfsStorageProvider("test")

    override fun beforeTest(testCase: TestCase) {
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    init {
        "create volume fails with invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                provider.createVolume("not/ok", "guid", "ok", mapOf())
            }
        }

        "create volume fails with invalid volume name" {
            shouldThrow<IllegalArgumentException> {
                provider.createVolume("ok", "guid", "not/ok", mapOf())
            }
        }

        "create volume fails with leading unsore in volume name" {
            shouldThrow<IllegalArgumentException> {
                provider.createVolume("ok", "guid", "_vol", mapOf())
            }
        }

        "create volume fails with duplicate volume" {
            every { executor.exec("zfs", "create", "-o",
                    "io.titan-data:metadata={\"a\":\"b\"}", "test/repo/foo/guid/vol") } throws
                    CommandException("", 1, "already exists")
            shouldThrow<ObjectExistsException> {
                provider.createVolume("foo", "guid", "vol", mapOf("a" to "b"))
            }
        }

        "create volume succeeds" {
            every { executor.exec("zfs", "create", "-o",
                    "io.titan-data:metadata={\"a\":\"b\"}", "test/repo/foo/guid/vol") } returns ""
            every { executor.exec("zfs", "snapshot", "test/repo/foo/guid/vol@initial") } returns ""
            every { executor.exec("mkdir", "-p", "/var/lib/test/mnt/foo/vol") } returns ""

            val vol = provider.createVolume("foo", "guid", "vol", mapOf("a" to "b"))
            vol.name shouldBe "vol"
            vol.mountpoint shouldBe "/var/lib/test/mnt/foo/vol"
            vol.properties!!["a"] shouldBe "b"
            vol.status shouldNotBe null

            verifySequence {
                executor.exec("zfs", "create", "-o",
                        "io.titan-data:metadata={\"a\":\"b\"}", "test/repo/foo/guid/vol")
                executor.exec("zfs", "snapshot", "test/repo/foo/guid/vol@initial")
                executor.exec("mkdir", "-p", "/var/lib/test/mnt/foo/vol")
            }
            confirmVerified()
        }

        "delete volume fails with invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                provider.deleteVolume("not/ok", "guid", "ok")
            }
        }

        "delete volume fails with invalid volume name" {
            shouldThrow<IllegalArgumentException> {
                provider.deleteVolume("ok", "guid", "not/ok")
            }
        }

        "delete volume succeeds" {
            every { executor.exec("zfs", "destroy", "-R", "test/repo/foo/guid/vol") } returns ""
            every { executor.exec("rmdir", "/var/lib/test/mnt/foo/vol") } returns ""
            provider.deleteVolume("foo", "guid", "vol")
            verifySequence {
                executor.exec("zfs", "destroy", "-R", "test/repo/foo/guid/vol")
                executor.exec("rmdir", "/var/lib/test/mnt/foo/vol")
            }
            confirmVerified()
        }

        "delete volume fails for unknown volume" {
            every { executor.exec("zfs", "destroy", "-R", "test/repo/foo/guid/vol") } throws
                    CommandException("", 1, "does not exist")
            shouldThrow<NoSuchObjectException> {
                provider.deleteVolume("foo", "guid", "vol")
            }
        }

        "get volume fails with invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                provider.getVolume("not/ok", "guid", "ok")
            }
        }

        "get volume fails with invalid volume name" {
            shouldThrow<IllegalArgumentException> {
                provider.getVolume("ok", "guid", "not/ok")
            }
        }

        "get volume succeeds" {
            every { executor.exec("zfs", "list", "-Ho", "io.titan-data:metadata",
                    "test/repo/foo/guid/vol") } returns "{\"a\":\"b\"}"
            val vol = provider.getVolume("foo", "guid", "vol")
            vol.name shouldBe "vol"
            vol.mountpoint shouldBe "/var/lib/test/mnt/foo/vol"
            vol.status shouldNotBe null
            vol.properties!!["a"] shouldBe "b"

            verifySequence {
                executor.exec("zfs", "list", "-Ho", "io.titan-data:metadata",
                        "test/repo/foo/guid/vol")
            }
            confirmVerified()
        }

        "mount volume fails with invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                provider.mountVolume("not/ok", "guid", "ok")
            }
        }

        "mount volume fails with invalid volume name" {
            shouldThrow<IllegalArgumentException> {
                provider.mountVolume("ok", "guid", "not/ok")
            }
        }

        "mount volume succeeds" {
            every { executor.exec("zfs", "list", "-Ho", "io.titan-data:metadata",
                    "test/repo/foo/guid/vol") } returns "{}"
            every { executor.exec("mount", "-t", "zfs", "test/repo/foo/guid/vol",
                    "/var/lib/test/mnt/foo/vol") } returns ""
            val vol = provider.mountVolume("foo", "guid", "vol")
            vol.name shouldBe "vol"
            vol.mountpoint shouldBe "/var/lib/test/mnt/foo/vol"
            verifySequence {
                executor.exec("zfs", "list", "-Ho", "io.titan-data:metadata",
                        "test/repo/foo/guid/vol")
                executor.exec("mount", "-t", "zfs", "test/repo/foo/guid/vol",
                        "/var/lib/test/mnt/foo/vol")
            }
            confirmVerified()
        }

        "unmount volume fails with invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                provider.unmountVolume("not/ok", "ok")
            }
        }

        "unmount volume fails with invalid volume name" {
            shouldThrow<IllegalArgumentException> {
                provider.unmountVolume("ok", "not/ok")
            }
        }

        "unmount volume succeeds" {
            every { executor.exec("umount", "/var/lib/test/mnt/foo/vol") } returns ""
            provider.unmountVolume("foo", "vol")
            verifySequence {
                executor.exec("umount", "/var/lib/test/mnt/foo/vol")
            }
            confirmVerified()
        }

        "unmount volume succeeds if not mounted" {
            every { executor.exec("umount", "/var/lib/test/mnt/foo/vol") } throws
                    CommandException("", 1, "not mounted")
            provider.unmountVolume("foo", "vol")
        }

        "list volumes fails with invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                provider.listVolumes("not/ok", "guid")
            }
        }

        "list volumes succeeds" {
            every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:metadata",
                    "-r", "test/repo/foo/guid") } returns arrayOf(
                    "test/repo/foo\t{}",
                    "test/repo/foo/guid/one\t{\"a\":\"b\"}",
                    "test/repo/foo/guid/two\t{\"c\":\"d\"}"
            ).joinToString("\n")
            val volumes = provider.listVolumes("foo", "guid")
            volumes.size shouldBe 2
            volumes[0].name shouldBe "one"
            volumes[0].mountpoint shouldBe "/var/lib/test/mnt/foo/one"
            volumes[0].properties!!["a"] shouldBe "b"
            volumes[1].name shouldBe "two"
            volumes[1].mountpoint shouldBe "/var/lib/test/mnt/foo/two"
            volumes[1].properties!!["c"] shouldBe "d"
        }

        "list volumes ignores datasets with leading underscores" {
            every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:metadata",
                    "-r", "test/repo/foo/guid") } returns arrayOf(
                    "test/repo/foo\t{}",
                    "test/repo/foo/guid/one\t{\"a\":\"b\"}",
                    "test/repo/foo/guid/_two\t{\"c\":\"d\"}"
            ).joinToString("\n")
            val volumes = provider.listVolumes("foo", "guid")
            volumes.size shouldBe 1
            volumes[0].name shouldBe "one"
            volumes[0].mountpoint shouldBe "/var/lib/test/mnt/foo/one"
            volumes[0].properties!!["a"] shouldBe "b"
        }
    }
}
