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
import io.titandata.models.Volume
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
                provider.createVolume("not/ok", "guid", Volume(name="ok", properties=emptyMap()))
            }
        }

        "create volume fails with invalid volume name" {
            shouldThrow<IllegalArgumentException> {
                provider.createVolume("ok", "guid", Volume(name="not/ok", properties=emptyMap()))
            }
        }

        "create volume fails with leading unsore in volume name" {
            shouldThrow<IllegalArgumentException> {
                provider.createVolume("ok", "guid", Volume(name="_vol", properties=emptyMap()))
            }
        }

        "create volume fails with duplicate volume" {
            every { executor.exec("zfs", "create", "-o",
                    "io.titan-data:metadata={\"a\":\"b\"}", "test/repo/foo/guid/vol") } throws
                    CommandException("", 1, "already exists")
            shouldThrow<ObjectExistsException> {
                provider.createVolume("foo", "guid", Volume(name="vol", properties=mapOf("a" to "b")))
            }
        }

        "create volume succeeds" {
            every { executor.exec("zfs", "create", "-o",
                    "io.titan-data:metadata={\"a\":\"b\"}", "test/repo/foo/guid/vol") } returns ""
            every { executor.exec("zfs", "snapshot", "test/repo/foo/guid/vol@initial") } returns ""
            every { executor.exec("mkdir", "-p", "/var/lib/test/mnt/foo/vol") } returns ""

            provider.createVolume("foo", "guid", Volume(name="vol", properties=mapOf("a" to "b")))

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

        "mount volume fails with invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                provider.mountVolume("not/ok", "guid", Volume(name="ok", properties=emptyMap()))
            }
        }

        "mount volume fails with invalid volume name" {
            shouldThrow<IllegalArgumentException> {
                provider.mountVolume("ok", "guid", Volume(name="not/ok", properties=emptyMap()))
            }
        }

        "mount volume succeeds" {
            every { executor.exec("zfs", "list", "-Ho", "io.titan-data:metadata",
                    "test/repo/foo/guid/vol") } returns "{}"
            every { executor.exec("mount", "-t", "zfs", "test/repo/foo/guid/vol",
                    "/var/lib/test/mnt/foo/vol") } returns ""
            provider.mountVolume("foo", "guid", Volume(name="vol", properties=emptyMap()))
            verifySequence {
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
    }
}
