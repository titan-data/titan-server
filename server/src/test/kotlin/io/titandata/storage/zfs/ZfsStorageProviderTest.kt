/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.storage.zfs

import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.mockk.MockKAnnotations
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.impl.annotations.OverrideMockKs
import io.mockk.verifyAll
import io.titandata.exception.CommandException
import io.titandata.util.CommandExecutor

class ZfsStorageProviderTest : StringSpec() {

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
        "provider defaults to titan as pool name" {
            val defaultProvider = ZfsStorageProvider()
            defaultProvider.poolName shouldBe "titan"
        }

        "create volume set succeeds" {
            every { executor.exec(*anyVararg()) } returns ""
            provider.createVolumeSet("vs")
            verifyAll {
                executor.exec("zfs", "create", "-o", "mountpoint=legacy", "test/data/vs")
            }
        }

        "clone volume set clones each volume" {
            every { executor.exec(*anyVararg()) } returns ""
            provider.cloneVolumeSet("source", "commit", "dest", listOf("one", "two"))
            verifyAll {
                executor.exec("zfs", "create", "-o", "mountpoint=legacy", "test/data/dest")
                executor.exec("zfs", "clone", "test/data/source/one@commit", "test/data/dest/one")
                executor.exec("zfs", "clone", "test/data/source/two@commit", "test/data/dest/two")
            }
        }

        "destroy volume set destroys dataset and removes directory" {
            every { executor.exec(*anyVararg()) } returns ""
            provider.deleteVolumeSet("vs")
            verifyAll {
                executor.exec("zfs", "destroy", "test/data/vs")
                executor.exec("rm", "-rf", "/var/lib/test/mnt/vs")
            }
        }

        "destroy volume set ignores no such file or directory" {
            every { executor.exec("zfs", *anyVararg()) } returns ""
            every { executor.exec("rm", *anyVararg()) } throws CommandException("", 1, "No such file or directory")
            provider.deleteVolumeSet("vs")
        }

        "destroy volume throws exception on other errors" {
            every { executor.exec("zfs", *anyVararg()) } returns ""
            every { executor.exec("rm", *anyVararg()) } throws CommandException("", 1, "")
            shouldThrow<CommandException> {
                provider.deleteVolumeSet("vs")
            }
        }

        "get volume status succeeds" {
            every { executor.exec(*anyVararg()) } returns "20\t30"
            val status = provider.getVolumeStatus("vs", "vol")
            status.name shouldBe "vol"
            status.logicalSize shouldBe 20
            status.actualSize shouldBe 30
            verifyAll {
                executor.exec("zfs", "list", "-pHo", "logicalreferenced,referenced", "test/data/vs/vol")
            }
        }

        "create commit succeeds" {
            every { executor.exec(*anyVararg()) } returns ""
            provider.createCommit("vs", "commit", listOf("one", "two"))
            verifyAll {
                executor.exec("zfs", "snapshot", "-r", "test/data/vs@commit")
            }
        }

        "get commit sums volume sizes" {
            every { executor.exec(*anyVararg()) } returns "1\t2\t3"
            val status = provider.getCommitStatus("vs", "commit", listOf("one", "two"))
            status.logicalSize shouldBe 2
            status.actualSize shouldBe 4
            status.uniqueSize shouldBe 6
            verifyAll {
                executor.exec("zfs", "list", "-Hpo", "logicalreferenced,referenced,used", "-t",
                        "snapshot", "test/data/vs/one@commit")
                executor.exec("zfs", "list", "-Hpo", "logicalreferenced,referenced,used", "-t",
                        "snapshot", "test/data/vs/two@commit")
            }
        }

        "delete commit succeeds" {
            every { executor.exec(*anyVararg()) } returns ""
            provider.deleteCommit("vs", "commit", listOf("one", "two"))
            verifyAll {
                executor.exec("zfs", "destroy", "-r", "test/data/vs@commit")
            }
        }

        "create volume succeeds" {
            every { executor.exec(*anyVararg()) } returns ""
            provider.createVolume("vs", "vol")
            verifyAll {
                executor.exec("zfs", "create", "test/data/vs/vol")
            }
        }

        "delete volume succeeds" {
            every { executor.exec(*anyVararg()) } returns ""
            provider.deleteVolume("vs", "vol")
            verifyAll {
                executor.exec("zfs", "destroy", "test/data/vs/vol")
                executor.exec("rmdir", "/var/lib/test/mnt/vs/vol")
            }
        }

        "mount volume succeeds" {
            every { executor.exec(*anyVararg()) } returns ""
            val mountpoint = provider.mountVolume("vs", "vol")
            mountpoint shouldBe "/var/lib/test/mnt/vs/vol"
            verifyAll {
                executor.exec("mkdir", "-p", mountpoint)
                executor.exec("mount", "-t", "zfs", "test/data/vs/vol", mountpoint)
            }
        }

        "unmount volume succeeds" {
            every { executor.exec(*anyVararg()) } returns ""
            provider.unmountVolume("vs", "vol")
            verifyAll {
                executor.exec("umount", "/var/lib/test/mnt/vs/vol")
            }
        }

        "unmount ignored not mounted error" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "not mounted")
            provider.unmountVolume("vs", "vol")
        }

        "unmount invokes lsof on EBUSY" {
            every { executor.exec("umount", *anyVararg()) } throws CommandException("", 1, "target is busy")
            every { executor.exec("lsof") } returns ""
            shouldThrow<CommandException> {
                provider.unmountVolume("vs", "vol")
            }
            verifyAll {
                executor.exec("umount", "/var/lib/test/mnt/vs/vol")
                executor.exec("lsof")
            }
        }

        "unmount ignores lsof failure" {
            every { executor.exec("umount", *anyVararg()) } throws CommandException("", 1, "target is busy")
            every { executor.exec("lsof") } throws CommandException("", 1, "")
            val ex = shouldThrow<CommandException> {
                provider.unmountVolume("vs", "vol")
            }
            ex.output shouldBe "target is busy"
            verifyAll {
                executor.exec("umount", "/var/lib/test/mnt/vs/vol")
                executor.exec("lsof")
            }
        }
    }
}
