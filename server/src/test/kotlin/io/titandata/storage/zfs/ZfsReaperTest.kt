/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.storage.zfs

import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.specs.StringSpec
import io.mockk.MockKAnnotations
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.impl.annotations.OverrideMockKs
import io.mockk.verify
import io.titandata.exception.CommandException
import io.titandata.util.CommandExecutor

class ZfsReaperTest : StringSpec() {

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
        "reaper destroys datasets with the reaper property set" {
            every { executor.exec("zfs", "list", "-Hpo", "name,io.titan-data:reaper", "-r", "-d", "2", "test/repo") } returns arrayOf(
                    "test/repo/foo\t-",
                    "test/repo/foo/guid\t-",
                    "test/repo/bar\t-",
                    "test/repo/bar/guid\ton"
            ).joinToString("\n")
            every { executor.exec("zfs", "destroy", "-r", "test/repo/bar/guid") } returns ""

            provider.reaper.reap()

            verify {
                executor.exec("zfs", "destroy", "-r", "test/repo/bar/guid")
            }
        }

        "reaper ignores failure when destroying datasets" {
            every { executor.exec("zfs", "list", "-Hpo", "name,io.titan-data:reaper", "-r", "-d", "2", "test/repo") } returns arrayOf(
                    "test/repo/foo\t-",
                    "test/repo/foo/guid\t-",
                    "test/repo/bar\t-",
                    "test/repo/bar/guid\ton"
            ).joinToString("\n")
            every { executor.exec("zfs", "destroy", "-r", "test/repo/bar/guid") } throws CommandException("", 1, "dataset is busy")

            provider.reaper.reap()

            verify {
                executor.exec("zfs", "destroy", "-r", "test/repo/bar/guid")
            }
        }
    }
}
