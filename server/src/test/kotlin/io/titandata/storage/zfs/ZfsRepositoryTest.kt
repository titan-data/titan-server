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
import io.mockk.verify
import io.mockk.verifySequence
import io.titandata.exception.CommandException
import io.titandata.exception.InvalidStateException
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Repository
import io.titandata.util.CommandExecutor
import io.titandata.util.GuidGenerator

class ZfsRepositoryTest : StringSpec() {

    @MockK
    lateinit var executor: CommandExecutor

    @MockK
    lateinit var generator: GuidGenerator

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
        "repository info is parsed correctly" {
            val result = provider.repositoryManager.parseRepository("test/repo/name\t{\"a\": \"b\"}")
            result shouldNotBe null
            result!!.name shouldBe "name"
            result.properties["a"] shouldBe "b"
        }

        "repository info with tabs is parsed correctly" {
            val result = provider.repositoryManager.parseRepository("test/repo/name\t{\"a\": \"b\tc\"}\n")
            result shouldNotBe null
            result!!.properties["a"] shouldBe "b\tc"
        }

        "repository parsing returns null for pool" {
            val result = provider.repositoryManager.parseRepository("test\t{}\n")
            result shouldBe null
        }

        "repository parsing fails for invalid pool" {
            val result = provider.repositoryManager.parseRepository("test2/name\t{}")
            result shouldBe null
        }

        "repository info with bad metadata throws an exception" {
            shouldThrow<InvalidStateException> {
                provider.repositoryManager.parseRepository("test/repo/name\t-")
            }
        }

        "get repository status succeeds" {
            every { executor.exec(*anyVararg()) } returns ""
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy,io.titan-data:metadata", "-t", "snapshot",
                    "-d", "2", "test/repo/foo") } returns "test/repo/foo/guid@hash\toff\t{}\n"
            every { executor.exec("zfs", "list", "-Ho", "io.titan-data:metadata",
                    "test/repo/foo/guid@hash") } returns "{\"a\":\"b\"}\n"
            every { executor.exec("zfs", "list", "-Hpo", "io.titan-data:active",
                    "test/repo/foo") } returns "guid"
            every { executor.exec("zfs", "list", "-pHo", "logicalused,used", "test/repo/foo/guid") } returns "40\t20\n"
            every { executor.exec("zfs", "list", "-Ho", "origin", "-r", "test/repo/foo/guid") } returns
                    "test/repo/foo/guidtwo/v0@sourcehash\n"
            every { executor.exec("zfs", "list", "-d", "1",
                    "-pHo", "name,logicalreferenced,referenced,io.titan-data:metadata", "test/repo/foo/guid") } returns arrayOf(
                    "test/repo/foo/guid\t4\t6\t{}",
                    "test/repo/foo/guid/v0\t5\t10\t{\"path\":\"/var/a\"}",
                    "test/repo/foo/guid/v1\t8\t16\t{\"path\":\"/var/b\"}"
            ).joinToString("\n")
            val status = provider.getRepositoryStatus("foo")
            status.sourceCommit shouldBe "sourcehash"
            status.lastCommit shouldBe "hash"
            status.logicalSize shouldBe 40L
            status.actualSize shouldBe 20L
            status.volumeStatus.size shouldBe 2
            status.volumeStatus[0].name shouldBe "v0"
            status.volumeStatus[0].logicalSize shouldBe 5
            status.volumeStatus[0].actualSize shouldBe 10
            status.volumeStatus[0].properties["path"] shouldBe "/var/a"
            status.volumeStatus[1].name shouldBe "v1"
            status.volumeStatus[1].logicalSize shouldBe 8
            status.volumeStatus[1].actualSize shouldBe 16
            status.volumeStatus[1].properties["path"] shouldBe "/var/b"
        }

        "source commit tracks latest snapshot" {
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy,io.titan-data:metadata", "-t", "snapshot",
                    "-d", "2", "test/repo/foo") } returns "test/repo/foo/guid@hash\toff\t{}\n"
            every { executor.exec("zfs", "list", "-Ho", "io.titan-data:metadata",
                    "test/repo/foo/guid@hash") } returns "{\"a\":\"b\"}\n"
            every { executor.exec("zfs", "list", "-Hpo", "io.titan-data:active",
                    "test/repo/foo") } returns "guid"
            every { executor.exec("zfs", "list", "-pHo", "logicalused,used", "test/repo/foo/guid") } returns "40\t20\n"
            every { executor.exec("zfs", "list", "-Ho", "origin", "-r", "test/repo/foo/guid") } returns
                    "test/repo/foo/guidtwo/v0@sourcehash\n"
            every { executor.exec("zfs", "list", "-pHo", "name,creation", "-t", "snapshot", "-d", "1",
                    "test/repo/foo/guid") } returns
                    "test/repo/foo/guid@one\t1\ntest/repo/foo/guid@two\t2\n"
            every { executor.exec("zfs", "list", "-d", "1",
                    "-pHo", "name,logicalreferenced,referenced,io.titan-data:metadata", "test/repo/foo/guid") } returns arrayOf(
                    "test/repo/foo/guid\t4\t6\t{}",
                    "test/repo/foo/guid/v0\t5\t10\t{\"path\":\"/var/a\"}",
                    "test/repo/foo/guid/v1\t8\t16\t{\"path\":\"/var/b\"}"
            ).joinToString("\n")
            val status = provider.getRepositoryStatus("foo")
            status.sourceCommit shouldBe "two"
        }

        "delete repository fails with invalid name" {
            shouldThrow<IllegalArgumentException> {
                provider.deleteRepository("not/a/name")
            }
        }

        "delete repository succeeds" {
            every { executor.exec(*anyVararg()) } returns ""
            provider.deleteRepository("foo")
            verify {
                executor.exec("zfs", "destroy", "-R", "test/repo/foo")
            }
            confirmVerified()
        }

        "delete non-existent repository throws no such object" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "does not exist")
            shouldThrow<NoSuchObjectException> {
                provider.deleteRepository("foo")
            }
        }

        "create repository fails with invalid name" {
            val repo = Repository(name = "foo/bar", properties = mapOf("a" to "b"))
            shouldThrow<IllegalArgumentException> {
                provider.createRepository(repo)
            }
        }

        "create repository succeeds" {
            every { generator.get() } returns "guid"
            every { executor.exec(*anyVararg()) } returns ""
            val repo = Repository(name = "foo", properties = mapOf("a" to "b"))
            provider.createRepository(repo)

            verifySequence {
                executor.exec("zfs", "create", "-o", "mountpoint=legacy", "-o",
                        "io.titan-data:active=guid", "-o",
                        "io.titan-data:metadata={\"a\":\"b\"}", "test/repo/foo")
                executor.exec("zfs", "create", "test/repo/foo/guid")
                executor.exec("zfs", "snapshot", "-o",
                        "io.titan-data:metadata={}", "test/repo/foo/guid@initial")
            }
            confirmVerified()
        }

        "create repository fails with object exists exception" {
            val repo = Repository(name = "foo", properties = mapOf("a" to "b"))
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "already exists")
            every { generator.get() } returns "guid"
            shouldThrow<ObjectExistsException> {
                provider.createRepository(repo)
            }
        }

        "get active dataset returns correct dataset" {
            every { executor.exec(*anyVararg()) } returns "guid"
            val dataset = provider.getActive("foo")
            dataset shouldBe "guid"
        }

        "get active dataset throws error for no such dataset" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "does not exist")
            shouldThrow<NoSuchObjectException> {
                provider.getActive("foo")
            }
        }
    }
}
