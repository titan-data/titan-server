/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.storage.zfs

import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.matchers.string.shouldContain
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
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Commit
import io.titandata.util.CommandExecutor
import io.titandata.util.GuidGenerator

class ZfsCommitTest : StringSpec() {

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
        "create commit fails if invalid repo name specified" {
            val commit = Commit(id = "ok", properties = mapOf("a" to "b"))
            shouldThrow<IllegalArgumentException> {
                provider.createCommit("not/ok", commit)
            }
        }

        "create commit fails if invalid id used" {
            val commit = Commit(id = "not/ok", properties = mapOf("a" to "b"))
            shouldThrow<IllegalArgumentException> {
                provider.createCommit("foo", commit)
            }
        }

        "create commit fails if reserved initial id used" {
            val commit = Commit(id = "initial", properties = mapOf("a" to "b"))
            shouldThrow<IllegalArgumentException> {
                provider.createCommit("foo", commit)
            }
        }

        "create commit for unknown repository throws error" {
            val commit = Commit(id = "hash", properties = mapOf("a" to "b"))
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "does not exist")
            val exception = shouldThrow<NoSuchObjectException> {
                provider.createCommit("foo", commit)
            }
            exception.message shouldContain "repository"
        }

        "create commit succeeds" {
            val commit = Commit(id = "hash", properties = mapOf("a" to "b"))
            every { executor.exec("zfs", "list", "-Hpo", "io.titan-data:active",
                    "test/repo/foo") } returns "guid\n"
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot",
                    "-d", "2", "test/repo/foo") } returns ""
            every { executor.exec("zfs", "snapshot", "-r", "-o",
                    "io.titan-data:metadata={\"a\":\"b\"}", "test/repo/foo/guid@hash") } returns ""
            every { executor.exec("zfs", "list", "-Hpo", "creation",
                    "test/repo/foo/guid@hash") } returns "1556492646\n"
            every { executor.exec("zfs", "set",
                    "io.titan-data:metadata={\"a\":\"b\",\"timestamp\":\"2019-04-28T23:04:06Z\"}",
                    "test/repo/foo/guid@hash") } returns ""
            val result = provider.createCommit("foo", commit)
            result.properties["timestamp"] shouldBe "2019-04-28T23:04:06Z"
            result.id shouldBe "hash"
        }

        "create commit of existing hash on same dataset throws exception" {
            val commit = Commit(id = "hash", properties = mapOf("a" to "b"))
            every { executor.exec("zfs", "list", "-Hpo", "io.titan-data:active",
                    "test/repo/foo") } returns "guid\n"
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot",
                    "-d", "2", "test/repo/foo") } returns ""
            every { executor.exec("zfs", "snapshot", "-r", "-o",
                    "io.titan-data:metadata={\"a\":\"b\"}", "test/repo/foo/guid@hash") } throws
                    CommandException("", 1, "snapshot already exists")
            val exception = shouldThrow<ObjectExistsException> {
                provider.createCommit("foo", commit)
            }
            exception.message shouldContain "commit"
        }

        "create commit of existing hash on different dataset throws exception" {
            val commit = Commit(id = "hash", properties = mapOf("a" to "b"))
            every { executor.exec("zfs", "list", "-Hpo", "io.titan-data:active",
                    "test/repo/foo") } returns "guid\n"
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot",
                    "-d", "2", "test/repo/foo") } returns "test/repo/foo/guid2@hash\toff"
            val exception = shouldThrow<ObjectExistsException> {
                provider.createCommit("foo", commit)
            }
            exception.message shouldContain "commit"
        }

        "get commit guid returns correct result" {
            every { executor.exec(*anyVararg()) } returns
                    arrayOf("test/repo/foo@hash1\toff",
                    "test/repo/foo/two@hash2\toff",
                    "test/repo/bar/one@hash1\toff",
                    "test/repo/foo/one@hash1\toff").joinToString("\n")
            val guid = provider.getCommitGuid("foo", "hash1")
            guid shouldBe "one"
        }

        "get commit guid throws exception for non-existent repo" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "does not exist")
            shouldThrow<NoSuchObjectException> {
                provider.getCommitGuid("foo", "hash1")
            }
        }

        "get commit guid returns null if no hash found" {
            every { executor.exec(*anyVararg()) } returns
                    arrayOf("test/repo/foo@hash1\toff",
                            "test/repo/foo/two@hash2\toff",
                            "test/repo/bar/one@hash1\toff",
                            "test/repo/baz/one@hash1\toff").joinToString("\n")
            val guid = provider.getCommitGuid("foo", "hash1")
            guid shouldBe null
        }

        "get commit guid fails with invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                provider.getCommitGuid("not/ok", "ok")
            }
        }

        "get commit guid fails with invalid commit name" {
            shouldThrow<IllegalArgumentException> {
                provider.getCommitGuid("ok", "not/ok")
            }
        }

        "get commit guid fails with reserved commit name" {
            shouldThrow<IllegalArgumentException> {
                provider.getCommitGuid("ok", "initial")
            }
        }

        "get commit succeeds" {
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot",
                    "-d", "2", "test/repo/foo") } returns "test/repo/foo/guid@hash\toff\n"
            every { executor.exec("zfs", "list", "-Ho", "io.titan-data:metadata",
                    "test/repo/foo/guid@hash") } returns "{\"a\":\"b\"}\n"
            val commit = provider.getCommit("foo", "hash")
            commit.id shouldBe "hash"
            commit.properties["a"] shouldBe "b"
        }

        "get commit fails if no commit found" {
            every { executor.exec(*anyVararg()) } returns "test/repo/foo/guid@hash2\toff\n"
            shouldThrow<NoSuchObjectException> {
                provider.getCommit("foo", "hash")
            }
        }

        "get commit fails if commit has defer_destroy set" {
            every {
                executor.exec(*anyVararg())
            } returns "test/repo/foo/guid@hash\ton"
            shouldThrow<NoSuchObjectException> {
                provider.getCommit("foo", "hash")
            }
        }

        "get commit status suceeds" {
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot",
                    "-d", "2", "test/repo/foo") } returns "test/repo/foo/guid@hash\toff\n"
            every { executor.exec("zfs", "list", "-Ho", "io.titan-data:metadata",
                    "test/repo/foo/guid@hash") } returns "{\"a\":\"b\"}\n"
            every { executor.exec("zfs", "list", "-Hpo", "name,logicalreferenced,referenced,used", "-t",
                    "snapshot", "-r", "test/repo/foo/guid") } returns arrayOf(
                    "test/repo/foo/guid@hash\t1\t1\t1",
                    "test/repo/foo/guid/v0@hash\t1\t2\t3",
                    "test/repo/foo/guid/v0@otherhash\t2\t2\t2",
                    "test/repo/foo/guid/v1@hash\t2\t4\t6"
            ).joinToString("\n")

            val status = provider.getCommitStatus("foo", "hash")
            status.logicalSize shouldBe 3L
            status.actualSize shouldBe 6L
            status.uniqueSize shouldBe 9L
        }

        "commit info with tabs is parsed correctly" {
            val result = provider.commitManager.parseCommit("test/repo/repo/guid@hash\toff\t{\"a\": \"b\tc\"}\n")
            result shouldNotBe null
            result!!.properties["a"] shouldBe "b\tc"
        }

        "list commits fails with invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                provider.listCommits("not/ok")
            }
        }

        "list commits returns empty list with no output" {
            every { executor.exec(*anyVararg()) } returns ""
            val result = provider.listCommits("foo")
            result.size shouldBe 0
            verify {
                executor.exec("zfs", "list", "-Ho",
                        "name,defer_destroy,io.titan-data:metadata", "-t", "snapshot", "-d",
                        "2", "test/repo/foo")
            }
            confirmVerified()
        }

        "list commits succeeds" {
            every { executor.exec(*anyVararg()) } returns arrayOf(
                    "test/repo/foo@ignore\toff\t{}",
                    "test/repo/foo/guid1@hash1\toff\t{\"a\":\"b\"}",
                    "test/repo/foo/guid2@hash2\toff\t{\"c\":\"d\"}"
            ).joinToString("\n")
            val result = provider.listCommits("foo")
            result.size shouldBe 2
            result[0].id shouldBe "hash1"
            result[0].properties["a"] shouldBe "b"
            result[1].id shouldBe "hash2"
            result[1].properties["c"] shouldBe "d"
        }

        "list commits ignores initial commit" {
            every { executor.exec(*anyVararg()) } returns arrayOf(
                    "test/repo/foo@ignore\toff\t{}",
                    "test/repo/foo/guid1@initial\toff\t{\"a\":\"b\"}",
                    "test/repo/foo/guid2@hash2\toff\t{\"c\":\"d\"}"
            ).joinToString("\n")
            val result = provider.listCommits("foo")
            result.size shouldBe 1
            result[0].id shouldBe "hash2"
            result[0].properties["c"] shouldBe "d"
        }

        "list commits ignores snapshots with defer_destory set" {
            every { executor.exec(*anyVararg()) } returns arrayOf(
                    "test/repo/foo@ignore\toff\t{}",
                    "test/repo/foo/guid1@hash1\ton\t{\"a\":\"b\"}",
                    "test/repo/foo/guid2@hash2\toff\t{\"c\":\"d\"}"
            ).joinToString("\n")
            val result = provider.listCommits("foo")
            result.size shouldBe 1
            result[0].id shouldBe "hash2"
            result[0].properties["c"] shouldBe "d"
        }

        "list commits throws exception for non-existent repo" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "does not exist")
            shouldThrow<NoSuchObjectException> {
                provider.listCommits("foo")
            }
        }

        "delete commit fails with invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                provider.deleteCommit("not/ok", "ok")
            }
        }

        "delete commit fails with invalid commit name" {
            shouldThrow<IllegalArgumentException> {
                provider.deleteCommit("ok", "not/ok")
            }
        }

        "delete commit fails if commit not found" {
            every {
                executor.exec(*anyVararg())
            } returns "test/repo/foo/guid@hash2"
            shouldThrow<NoSuchObjectException> {
                provider.deleteCommit("foo", "hash")
            }
        }

        "delete commit on active empty guid doesn't destroy dataset" {
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot",
                        "-d", "2", "test/repo/foo")
            } returns "test/repo/foo/guid@hash\toff\n"
            every { executor.exec("zfs", "destroy", "-rd", "test/repo/foo/guid@hash") } returns ""
            every { executor.exec("zfs", "list", "-Hpo", "io.titan-data:active",
                    "test/repo/foo") } returns "guid"
            provider.deleteCommit("foo", "hash")
            verifySequence {
                executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot",
                        "-d", "2", "test/repo/foo")
                executor.exec("zfs", "destroy", "-rd", "test/repo/foo/guid@hash")
                executor.exec("zfs", "list", "-Hpo", "io.titan-data:active",
                        "test/repo/foo")
            }
            confirmVerified()
        }

        "delete commit on inactive but non-empty guid doesn't destroy dataset" {
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot",
                    "-d", "2", "test/repo/foo")
            } returns "test/repo/foo/guid@hash\toff"
            every { executor.exec("zfs", "destroy", "-rd", "test/repo/foo/guid@hash") } returns ""
            every { executor.exec("zfs", "list", "-Hpo", "io.titan-data:active",
                    "test/repo/foo") } returns "guid2"
            every { executor.exec("zfs", "list", "-H", "-t", "snapshot", "-d", "1",
                    "test/repo/foo/guid") } returns "blah"
            provider.deleteCommit("foo", "hash")
            verifySequence {
                executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot",
                        "-d", "2", "test/repo/foo")
                executor.exec("zfs", "destroy", "-rd", "test/repo/foo/guid@hash")
                executor.exec("zfs", "list", "-Hpo", "io.titan-data:active",
                        "test/repo/foo")
                executor.exec("zfs", "list", "-H", "-t", "snapshot", "-d", "1",
                        "test/repo/foo/guid")
            }
            confirmVerified()
        }

        "delete last commit on inactive guid destroy's dataset" {
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot",
                    "-d", "2", "test/repo/foo")
            } returns "test/repo/foo/guid@hash\toff"
            every { executor.exec("zfs", "destroy", "-rd", "test/repo/foo/guid@hash") } returns ""
            every { executor.exec("zfs", "list", "-Hpo", "io.titan-data:active",
                    "test/repo/foo") } returns "guid2"
            every { executor.exec("zfs", "list", "-H", "-t", "snapshot", "-d", "1",
                    "test/repo/foo/guid") } returns ""
            every { executor.exec("zfs", "destroy", "-r", "test/repo/foo/guid") } returns ""
            provider.deleteCommit("foo", "hash")
            verifySequence {
                executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot",
                        "-d", "2", "test/repo/foo")
                executor.exec("zfs", "destroy", "-rd", "test/repo/foo/guid@hash")
                executor.exec("zfs", "list", "-Hpo", "io.titan-data:active",
                        "test/repo/foo")
                executor.exec("zfs", "list", "-H", "-t", "snapshot", "-d", "1",
                        "test/repo/foo/guid")
                executor.exec("zfs", "destroy", "-r", "test/repo/foo/guid")
            }
            confirmVerified()
        }

        "checkout with invalid repo name fails" {
            shouldThrow<IllegalArgumentException> {
                provider.checkoutCommit("not/ok", "ok")
            }
        }

        "checkout with invalid commit name fails" {
            shouldThrow<IllegalArgumentException> {
                provider.checkoutCommit("ok", "not/ok")
            }
        }

        "checkout of non-existent commit fails" {
            every { executor.exec(*anyVararg()) } returns ""
            shouldThrow<NoSuchObjectException> {
                provider.checkoutCommit("foo", "hash")
            }
        }

        "checkout sets active dataset correctly" {
            every { generator.get() } returns "newguid"
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot",
                    "-d", "2", "test/repo/foo")
            } returns "test/repo/foo/guid@hash\toff"
            every { executor.exec("zfs", "list", "-rHo", "name,io.titan-data:metadata", "test/repo/foo/guid") } returns
                    arrayOf("test/repo/foo/guid\t-", "test/repo/foo/guid/v0\t{}", "test/repo/foo/guid/v1\t{}").joinToString("\n")
            every { executor.exec("zfs", "create", "test/repo/foo/newguid") } returns ""
            every { executor.exec("zfs", "clone", "-o", "io.titan-data:metadata={}", "test/repo/foo/guid/v0@hash",
                    "test/repo/foo/newguid/v0") } returns ""
            every { executor.exec("zfs", "clone", "-o", "io.titan-data:metadata={}", "test/repo/foo/guid/v1@hash",
                    "test/repo/foo/newguid/v1") } returns ""
            every { executor.exec("zfs", "set", "io.titan-data:active=newguid",
                    "test/repo/foo") } returns ""
            provider.checkoutCommit("foo", "hash")

            verifySequence {
                executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot",
                        "-d", "2", "test/repo/foo")
                executor.exec("zfs", "list", "-rHo", "name,io.titan-data:metadata", "test/repo/foo/guid")
                executor.exec("zfs", "create", "test/repo/foo/newguid")
                executor.exec("zfs", "clone", "-o", "io.titan-data:metadata={}", "test/repo/foo/guid/v0@hash", "test/repo/foo/newguid/v0")
                executor.exec("zfs", "clone", "-o", "io.titan-data:metadata={}", "test/repo/foo/guid/v1@hash", "test/repo/foo/newguid/v1")
                executor.exec("zfs", "set", "io.titan-data:active=newguid", "test/repo/foo")
            }
            confirmVerified()
        }
    }
}
