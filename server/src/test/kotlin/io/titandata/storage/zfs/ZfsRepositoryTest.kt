/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package io.titandata.storage.zfs

import io.titandata.exception.CommandException
import io.titandata.exception.InvalidStateException
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.EngineRemote
import io.titandata.models.NopRemote
import io.titandata.models.Repository
import io.titandata.util.CommandExecutor
import io.titandata.util.GuidGenerator
import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.matchers.types.shouldBeInstanceOf
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

        "empty zfs list generates empty repository list" {
            every { executor.exec(*anyVararg()) } returns ""
            val result = provider.listRepositories()
            result.size shouldBe 0
            verify {
                executor.exec("zfs", "list", "-Ho", "name,io.titan-data:metadata",
                        "-d", "1", "test/repo")
            }
            confirmVerified()
        }

        "zfs list generates list of one repository" {
            every { executor.exec(*anyVararg()) } returns
                    "test\t-\n" +
                    "test/repo/repo\t{\"a\": \"b\"}\n"
            val result = provider.listRepositories()
            result.size shouldBe 1
            result[0].name shouldBe "repo"
            result[0].properties["a"] shouldBe "b"
        }

        "zfs list generates list of two repositories" {
            every { executor.exec(*anyVararg()) } returns
                    "test\t-\n" +
                    "test/repo/repo1\t{}\n" +
                    "test/repo/repo2\t{}\n"
            val result = provider.listRepositories()
            result.size shouldBe 2
            result[0].name shouldBe "repo1"
            result[1].name shouldBe "repo2"
        }

        "get repository succeeds" {
            every { executor.exec(*anyVararg()) } returns "test/repo/repo\t{\"a\": \"b\"}"
            val result = provider.getRepository("repo")
            result.name shouldBe "repo"
            result.properties["a"] shouldBe "b"
            verify {
                executor.exec("zfs", "list", "-Ho",
                        "name,io.titan-data:metadata", "test/repo/repo")
            }
            confirmVerified()
        }

        "get repository fails with invalid name" {
            shouldThrow<IllegalArgumentException> {
                provider.getRepository("not/valid")
            }
        }

        "object not found thrown if repository doesn't exist" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "dataset does not exist")
            shouldThrow<NoSuchObjectException> {
                provider.getRepository("foo")
            }
        }

        "get repository throws command exception for other reasons" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "")
            shouldThrow<CommandException> {
                provider.getRepository("foo")
            }
        }

        "get repository fails with invalid state" {
            every { executor.exec(*anyVararg()) } returns "test/repo/foo\t-"
            shouldThrow<InvalidStateException> {
                provider.getRepository("foo")
            }
        }

        "update fails with invalid name" {
            val repo = Repository(name = "not/a/name", properties = mapOf("a" to "b"))
            shouldThrow<IllegalArgumentException> {
                provider.updateRepository("foo", repo)
            }
        }

        "metadata is converted to json on update" {
            val repo = Repository(name = "foo", properties = mapOf("a" to "b"))
            every { executor.exec(*anyVararg()) } returns ""
            provider.updateRepository("foo", repo)
            verify {
                executor.exec("zfs", "set", "io.titan-data:metadata={\"a\":\"b\"}",
                        "test/repo/foo")
            }
            confirmVerified()
        }

        "filesystems are renamed when name is updated" {
            val repo = Repository(name = "bar", properties = mapOf("a" to "b"))
            every { executor.exec(*anyVararg()) } returns ""
            provider.updateRepository("foo", repo)

            verifySequence {
                executor.exec("zfs", "set", "io.titan-data:metadata={\"a\":\"b\"}",
                        "test/repo/foo")
                executor.exec("zfs", "rename", "test/repo/foo", "test/repo/bar")
            }
            confirmVerified()
        }

        "rename to an existing repository throws an exception" {
            val repo = Repository(name = "bar", properties = mapOf("a" to "b"))
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "already exists")
            shouldThrow<ObjectExistsException> {
                provider.updateRepository("foo", repo)
            }
        }

        "no such object thrown when updating a non-existent repo" {
            val repo = Repository(name = "foo", properties = mapOf("a" to "b"))
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "does not exist")
            shouldThrow<NoSuchObjectException> {
                provider.updateRepository("foo", repo)
            }
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

        "get remotes with no property set succeeds" {
            every { executor.exec(*anyVararg()) } returns "-\n"
            val result = provider.getRemotes("repo")
            result.size shouldBe 0
        }

        "get remotes returns success" {
            every { executor.exec(*anyVararg()) } returns
                "[{\"provider\":\"nop\",\"name\":\"foo\"}," +
                        "{\"provider\":\"engine\",\"name\":\"bar\",\"address\":\"a\"," +
                        "\"username\":\"u\",\"password\":\"p\"}]"
            val result = provider.getRemotes("repo")
            result.size shouldBe 2
            result[0].shouldBeInstanceOf<NopRemote>()
            result[0].provider shouldBe "nop"
            result[0].name shouldBe "foo"
            result[1].shouldBeInstanceOf<EngineRemote>()
            result[1].provider shouldBe "engine"
            result[1].name shouldBe "bar"
            (result[1] as EngineRemote).username shouldBe "u"
            (result[1] as EngineRemote).address shouldBe "a"
            (result[1] as EngineRemote).password shouldBe "p"
        }

        "update remotes succeeds" {
            every { executor.exec(*anyVararg()) } returns ""
            provider.updateRemotes("repo", listOf(NopRemote(name = "foo")))
            verifySequence {
                executor.exec("zfs", "set",
                        "io.titan-data:remotes=[{\"provider\":\"nop\",\"name\":\"foo\"}]",
                        "test/repo/repo")
            }
            confirmVerified()
        }
    }
}
