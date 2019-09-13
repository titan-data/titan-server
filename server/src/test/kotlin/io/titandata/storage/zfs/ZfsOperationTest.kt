/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package io.titandata.storage.zfs

import io.titandata.exception.CommandException
import io.titandata.exception.NoSuchObjectException
import io.titandata.models.Commit
import io.titandata.models.NopParameters
import io.titandata.models.Operation
import io.titandata.storage.OperationData
import io.titandata.util.CommandExecutor
import io.titandata.util.GuidGenerator
import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.matchers.string.shouldContain
import io.kotlintest.shouldBe
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

class ZfsOperationTest : StringSpec() {

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

    fun getOperation(): OperationData {
        val operation = Operation(id = "id", type = Operation.Type.PUSH,
                state = Operation.State.RUNNING, remote = "remote", commitId = "commit")
        return OperationData(operation = operation, params = NopParameters())
    }

    fun mockOperation() {
        val json = "{\"operation\":{\"id\":\"id\"," +
                "\"type\":\"PUSH\",\"state\":\"RUNNING\",\"remote\":\"remote\"," +
                "\"commitId\":\"commit\"},\"params\":{\"provider\":\"nop\"}}"
        every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:metadata",
                "test/repo/foo") } returns "test/repo/foo\t{}"
        every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:operation",
                "test/repo/foo/id") } returns "test/repo/foo/id\t$json"
    }

    init {
        "create operation fails if invalid repo name specified" {
            shouldThrow<IllegalArgumentException> {
                provider.createOperation("not/ok", getOperation())
            }
        }

        "create operation fails if invalid local commit used" {
            val operation = Operation(id = "id", type = Operation.Type.PUSH,
                    state = Operation.State.RUNNING, remote = "remote", commitId = "commit")
            val data = OperationData(operation = operation, params = NopParameters())
            shouldThrow<IllegalArgumentException> {
                provider.createOperation("repo", data, "not/ok")
            }
        }

        "create operation fails if invalid operation commit used" {
            val operation = Operation(id = "id", type = Operation.Type.PUSH,
                    state = Operation.State.RUNNING, remote = "remote", commitId = "not/ok")
            val data = OperationData(operation = operation, params = NopParameters())
            shouldThrow<IllegalArgumentException> {
                provider.createOperation("repo", data)
            }
        }

        "create operation fails if invalid operation id used" {
            val operation = Operation(id = "not/ok", type = Operation.Type.PUSH,
                    state = Operation.State.RUNNING, remote = "remote", commitId = "commit")
            shouldThrow<IllegalArgumentException> {
                provider.createOperation("repo",
                        OperationData(operation = operation, params = NopParameters()))
            }
        }

        "create operation fails for unknown repository" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "does not exist")
            val exception = shouldThrow<NoSuchObjectException> {
                provider.createOperation("foo", getOperation(), null)
            }
            exception.message shouldContain "repository"
        }

        "create operation succeeds" {
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot",
                    "-d", "2", "test/repo/foo")
            } returns "test/repo/foo/guid@hash\toff"
            every { executor.exec("zfs", "list", "-rHo", "name,io.titan-data:metadata", "test/repo/foo/guid") } returns
                    arrayOf("test/repo/foo/guid\t-", "test/repo/foo/guid/v0\t{}", "test/repo/foo/guid/v1\t{}").joinToString("\n")
            every { executor.exec("zfs", "create", "test/repo/foo/id") } returns ""
            every { executor.exec("zfs", "clone", "-o", "io.titan-data:metadata={}", "test/repo/foo/guid/v0@hash",
                    "test/repo/foo/id/v0") } returns ""
            every { executor.exec("zfs", "clone", "-o", "io.titan-data:metadata={}", "test/repo/foo/guid/v1@hash",
                    "test/repo/foo/id/v1") } returns ""
            val op = getOperation()
            val json = "{\"operation\":{\"id\":\"id\"," +
                    "\"type\":\"PUSH\",\"state\":\"RUNNING\",\"remote\":\"remote\"," +
                    "\"commitId\":\"commit\"},\"params\":{\"provider\":\"nop\",\"delay\":0}}"
            every { executor.exec("zfs", "set", "io.titan-data:operation=$json", "test/repo/foo/id") } returns ""
            provider.createOperation("foo", op, "hash")

            verifySequence {
                executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot",
                        "-d", "2", "test/repo/foo")
                executor.exec("zfs", "list", "-rHo", "name,io.titan-data:metadata", "test/repo/foo/guid")
                executor.exec("zfs", "create", "test/repo/foo/id")
                executor.exec("zfs", "clone", "-o", "io.titan-data:metadata={}", "test/repo/foo/guid/v0@hash", "test/repo/foo/id/v0")
                executor.exec("zfs", "clone", "-o", "io.titan-data:metadata={}", "test/repo/foo/guid/v1@hash", "test/repo/foo/id/v1")
                executor.exec("zfs", "set", "io.titan-data:operation=$json", "test/repo/foo/id")
            }
            confirmVerified()
        }

        "list operations returns an empty list" {
            every { executor.exec(*anyVararg()) } returns ""
            val result = provider.listOperations("foo")
            result.size shouldBe 0
        }

        "list operations throws exception for no such repository" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "does not exist")
            shouldThrow<NoSuchObjectException> {
                provider.listOperations("foo")
            }
        }

        "list operations returns correct result" {
            val op1 = OperationData(operation = Operation(id = "id1",
                    type = Operation.Type.PUSH,
                    state = Operation.State.RUNNING, remote = "remote", commitId = "commit1"),
                    params = NopParameters())
            val json1 = provider.gson.toJson(op1)
            val op2 = OperationData(operation = Operation(id = "id2",
                    type = Operation.Type.PULL,
                    state = Operation.State.RUNNING, remote = "remote", commitId = "commit2"),
                    params = NopParameters())
            val json2 = provider.gson.toJson(op2)
            every { executor.exec(*anyVararg()) } returns arrayOf(
                    "test/repo/foo\t-",
                    "test/repo/foo/nottop\t-",
                    "test/repo/foo/id1\t$json1",
                    "test/repo/foo/id2\t$json2"
                    ).joinToString("\n")

            val result = provider.listOperations("foo")
            result.size shouldBe 2
            result[0].operation.id shouldBe op1.operation.id
            result[0].operation.commitId shouldBe op1.operation.commitId
            result[1].operation.id shouldBe op2.operation.id
            result[1].operation.commitId shouldBe op2.operation.commitId
        }

        "get operation for non existent repo fails" {
            every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:metadata",
                    "test/repo/foo") } throws CommandException("", 1, "does not exist")
            val exception = shouldThrow<NoSuchObjectException> {
                provider.getOperation("foo", "op")
            }
            exception.message shouldContain "repository"
        }

        "get operation for non existent operation fails" {
            every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:metadata",
                    "test/repo/foo") } returns "test/repo/foo\t{}"
            every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:operation",
                    "test/repo/foo/op") } throws CommandException("", 1, "does not exist")
            val exception = shouldThrow<NoSuchObjectException> {
                provider.getOperation("foo", "op")
            }
            exception.message shouldContain "operation"
        }

        "get operation for a normal guid fails" {
            every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:metadata",
                    "test/repo/foo") } returns "test/repo/foo\t{}"
            every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:operation",
                    "test/repo/foo/op") } returns "test/repo/foo/op\t-"
            val exception = shouldThrow<NoSuchObjectException> {
                provider.getOperation("foo", "op")
            }
            exception.message shouldContain "operation"
        }

        "get operation succeeds" {
            mockOperation()
            val op = provider.getOperation("foo", "id")
            op.operation.id shouldBe "id"
            op.operation.commitId shouldBe "commit"
        }

        "commit operation fails for unknown repo" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "does not exist")
            val exception = shouldThrow<NoSuchObjectException> {
                val commit = Commit(id = "commit", properties = mapOf("a" to "b"))
                provider.commitOperation("foo", "id", commit)
            }
            exception.message shouldContain "repository"
        }

        "commit operation succeeds" {
            mockOperation()
            every { executor.exec("zfs", "snapshot", "-r", "-o",
                    "io.titan-data:metadata={\"a\":\"b\"}", "test/repo/foo/id@commit") } returns ""
            every { executor.exec("zfs", "inherit", "io.titan-data:operation",
                    "test/repo/foo/id") } returns ""

            val commit = Commit(id = "commit", properties = mapOf("a" to "b"))
            provider.commitOperation("foo", "id", commit)

            verify {
                executor.exec("zfs", "snapshot", "-r", "-o",
                        "io.titan-data:metadata={\"a\":\"b\"}", "test/repo/foo/id@commit")
                executor.exec("zfs", "inherit", "io.titan-data:operation",
                        "test/repo/foo/id")
            }
        }

        "discard operation fails for unknown repo" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "does not exist")
            val exception = shouldThrow<NoSuchObjectException> {
                provider.discardOperation("foo", "id")
            }
            exception.message shouldContain "repository"
        }

        "discard operation succeeds" {
            mockOperation()
            every { executor.exec("zfs", "destroy", "-r", "test/repo/foo/id") } returns ""

            provider.discardOperation("foo", "id")

            verify {
                executor.exec("zfs", "destroy", "-r", "test/repo/foo/id")
            }
        }

        "update operation state fails for unknown repo" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "does not exist")
            val exception = shouldThrow<NoSuchObjectException> {
                provider.updateOperationState("foo", "id", Operation.State.COMPLETE)
            }
            exception.message shouldContain "repository"
        }

        "update operation state succeeds" {
            mockOperation()
            val newJson = "{\"operation\":{\"id\":\"id\"," +
                    "\"type\":\"PUSH\",\"state\":\"COMPLETE\",\"remote\":\"remote\"," +
                    "\"commitId\":\"commit\"},\"params\":{\"provider\":\"nop\",\"delay\":0}}"
            every { executor.exec("zfs", "set", "io.titan-data:operation=$newJson",
                    "test/repo/foo/id") } returns ""

            provider.updateOperationState("foo", "id", Operation.State.COMPLETE)

            verify {
                executor.exec("zfs", "set", "io.titan-data:operation=$newJson",
                        "test/repo/foo/id")
            }
        }

        "mount operation volumes fail for unknown repo" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "does not exist")
            val exception = shouldThrow<NoSuchObjectException> {
                provider.mountOperationVolumes("foo", "id")
            }
            exception.message shouldContain "repository"
        }

        "mount operation volumes succeeds" {
            mockOperation()
            every { executor.exec("zfs", "list", "-Hpo", "io.titan-data:active",
                    "test/repo/foo") } returns "guid"
            every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:metadata",
                    "-r", "test/repo/foo/guid") } returns arrayOf(
                    "test/repo/foo/guid/one\t{\"a\":\"b\"}",
                    "test/repo/foo/guid/two\t{\"c\":\"d\"}"
            ).joinToString("\n")
            every { executor.exec("mkdir", "-p", "/var/lib/test/mnt/id/one") } returns ""
            every { executor.exec("mount", "-t", "zfs",
                    "test/repo/foo/id/one", "/var/lib/test/mnt/id/one") } returns ""
            every { executor.exec("mkdir", "-p", "/var/lib/test/mnt/id/two") } returns ""
            every { executor.exec("mount", "-t", "zfs",
                    "test/repo/foo/id/two", "/var/lib/test/mnt/id/two") } returns ""
            val result = provider.mountOperationVolumes("foo", "id")
            result shouldBe "/var/lib/test/mnt/id"

            verify {
                executor.exec("mount", "-t", "zfs",
                        "test/repo/foo/id/one", "/var/lib/test/mnt/id/one")
                executor.exec("mount", "-t", "zfs",
                        "test/repo/foo/id/two", "/var/lib/test/mnt/id/two")
            }
        }

        "unmount operation volumes fail for unknown repo" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "does not exist")
            val exception = shouldThrow<NoSuchObjectException> {
                provider.unmountOperationVolumes("foo", "id")
            }
            exception.message shouldContain "repository"
        }

        "unmount operation volumes succeeds" {
            mockOperation()
            every { executor.exec("zfs", "list", "-Hpo", "io.titan-data:active",
                    "test/repo/foo") } returns "guid"
            every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:metadata",
                    "-r", "test/repo/foo/guid") } returns arrayOf(
                    "test/repo/foo/guid/one\t{\"a\":\"b\"}",
                    "test/repo/foo/guid/two\t{\"c\":\"d\"}"
            ).joinToString("\n")
            every { executor.exec("umount", "/var/lib/test/mnt/id/one") } returns ""
            every { executor.exec("umount", "/var/lib/test/mnt/id/two") } returns ""
            provider.unmountOperationVolumes("foo", "id")

            verify {
                executor.exec("umount", "/var/lib/test/mnt/id/one")
                executor.exec("umount", "/var/lib/test/mnt/id/two")
            }
        }

        "create scratch space creates and mounts directory" {
            mockOperation()
            every { executor.exec("zfs", "create", "test/repo/foo/id/_scratch") } returns ""
            every { executor.exec("mkdir", "-p", "/var/lib/test/mnt/id/_scratch") } returns ""
            every { executor.exec("mount", "-t", "zfs", "test/repo/foo/id/_scratch", "/var/lib/test/mnt/id/_scratch") } returns ""
            provider.createOperationScratch("foo", "id")

            verify {
                executor.exec("zfs", "create", "test/repo/foo/id/_scratch")
                executor.exec("mkdir", "-p", "/var/lib/test/mnt/id/_scratch")
                executor.exec("mount", "-t", "zfs", "test/repo/foo/id/_scratch", "/var/lib/test/mnt/id/_scratch")
            }
        }

        "destroy scratch space unmounts and destroys dataset" {
            mockOperation()
            every { executor.exec("umount", "/var/lib/test/mnt/id/_scratch") } returns ""
            every { executor.exec("zfs", "destroy", "test/repo/foo/id/_scratch") } returns ""
            provider.destroyOperationScratch("foo", "id")

            verify {
                executor.exec("umount", "/var/lib/test/mnt/id/_scratch")
                executor.exec("zfs", "destroy", "test/repo/foo/id/_scratch")
            }
        }
    }
}
