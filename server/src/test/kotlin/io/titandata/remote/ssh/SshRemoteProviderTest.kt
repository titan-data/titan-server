/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.ssh

import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.matchers.string.shouldContain
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
import io.mockk.impl.annotations.SpyK
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import io.titandata.ProviderModule
import io.titandata.exception.CommandException
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Volume
import io.titandata.operation.OperationExecutor
import io.titandata.storage.zfs.ZfsStorageProvider
import io.titandata.util.CommandExecutor
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.InputStream

class SshRemoteProviderTest : StringSpec() {

    @SpyK
    var executor: CommandExecutor = CommandExecutor()

    @MockK
    lateinit var zfsStorageProvider: ZfsStorageProvider

    @InjectMockKs
    @OverrideMockKs
    var providers = ProviderModule("test")

    @InjectMockKs
    @OverrideMockKs
    lateinit var sshRemoteProvider: SshRemoteProvider

    override fun beforeTest(testCase: TestCase) {
        sshRemoteProvider = SshRemoteProvider(providers)
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    fun getResource(name: String): InputStream {
        return this.javaClass.getResource(name).openStream()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    fun getRemote(): SshRemote {
        return SshRemote(
                name = "remote",
                address = "localhost",
                username = "root",
                password = "root",
                path = "/var/tmp"
        )
    }

    init {
        "list commits returns an empty list" {
            every { executor.exec(*anyVararg()) } returns ""
            val result = sshRemoteProvider.listCommits(getRemote(), SshParameters(), null)
            result.size shouldBe 0
        }

        "list commits returns correct metadata" {
            every { executor.exec("sshpass", "-f", any(), "ssh", "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null", "root@localhost", "ls", "-1", "/var/tmp") } returns "a\nb\n"
            every { executor.exec("sshpass", "-f", any(), "ssh", "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null", "root@localhost", "cat", "/var/tmp/a/metadata.json") } returns "{\"id\":\"a\",\"properties\":{}}"
            every { executor.exec("sshpass", "-f", any(), "ssh", "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null", "root@localhost", "cat", "/var/tmp/b/metadata.json") } returns "{\"id\":\"b\",\"properties\":{}}"
            val result = sshRemoteProvider.listCommits(getRemote(), SshParameters(), null)
            result.size shouldBe 2
            result[0].id shouldBe "a"
            result[1].id shouldBe "b"
        }

        "list commits filters result" {
            every { executor.exec("sshpass", "-f", any(), "ssh", "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null", "root@localhost", "ls", "-1", "/var/tmp") } returns "a\nb\n"
            every { executor.exec("sshpass", "-f", any(), "ssh", "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null", "root@localhost", "cat", "/var/tmp/a/metadata.json") } returns "{\"id\":\"a\",\"properties\":{\"tags\":{\"c\":\"d\"}}}"
            every { executor.exec("sshpass", "-f", any(), "ssh", "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null", "root@localhost", "cat", "/var/tmp/b/metadata.json") } returns "{\"id\":\"b\",\"properties\":{}}"
            val result = sshRemoteProvider.listCommits(getRemote(), SshParameters(), listOf("c"))
            result.size shouldBe 1
            result[0].id shouldBe "a"
        }

        "list commits ignores missing file" {
            every { executor.exec("sshpass", "-f", any(), "ssh", "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null", "root@localhost", "ls", "-1", "/var/tmp") } returns "a\n"
            every { executor.exec("sshpass", "-f", any(), "ssh", "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null", "root@localhost", "cat", "/var/tmp/a/metadata.json") } throws CommandException("", 1,
                    "No such file or directory")

            val result = sshRemoteProvider.listCommits(getRemote(), SshParameters(), null)
            result.size shouldBe 0
        }

        "get commit returns failure if file doesn't exist" {
            every { executor.exec("sshpass", "-f", any(), "ssh", "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null", "root@localhost", "cat", "/var/tmp/a/metadata.json") } throws CommandException("", 1,
                    "No such file or directory")
            shouldThrow<NoSuchObjectException> {
                sshRemoteProvider.getCommit(getRemote(), "a", SshParameters())
            }
        }

        "get commit returns correct metadata" {
            every { executor.exec("sshpass", "-f", any(), "ssh", "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null", "root@localhost", "cat", "/var/tmp/a/metadata.json") } returns "{\"id\":\"a\",\"properties\":{\"b\":\"c\"}}"
            val commit = sshRemoteProvider.getCommit(getRemote(), "a", SshParameters())
            commit.id shouldBe "a"
            commit.properties["b"] shouldBe "c"
        }

        "temporary password file is correctly removed" {
            val slot = slot<String>()
            every { executor.exec("sshpass", "-f", capture(slot), "ssh", "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null", "root@localhost", "cat", "/var/tmp/a/metadata.json") } returns "{\"id\":\"a\",\"properties\":{\"b\":\"c\"}}"
            sshRemoteProvider.getCommit(getRemote(), "a", SshParameters())
            val file = File(slot.captured)
            file.exists() shouldBe false
        }

        "validate pull operation succeeds if remote commit exists" {
            every { executor.exec("sshpass", "-f", any(), "ssh", "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null", "root@localhost", "cat", "/var/tmp/a/metadata.json") } returns "{\"id\":\"a\",\"properties\":{}}"
            sshRemoteProvider.validateOperation(getRemote(), "a", Operation.Type.PULL, SshParameters(), false)
        }

        "validate pull operation fails if remote commit does not exist" {
            every {
                executor.exec("sshpass", "-f", any(), "ssh", "-o", "StrictHostKeyChecking=no",
                        "-o", "UserKnownHostsFile=/dev/null", "root@localhost", "cat", "/var/tmp/a/metadata.json")
            } throws CommandException("", 1, "No such file or directory")
            shouldThrow<NoSuchObjectException> {
                sshRemoteProvider.validateOperation(getRemote(), "a", Operation.Type.PULL, SshParameters(), false)
            }
        }

        "validate push operation succeeds if remote commit does not exists" {
            every { executor.exec("sshpass", "-f", any(), "ssh", "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null", "root@localhost", "cat", "/var/tmp/a/metadata.json") } throws CommandException("", 1, "No such file or directory")
            sshRemoteProvider.validateOperation(getRemote(), "a", Operation.Type.PUSH, SshParameters(), false)
        }

        "validate push operation fails if remote commit exists" {
            every { executor.exec("sshpass", "-f", any(), "ssh", "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null", "root@localhost", "cat", "/var/tmp/a/metadata.json") } returns "{\"id\":\"a\",\"properties\":{}}"
            shouldThrow<ObjectExistsException> {
                sshRemoteProvider.validateOperation(getRemote(), "a", Operation.Type.PUSH, SshParameters(), false)
            }
        }

        "validate metadata only push operation succeeds if remote commit exists" {
            every { executor.exec("sshpass", "-f", any(), "ssh", "-o", "StrictHostKeyChecking=no",
                    "-o", "UserKnownHostsFile=/dev/null", "root@localhost", "cat", "/var/tmp/a/metadata.json") } returns "{\"id\":\"a\",\"properties\":{}}"
            sshRemoteProvider.validateOperation(getRemote(), "a", Operation.Type.PUSH, SshParameters(), true)
        }

        "validate metadata only push operation fails if remote commit does not exist" {
            every {
                executor.exec("sshpass", "-f", any(), "ssh", "-o", "StrictHostKeyChecking=no",
                        "-o", "UserKnownHostsFile=/dev/null", "root@localhost", "cat", "/var/tmp/a/metadata.json")
            } throws CommandException("", 1, "No such file or directory")
            shouldThrow<NoSuchObjectException> {
                sshRemoteProvider.validateOperation(getRemote(), "a", Operation.Type.PUSH, SshParameters(), true)
            }
        }

        "run operation succeeds" {
            val operation = Operation(id = "id", type = Operation.Type.PUSH,
                    remote = "remote", commitId = "commitId")
            val remote = getRemote()
            val request = SshParameters()
            val operationExecutor = OperationExecutor(providers, operation, "repo", remote, request)

            val process = mockk<Process>()
            every { process.inputStream } answers { getResource("/rsync2.out") }
            every { process.waitFor() } returns 0
            every { process.waitFor(any(), any()) } returns false
            every { process.exitValue() } returns 0
            every { process.outputStream } returns ByteArrayOutputStream()
            every { process.isAlive() } returns false
            every { process.destroy() } just Runs

            every { executor.start(*anyVararg()) } returns process

            every { zfsStorageProvider.mountOperationVolumes(any(), any()) } returns
                    "/var/operation"
            every { zfsStorageProvider.getCommit(any(), any()) } returns Commit(id = "commitId", properties = mapOf())
            every { zfsStorageProvider.listVolumes(any(), any()) } returns
                    listOf(Volume(name = "v0"), Volume(name = "v1", properties = mapOf("path" to "/volume")))
            every { zfsStorageProvider.unmountOperationVolumes(any(), any()) } just Runs
            every { zfsStorageProvider.createOperation("repo", any(), any()) } just Runs
            every { zfsStorageProvider.createOperationScratch("repo", any()) } returns ""
            every { zfsStorageProvider.destroyOperationScratch("repo", any()) } just Runs

            operationExecutor.run()

            val progress = operationExecutor.getProgress()
            progress.size shouldBe 7
            progress[0].type shouldBe ProgressEntry.Type.START
            progress[0].message shouldBe "Syncing v0"
            progress[1].type shouldBe ProgressEntry.Type.PROGRESS
            progress[2].type shouldBe ProgressEntry.Type.END
            progress[3].type shouldBe ProgressEntry.Type.START
            progress[3].message shouldBe "Syncing /volume"
            progress[4].type shouldBe ProgressEntry.Type.PROGRESS
            progress[5].type shouldBe ProgressEntry.Type.END
            progress[6].type shouldBe ProgressEntry.Type.COMPLETE
        }

        "run operation fails if rsync fails" {
            val operation = Operation(id = "id", type = Operation.Type.PUSH,
                    remote = "remote", commitId = "commitId")
            val remote = getRemote()
            val request = SshParameters()
            val operationExecutor = OperationExecutor(providers, operation, "repo", remote, request)

            val process = mockk<Process>()
            every { process.inputStream } returns ByteArrayInputStream("".toByteArray())
            every { process.errorStream } returns ByteArrayInputStream("error string".toByteArray())
            every { process.waitFor() } returns 0
            every { process.waitFor(any(), any()) } returns false
            every { process.exitValue() } returns 1
            every { process.isAlive() } returns false
            every { process.destroy() } just Runs

            every { executor.start(*anyVararg()) } returns process

            every { zfsStorageProvider.mountOperationVolumes(any(), any()) } returns
                    "/var/operation"
            every { zfsStorageProvider.getCommit(any(), any()) } returns Commit(id = "commitId", properties = mapOf())
            every { zfsStorageProvider.listVolumes(any(), any()) } returns
                    listOf(Volume(name = "v0"), Volume(name = "v1", properties = mapOf("path" to "/volume")))
            every { zfsStorageProvider.unmountOperationVolumes(any(), any()) } just Runs
            every { zfsStorageProvider.createOperation("repo", any(), any()) } just Runs
            every { zfsStorageProvider.createOperationScratch("repo", any()) } returns ""
            every { zfsStorageProvider.destroyOperationScratch("repo", any()) } just Runs

            operationExecutor.run()

            val progress = operationExecutor.getProgress()

            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.START
            progress[1].type shouldBe ProgressEntry.Type.FAILED
            progress[1].message shouldContain "error string"
        }
    }
}
