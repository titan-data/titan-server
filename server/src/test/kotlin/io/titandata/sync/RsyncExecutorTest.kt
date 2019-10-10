/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.sync

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
import io.mockk.impl.annotations.OverrideMockKs
import io.mockk.impl.annotations.SpyK
import io.mockk.just
import io.mockk.mockk
import io.titandata.ProviderModule
import io.titandata.exception.CommandException
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.operation.OperationExecutor
import io.titandata.remote.ssh.SshParameters
import io.titandata.remote.ssh.SshRemote
import io.titandata.util.CommandExecutor
import java.io.ByteArrayInputStream
import java.io.InputStream

class RsyncExecutorTest : StringSpec() {

    @SpyK
    var executor: CommandExecutor = CommandExecutor()

    @InjectMockKs
    @OverrideMockKs
    var providers = ProviderModule("test")

    lateinit var operationExecutor: OperationExecutor

    override fun beforeTest(testCase: TestCase) {
        val operation = Operation(id = "id", type = Operation.Type.PUSH,
                remote = "remote", commitId = "commitId")
        val remote = SshRemote(name = "remote", address = "host", username = "root",
                password = "root", path = "/path")
        val request = SshParameters()
        operationExecutor = OperationExecutor(providers, operation, "repo", remote, request)
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    fun getResource(name: String): InputStream {
        return this.javaClass.getResource(name).openStream()
    }

    fun getRsync(): RsyncExecutor {
        return RsyncExecutor(operationExecutor, null, "password", null, "src", "dst",
                providers.commandExecutor)
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    init {
        "rsync progress is processed correctly" {
            val rsync = getRsync()
            val stream = getResource("/rsync1.out")

            rsync.processOutput(stream)

            val progress = operationExecutor.getProgress()
            progress.size shouldBe 236
            val entry = progress[90]
            entry.type shouldBe ProgressEntry.Type.PROGRESS
            entry.percent shouldBe 50
            entry.message shouldBe "13.56MB (14.88MB/s)"

            val completion = progress[235]
            completion.type shouldBe ProgressEntry.Type.END
            completion.message shouldBe "10.64MB sent  4.13KB received  (4.26MB/sec)"
        }

        "number to string returns correct result" {
            val rsync = getRsync()

            val Ki = 1024.0
            val Mi = 1024 * Ki
            val Gi = 1024 * Mi

            rsync.numberToString(45.0) shouldBe "45"
            rsync.numberToString(1023.0) shouldBe "1023"
            rsync.numberToString(1024.0) shouldBe "1K"
            rsync.numberToString(1025.0) shouldBe "1.00K"
            rsync.numberToString(2.5 * Ki) shouldBe "2.50K"
            rsync.numberToString(900 * Ki) shouldBe "900K"
            rsync.numberToString(900.2 * Ki) shouldBe "900.2K"
            rsync.numberToString(1000.6 * Ki) shouldBe "1001K"
            rsync.numberToString(10239.0) shouldBe "10.00K"
            rsync.numberToString(73.23 * Gi) shouldBe "73.23G"
        }

        "run generates correct output" {
            val stream = getResource("/rsync2.out")
            val process = mockk<Process>()
            every { process.inputStream } returns stream
            every { process.waitFor() } returns 0
            every { process.exitValue() } returns 0
            every { process.destroy() } just Runs

            every { executor.start(*anyVararg()) } returns process

            val rsync = getRsync()
            rsync.run()

            val progress = operationExecutor.getProgress()
            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.PROGRESS
            progress[0].percent shouldBe 0
            progress[0].message shouldBe "0B (0B/s)"
            progress[1].type shouldBe ProgressEntry.Type.END
            progress[1].message shouldBe "112.6KB sent  4.13KB received  (43.58KB/sec)"
        }

        "run fails if command fails" {
            val process = mockk<Process>()
            every { process.inputStream } returns ByteArrayInputStream("".toByteArray())
            every { process.errorStream } returns ByteArrayInputStream("error string".toByteArray())
            every { process.waitFor() } returns 0
            every { process.exitValue() } returns 1
            every { process.destroy() } just Runs

            every { executor.start(*anyVararg()) } returns process

            val rsync = getRsync()

            val e = shouldThrow<CommandException> {
                rsync.run()
            }

            e.exitCode shouldBe 1
            e.output shouldBe "error string"
        }
    }
}
