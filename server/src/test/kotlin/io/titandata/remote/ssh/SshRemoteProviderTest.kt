/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.ssh

import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.matchers.string.shouldContain
import io.kotlintest.shouldBe
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
import io.titandata.ProviderModule
import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Repository
import io.titandata.models.Volume
import io.titandata.operation.OperationExecutor
import io.titandata.storage.OperationData
import io.titandata.storage.zfs.ZfsStorageProvider
import io.titandata.util.CommandExecutor
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import org.jetbrains.exposed.sql.transactions.transaction

class SshRemoteProviderTest : StringSpec() {

    val params = RemoteParameters("ssh")

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

    override fun beforeSpec(spec: Spec) {
        providers.metadata.init()
    }

    override fun beforeTest(testCase: TestCase) {
        sshRemoteProvider = SshRemoteProvider(providers)
        providers.metadata.clear()
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    fun getResource(name: String): InputStream {
        return this.javaClass.getResource(name).openStream()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    fun getRemote(): Remote {
        return Remote(
                provider = "ssh",
                name = "remote",
                properties = mapOf(
                        "address" to "localhost",
                        "username" to "root",
                        "password" to "root",
                        "path" to "/var/tmp"
                )
        )
    }

    fun createOperation(vs: String, type: Operation.Type = Operation.Type.PULL): OperationData {
        val data = OperationData(operation = Operation(
                id = vs,
                type = type,
                state = Operation.State.RUNNING,
                remote = "remote",
                commitId = "id"
        ), params = params, metadataOnly = false)
        transaction {
            providers.metadata.createOperation("foo", vs, data)
        }
        return data
    }

    init {
        "run operation succeeds" {
            val vs = transaction {
                providers.metadata.createRepository(Repository(name = "repo"))
                val vs = providers.metadata.createVolumeSet("repo", null, true)
                providers.metadata.createVolume(vs, Volume("v0", config = mapOf("mountpoint" to "/mountpoint")))
                providers.metadata.createVolume(vs, Volume(name = "v1", properties = mapOf("path" to "/volume"), config = mapOf("mountpoint" to "/mountpoint")))
                providers.metadata.createCommit("repo", vs, Commit("id"))
                vs
            }
            val data = createOperation(vs, Operation.Type.PUSH)
            val operationExecutor = OperationExecutor(providers, data.operation, "repo", getRemote(), data.params)

            every { zfsStorageProvider.activateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.deactivateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { zfsStorageProvider.deleteVolume(any(), any(), any()) } just Runs

            val process = mockk<Process>()
            every { process.inputStream } answers { getResource("/rsync2.out") }
            every { process.waitFor() } returns 0
            every { process.waitFor(any(), any()) } returns false
            every { process.exitValue() } returns 0
            every { process.outputStream } returns ByteArrayOutputStream()
            every { process.isAlive() } returns false
            every { process.destroy() } just Runs

            every { executor.start(*anyVararg()) } returns process

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
            val vs = transaction {
                providers.metadata.createRepository(Repository("repo"))
                val vs = providers.metadata.createVolumeSet("repo", null, true)
                providers.metadata.createVolume(vs, Volume("v0", config = mapOf("mountpoint" to "/mountpoint")))
                providers.metadata.createVolume(vs, Volume("v1", properties = mapOf("path" to "/volume"), config = mapOf("mountpoint" to "/mountpoint")))
                providers.metadata.createCommit("repo", vs, Commit("id"))
                vs
            }
            val data = createOperation(vs, Operation.Type.PUSH)
            val operationExecutor = OperationExecutor(providers, data.operation, "repo", getRemote(), data.params)

            every { zfsStorageProvider.activateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.deactivateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { zfsStorageProvider.deleteVolume(any(), any(), any()) } just Runs

            val process = mockk<Process>()
            every { process.inputStream } returns ByteArrayInputStream("".toByteArray())
            every { process.errorStream } returns ByteArrayInputStream("error string".toByteArray())
            every { process.waitFor() } returns 0
            every { process.waitFor(any(), any()) } returns false
            every { process.exitValue() } returns 1
            every { process.isAlive() } returns false
            every { process.destroy() } just Runs

            every { executor.start(*anyVararg()) } returns process

            operationExecutor.run()

            val progress = operationExecutor.getProgress()
            progress[0].type shouldBe ProgressEntry.Type.START
            progress[1].type shouldBe ProgressEntry.Type.FAILED
            progress[1].message shouldContain "error string"
        }
    }
}
