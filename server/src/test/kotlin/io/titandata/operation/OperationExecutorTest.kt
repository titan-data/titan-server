package io.titandata.operation

import io.kotlintest.Spec
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
import io.mockk.impl.annotations.MockK
import io.mockk.impl.annotations.OverrideMockKs
import io.mockk.impl.annotations.SpyK
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import io.titandata.ProviderModule
import io.titandata.exception.NoSuchObjectException
import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Repository
import io.titandata.models.Volume
import io.titandata.orchestrator.Reaper
import io.titandata.remote.nop.NopParameters
import io.titandata.remote.nop.NopRemote
import io.titandata.remote.nop.NopRemoteProvider
import io.titandata.storage.OperationData
import io.titandata.storage.zfs.ZfsStorageProvider
import org.jetbrains.exposed.sql.transactions.transaction

class OperationExecutorTest : StringSpec() {
    @MockK
    lateinit var zfsStorageProvider: ZfsStorageProvider

    @MockK
    lateinit var reaper: Reaper

    lateinit var vs: String

    @SpyK
    var nopRemoteProvider = NopRemoteProvider()

    @InjectMockKs
    @OverrideMockKs
    var providers = ProviderModule("test")

    override fun beforeSpec(spec: Spec) {
        providers.metadata.init()
    }

    override fun beforeTest(testCase: TestCase) {
        providers.metadata.clear()
        transaction {
            providers.metadata.createRepository(Repository("foo"))
            vs = providers.metadata.createVolumeSet("foo", null, true)
            providers.metadata.createVolume(vs, Volume("volume", config = mapOf("mountpoint" to "/mountpoint")))
            providers.metadata.addRemote("foo", NopRemote(name = "origin"))
        }
        val ret = MockKAnnotations.init(this)
        every { zfsStorageProvider.createCommit(any(), any(), any()) } just Runs
        return ret
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    fun createOperation(type: Operation.Type = Operation.Type.PULL): OperationData {
        val data = OperationData(operation = Operation(
                id = vs,
                type = type,
                state = Operation.State.RUNNING,
                remote = "remote",
                commitId = "id"
        ), params = NopParameters(), metadataOnly = false)
        transaction {
            providers.metadata.createOperation("foo", vs, data)
        }
        return data
    }

    fun getExecutor(data: OperationData): OperationExecutor {
        return OperationExecutor(providers, data.operation, "foo", NopRemote(name = "origin"), NopParameters(),
                data.metadataOnly)
    }

    init {
        "add progress appends entry" {
            val data = createOperation()
            val executor = getExecutor(data)
            executor.addProgress(ProgressEntry(type = ProgressEntry.Type.MESSAGE, message = "message"))
            val entries = transaction {
                providers.metadata.listProgressEntries(vs)
            }
            entries.size shouldBe 1
            entries[0].message shouldBe "message"
        }

        "add failed progress entry updates operation state" {
            val data = createOperation()
            val executor = getExecutor(data)
            executor.addProgress(ProgressEntry(type = ProgressEntry.Type.FAILED, message = "message"))
            executor.operation.state shouldBe Operation.State.FAILED
            val op = transaction {
                providers.metadata.getOperation(vs)
            }
            op.operation.state shouldBe Operation.State.FAILED
        }

        "add aborted progress entry updates operation state" {
            val data = createOperation()
            val executor = getExecutor(data)
            executor.addProgress(ProgressEntry(type = ProgressEntry.Type.ABORT, message = "message"))
            executor.operation.state shouldBe Operation.State.ABORTED
            val op = transaction {
                providers.metadata.getOperation(vs)
            }
            op.operation.state shouldBe Operation.State.ABORTED
        }

        "add completed progress entry updates operation state" {
            val data = createOperation()
            val executor = getExecutor(data)
            executor.addProgress(ProgressEntry(type = ProgressEntry.Type.COMPLETE, message = "message"))
            executor.operation.state shouldBe Operation.State.COMPLETE
            val op = transaction {
                providers.metadata.getOperation(vs)
            }
            op.operation.state shouldBe Operation.State.COMPLETE
        }

        "get progress tracks last updated progress" {
            val data = createOperation()
            val executor = getExecutor(data)
            executor.addProgress(ProgressEntry(type = ProgressEntry.Type.MESSAGE, message = "one"))
            var progress = executor.getProgress()
            progress.size shouldBe 1
            progress[0].message shouldBe "one"
            progress = executor.getProgress()
            progress.size shouldBe 0
            executor.addProgress(ProgressEntry(type = ProgressEntry.Type.MESSAGE, message = "two"))
            progress = executor.getProgress()
            progress.size shouldBe 1
            progress[0].message shouldBe "two"
        }

        "abort interrupts thread" {
            val thread = mockk<Thread>()
            every { thread.interrupt() } just Runs
            val data = createOperation()
            val executor = getExecutor(data)
            executor.thread = thread
            executor.abort()
            verify {
                thread.interrupt()
            }
        }

        "join joins thread" {
            val thread = mockk<Thread>()
            every { thread.join() } just Runs
            val data = createOperation()
            val executor = getExecutor(data)
            executor.thread = thread
            executor.join()
            verify {
                thread.join()
            }
        }

        "sync data for pull succeeds" {
            every { zfsStorageProvider.activateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.deactivateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { zfsStorageProvider.createVolume(any(), "_scratch") } returns mapOf("mountpoint" to "/scratch")
            every { zfsStorageProvider.deleteVolume(any(), any(), any()) } just Runs

            val data = createOperation()
            val executor = getExecutor(data)
            executor.syncData(nopRemoteProvider, null)

            verify {
                zfsStorageProvider.activateVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                zfsStorageProvider.activateVolume(data.operation.id, "volume", mapOf("mountpoint" to "/mountpoint"))
                zfsStorageProvider.deactivateVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                zfsStorageProvider.deactivateVolume(data.operation.id, "volume", mapOf("mountpoint" to "/mountpoint"))
                zfsStorageProvider.createVolume(data.operation.id, "_scratch")
                zfsStorageProvider.deleteVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                nopRemoteProvider.pullVolume(executor, null, Volume(name = "volume", config = mapOf("mountpoint" to "/mountpoint")), "/mountpoint", "/scratch")
            }
        }

        "sync data for push succeeds" {
            every { zfsStorageProvider.activateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.deactivateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { zfsStorageProvider.createVolume(any(), "_scratch") } returns mapOf("mountpoint" to "/scratch")
            every { zfsStorageProvider.deleteVolume(any(), any(), any()) } just Runs

            val data = createOperation(Operation.Type.PUSH)
            val executor = getExecutor(data)
            executor.syncData(nopRemoteProvider, null)

            verify {
                zfsStorageProvider.activateVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                zfsStorageProvider.activateVolume(data.operation.id, "volume", mapOf("mountpoint" to "/mountpoint"))
                zfsStorageProvider.deactivateVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                zfsStorageProvider.deactivateVolume(data.operation.id, "volume", mapOf("mountpoint" to "/mountpoint"))
                zfsStorageProvider.createVolume(data.operation.id, "_scratch")
                zfsStorageProvider.deleteVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                nopRemoteProvider.pushVolume(executor, null, Volume(name = "volume", config = mapOf("mountpoint" to "/mountpoint")), "/mountpoint", "/scratch")
            }
        }

        "pull operation succeeds" {
            every { zfsStorageProvider.activateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.deactivateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { zfsStorageProvider.createVolume(any(), "_scratch") } returns mapOf("mountpoint" to "/scratch")
            every { zfsStorageProvider.deleteVolume(any(), any(), any()) } just Runs

            val data = createOperation(Operation.Type.PULL)
            val executor = getExecutor(data)
            executor.run()

            data.operation.state shouldBe Operation.State.COMPLETE

            verify {
                nopRemoteProvider.pullVolume(executor, null, Volume("volume", config = mapOf("mountpoint" to "/mountpoint")), "/mountpoint", "/scratch")
                zfsStorageProvider.createCommit(data.operation.id, "id", listOf("volume"))
            }
        }

        "push operation succeeds" {
            every { zfsStorageProvider.activateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.deactivateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { zfsStorageProvider.createVolume(any(), "_scratch") } returns mapOf("mountpoint" to "/scratch")
            every { zfsStorageProvider.deleteVolume(any(), any(), any()) } just Runs

            transaction {
                providers.metadata.createCommit("foo", vs, Commit("id"))
            }

            val data = createOperation(Operation.Type.PUSH)
            val executor = getExecutor(data)
            executor.run()

            data.operation.state shouldBe Operation.State.COMPLETE

            verify {
                nopRemoteProvider.pushVolume(executor, null, Volume("volume", config = mapOf("mountpoint" to "/mountpoint")), "/mountpoint", "/scratch")
            }
        }

        "interrupted operation is aborted" {
            every { zfsStorageProvider.activateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.deactivateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { zfsStorageProvider.createVolume(any(), "_scratch") } returns mapOf("mountpoint" to "/scratch")
            every { zfsStorageProvider.deleteVolume(any(), any(), any()) } just Runs
            every { nopRemoteProvider.pullVolume(any(), any(), any(), any(), any()) } throws InterruptedException()

            val data = createOperation(Operation.Type.PULL)
            val executor = getExecutor(data)
            executor.run()

            data.operation.state shouldBe Operation.State.ABORTED

            verify {
                zfsStorageProvider.deactivateVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                zfsStorageProvider.deactivateVolume(data.operation.id, "volume", mapOf("mountpoint" to "/mountpoint"))
                zfsStorageProvider.deleteVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                nopRemoteProvider.pullVolume(executor, null, Volume(name = "volume", config = mapOf("mountpoint" to "/mountpoint")), "/mountpoint", "/scratch")
            }

            shouldThrow<NoSuchObjectException> {
                providers.commits.getCommit("foo", "id")
            }
        }

        "failed operation is marked failed" {
            every { zfsStorageProvider.activateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.deactivateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { zfsStorageProvider.createVolume(any(), "_scratch") } returns mapOf("mountpoint" to "/scratch")
            every { zfsStorageProvider.deleteVolume(any(), any(), any()) } just Runs
            every { nopRemoteProvider.pullVolume(any(), any(), any(), any(), any()) } throws Exception()

            val data = createOperation(Operation.Type.PULL)
            val executor = getExecutor(data)
            executor.run()

            data.operation.state shouldBe Operation.State.FAILED

            verify {
                zfsStorageProvider.deactivateVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                zfsStorageProvider.deactivateVolume(data.operation.id, "volume", mapOf("mountpoint" to "/mountpoint"))
                zfsStorageProvider.deleteVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                nopRemoteProvider.pullVolume(executor, null, Volume(name = "volume", config = mapOf("mountpoint" to "/mountpoint")), "/mountpoint", "/scratch")
            }

            shouldThrow<NoSuchObjectException> {
                providers.commits.getCommit("foo", "id")
            }
        }

        "provider fail operation is called" {
            every { zfsStorageProvider.activateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.deactivateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { zfsStorageProvider.createVolume(any(), "_scratch") } returns mapOf("mountpoint" to "/scratch")
            every { zfsStorageProvider.deleteVolume(any(), any(), any()) } just Runs
            every { nopRemoteProvider.pullVolume(any(), any(), any(), any(), any()) } throws Exception()
            every { nopRemoteProvider.startOperation(any()) } returns 1

            val data = createOperation(Operation.Type.PULL)
            val executor = getExecutor(data)
            executor.run()

            data.operation.state shouldBe Operation.State.FAILED

            verify {
                nopRemoteProvider.failOperation(executor, 1)
            }
        }
    }
}
