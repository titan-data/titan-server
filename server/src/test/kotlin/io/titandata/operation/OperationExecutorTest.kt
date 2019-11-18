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
import io.titandata.ServiceLocator
import io.titandata.context.docker.DockerZfsContext
import io.titandata.exception.NoSuchObjectException
import io.titandata.metadata.OperationData
import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Repository
import io.titandata.models.Volume
import io.titandata.remote.RemoteOperation
import io.titandata.remote.RemoteOperationType
import io.titandata.remote.RemoteProgress
import io.titandata.remote.nop.server.NopRemoteServer
import org.jetbrains.exposed.sql.transactions.transaction

class OperationExecutorTest : StringSpec() {
    @MockK
    lateinit var context: DockerZfsContext

    lateinit var vs: String

    @SpyK
    var nopProvider = NopRemoteServer()

    @InjectMockKs
    @OverrideMockKs
    var services = ServiceLocator(mockk())

    override fun beforeSpec(spec: Spec) {
        services.metadata.init()
    }

    override fun beforeTest(testCase: TestCase) {
        services.metadata.clear()
        transaction {
            services.metadata.createRepository(Repository("foo"))
            vs = services.metadata.createVolumeSet("foo", null, true)
            services.metadata.createVolume(vs, Volume("volume", config = mapOf("mountpoint" to "/mountpoint")))
            services.metadata.addRemote("foo", Remote("nop", "origin"))
        }
        val ret = MockKAnnotations.init(this)
        services.setRemoteProvider("nop", nopProvider)
        every { context.createCommit(any(), any(), any()) } just Runs
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
        ), params = RemoteParameters("nop"), metadataOnly = false)
        transaction {
            services.metadata.createOperation("foo", vs, data)
        }
        return data
    }

    fun createRemoteOperation(type: RemoteOperationType = RemoteOperationType.PULL): RemoteOperation {
        return RemoteOperation(
                updateProgress = { _: RemoteProgress, _: String?, _: Int? -> Unit },
                operationId = "operation",
                commitId = "commit",
                commit = null,
                remote = emptyMap(),
                parameters = emptyMap(),
                type = type,
                data = null
        )
    }

    fun getExecutor(data: OperationData): OperationExecutor {
        return OperationExecutor(services, data.operation, "foo", Remote("nop", "origin"), RemoteParameters("nop"),
                data.metadataOnly)
    }

    init {
        "add progress appends entry" {
            val data = createOperation()
            val executor = getExecutor(data)
            executor.addProgress(ProgressEntry(type = ProgressEntry.Type.MESSAGE, message = "message"))
            val entries = transaction {
                services.metadata.listProgressEntries(vs)
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
                services.metadata.getOperation(vs)
            }
            op.operation.state shouldBe Operation.State.FAILED
        }

        "add aborted progress entry updates operation state" {
            val data = createOperation()
            val executor = getExecutor(data)
            executor.addProgress(ProgressEntry(type = ProgressEntry.Type.ABORT, message = "message"))
            executor.operation.state shouldBe Operation.State.ABORTED
            val op = transaction {
                services.metadata.getOperation(vs)
            }
            op.operation.state shouldBe Operation.State.ABORTED
        }

        "add completed progress entry updates operation state" {
            val data = createOperation()
            val executor = getExecutor(data)
            executor.addProgress(ProgressEntry(type = ProgressEntry.Type.COMPLETE, message = "message"))
            executor.operation.state shouldBe Operation.State.COMPLETE
            val op = transaction {
                services.metadata.getOperation(vs)
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
            every { context.activateVolume(any(), any(), any()) } just Runs
            every { context.deactivateVolume(any(), any(), any()) } just Runs
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.createVolume(any(), "_scratch") } returns mapOf("mountpoint" to "/scratch")
            every { context.deleteVolume(any(), any(), any()) } just Runs

            val data = createOperation()
            val executor = getExecutor(data)
            val remoteOperation = createRemoteOperation()
            executor.syncData(nopProvider, remoteOperation)

            verify {
                context.activateVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                context.activateVolume(data.operation.id, "volume", mapOf("mountpoint" to "/mountpoint"))
                context.deactivateVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                context.deactivateVolume(data.operation.id, "volume", mapOf("mountpoint" to "/mountpoint"))
                context.createVolume(data.operation.id, "_scratch")
                context.deleteVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                nopProvider.syncVolume(remoteOperation, "volume", "volume", "/mountpoint", "/scratch")
            }
        }

        "sync data for push succeeds" {
            every { context.activateVolume(any(), any(), any()) } just Runs
            every { context.deactivateVolume(any(), any(), any()) } just Runs
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.createVolume(any(), "_scratch") } returns mapOf("mountpoint" to "/scratch")
            every { context.deleteVolume(any(), any(), any()) } just Runs

            val data = createOperation(Operation.Type.PUSH)
            val executor = getExecutor(data)
            val remoteOperation = createRemoteOperation(RemoteOperationType.PUSH)
            executor.syncData(nopProvider, remoteOperation)

            verify {
                context.activateVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                context.activateVolume(data.operation.id, "volume", mapOf("mountpoint" to "/mountpoint"))
                context.deactivateVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                context.deactivateVolume(data.operation.id, "volume", mapOf("mountpoint" to "/mountpoint"))
                context.createVolume(data.operation.id, "_scratch")
                context.deleteVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                nopProvider.syncVolume(remoteOperation, "volume", "volume", "/mountpoint", "/scratch")
            }
        }

        "pull operation succeeds" {
            every { context.activateVolume(any(), any(), any()) } just Runs
            every { context.deactivateVolume(any(), any(), any()) } just Runs
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.createVolume(any(), "_scratch") } returns mapOf("mountpoint" to "/scratch")
            every { context.deleteVolume(any(), any(), any()) } just Runs

            val data = createOperation(Operation.Type.PULL)
            val executor = getExecutor(data)
            executor.run()

            data.operation.state shouldBe Operation.State.COMPLETE

            verify {
                nopProvider.syncVolume(any(), "volume", "volume", "/mountpoint", "/scratch")
                context.createCommit(data.operation.id, "id", listOf("volume"))
            }
        }

        "push operation succeeds" {
            every { context.activateVolume(any(), any(), any()) } just Runs
            every { context.deactivateVolume(any(), any(), any()) } just Runs
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.createVolume(any(), "_scratch") } returns mapOf("mountpoint" to "/scratch")
            every { context.deleteVolume(any(), any(), any()) } just Runs

            transaction {
                services.metadata.createCommit("foo", vs, Commit("id"))
            }

            val data = createOperation(Operation.Type.PUSH)
            val executor = getExecutor(data)
            executor.run()

            data.operation.state shouldBe Operation.State.COMPLETE

            verify {
                nopProvider.syncVolume(any(), "volume", "volume", "/mountpoint", "/scratch")
            }
        }

        "interrupted operation is aborted" {
            every { context.activateVolume(any(), any(), any()) } just Runs
            every { context.deactivateVolume(any(), any(), any()) } just Runs
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.createVolume(any(), "_scratch") } returns mapOf("mountpoint" to "/scratch")
            every { context.deleteVolume(any(), any(), any()) } just Runs
            every { nopProvider.syncVolume(any(), any(), any(), any(), any()) } throws InterruptedException()

            val data = createOperation(Operation.Type.PULL)
            val executor = getExecutor(data)
            executor.run()

            data.operation.state shouldBe Operation.State.ABORTED

            verify {
                context.deactivateVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                context.deactivateVolume(data.operation.id, "volume", mapOf("mountpoint" to "/mountpoint"))
                context.deleteVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                nopProvider.syncVolume(any(), "volume", "volume", "/mountpoint", "/scratch")
            }

            shouldThrow<NoSuchObjectException> {
                services.commits.getCommit("foo", "id")
            }
        }

        "failed operation is marked failed" {
            every { context.activateVolume(any(), any(), any()) } just Runs
            every { context.deactivateVolume(any(), any(), any()) } just Runs
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.createVolume(any(), "_scratch") } returns mapOf("mountpoint" to "/scratch")
            every { context.deleteVolume(any(), any(), any()) } just Runs
            every { nopProvider.syncVolume(any(), any(), any(), any(), any()) } throws Exception()

            val data = createOperation(Operation.Type.PULL)
            val executor = getExecutor(data)
            executor.run()

            data.operation.state shouldBe Operation.State.FAILED

            verify {
                context.deactivateVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                context.deactivateVolume(data.operation.id, "volume", mapOf("mountpoint" to "/mountpoint"))
                context.deleteVolume(data.operation.id, "_scratch", mapOf("mountpoint" to "/scratch"))
                nopProvider.syncVolume(any(), "volume", "volume", "/mountpoint", "/scratch")
            }

            shouldThrow<NoSuchObjectException> {
                services.commits.getCommit("foo", "id")
            }
        }

        "provider fail operation is called" {
            every { context.activateVolume(any(), any(), any()) } just Runs
            every { context.deactivateVolume(any(), any(), any()) } just Runs
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.createVolume(any(), "_scratch") } returns mapOf("mountpoint" to "/scratch")
            every { context.deleteVolume(any(), any(), any()) } just Runs
            every { nopProvider.syncVolume(any(), any(), any(), any(), any()) } throws Exception()

            val data = createOperation(Operation.Type.PULL)
            val executor = getExecutor(data)
            executor.run()

            data.operation.state shouldBe Operation.State.FAILED

            verify {
                nopProvider.endOperation(any(), false)
            }
        }
    }
}
