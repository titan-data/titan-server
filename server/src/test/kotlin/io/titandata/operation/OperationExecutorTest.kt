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
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import io.titandata.ServiceLocator
import io.titandata.context.docker.DockerZfsContext
import io.titandata.exception.NoSuchObjectException
import io.titandata.metadata.OperationData
import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Repository
import io.titandata.models.Volume
import io.titandata.remote.RemoteOperation
import io.titandata.remote.RemoteOperationType
import io.titandata.remote.RemoteProgress
import org.jetbrains.exposed.sql.transactions.transaction

class OperationExecutorTest : StringSpec() {
    @MockK(relaxUnitFun = true)
    lateinit var context: DockerZfsContext

    lateinit var vs: String

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
        ), params = RemoteParameters("nop"), metadataOnly = false, repo = "foo")
        transaction {
            services.metadata.createOperation("foo", vs, data)
        }
        return data
    }

    fun createRemoteOperation(type: RemoteOperationType = RemoteOperationType.PULL): RemoteOperation {
        return RemoteOperation(
                updateProgress = { _: RemoteProgress, _: String?, _: Int? -> Unit },
                operationId = vs,
                commitId = "commit",
                commit = null,
                remote = emptyMap(),
                parameters = emptyMap(),
                type = type
        )
    }

    fun getExecutor(data: OperationData): OperationExecutor {
        return services.operations.createExecutor(data.operation, "foo", Remote("nop", "origin"), RemoteParameters("nop"),
                data.metadataOnly)
    }

    init {

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
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.createVolume(any(), "x-scratch") } returns mapOf("mountpoint" to "/scratch")

            val data = createOperation()
            val executor = getExecutor(data)
            val remoteOperation = createRemoteOperation()
            executor.syncData(services.remoteProvider("nop"), remoteOperation)

            verify {
                context.activateVolume(data.operation.id, "x-scratch", mapOf("mountpoint" to "/scratch"))
                context.activateVolume(data.operation.id, "volume", mapOf("mountpoint" to "/mountpoint"))
                context.deactivateVolume(data.operation.id, "x-scratch", mapOf("mountpoint" to "/scratch"))
                context.deactivateVolume(data.operation.id, "volume", mapOf("mountpoint" to "/mountpoint"))
                context.createVolume(data.operation.id, "x-scratch")
                context.deleteVolume(data.operation.id, "x-scratch", mapOf("mountpoint" to "/scratch"))
                context.syncVolumes(any(), any(), listOf(Volume("volume", emptyMap(), mapOf("mountpoint" to "/mountpoint"))),
                        Volume("x-scratch", emptyMap(), mapOf("mountpoint" to "/scratch")))
            }
        }

        "sync data for push succeeds" {
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.createVolume(any(), "x-scratch") } returns mapOf("mountpoint" to "/scratch")

            val data = createOperation(Operation.Type.PUSH)
            val executor = getExecutor(data)
            val remoteOperation = createRemoteOperation(RemoteOperationType.PUSH)
            executor.syncData(services.remoteProvider("nop"), remoteOperation)

            verify {
                context.activateVolume(data.operation.id, "x-scratch", mapOf("mountpoint" to "/scratch"))
                context.activateVolume(data.operation.id, "volume", mapOf("mountpoint" to "/mountpoint"))
                context.deactivateVolume(data.operation.id, "x-scratch", mapOf("mountpoint" to "/scratch"))
                context.deactivateVolume(data.operation.id, "volume", mapOf("mountpoint" to "/mountpoint"))
                context.createVolume(data.operation.id, "x-scratch")
                context.deleteVolume(data.operation.id, "x-scratch", mapOf("mountpoint" to "/scratch"))
                context.syncVolumes(any(), any(), listOf(Volume("volume", emptyMap(), mapOf("mountpoint" to "/mountpoint"))),
                        Volume("x-scratch", emptyMap(), mapOf("mountpoint" to "/scratch")))
            }
        }

        "pull operation succeeds" {
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.createVolume(any(), "x-scratch") } returns mapOf("mountpoint" to "/scratch")

            val data = createOperation(Operation.Type.PULL)
            val executor = getExecutor(data)
            executor.run()

            services.operations.getOperation(vs).state shouldBe Operation.State.COMPLETE

            verify {
                context.syncVolumes(any(), any(), listOf(Volume("volume", emptyMap(), mapOf("mountpoint" to "/mountpoint"))),
                        Volume("x-scratch", emptyMap(), mapOf("mountpoint" to "/scratch")))
                context.commitVolumeSet(data.operation.id, "id")
                context.commitVolume(data.operation.id, "id", "volume", mapOf("mountpoint" to "/mountpoint"))
            }
        }

        "push operation succeeds" {
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.createVolume(any(), "x-scratch") } returns mapOf("mountpoint" to "/scratch")

            transaction {
                services.metadata.createCommit("foo", vs, Commit("id"))
            }

            val data = createOperation(Operation.Type.PUSH)
            val executor = getExecutor(data)
            executor.run()

            services.operations.getOperation(vs).state shouldBe Operation.State.COMPLETE

            verify {
                context.syncVolumes(any(), any(), listOf(Volume("volume", emptyMap(), mapOf("mountpoint" to "/mountpoint"))),
                        Volume("x-scratch", emptyMap(), mapOf("mountpoint" to "/scratch")))
            }
        }

        "interrupted operation is aborted" {
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.createVolume(any(), "x-scratch") } returns mapOf("mountpoint" to "/scratch")
            every { context.syncVolumes(any(), any(), any(), any()) } throws InterruptedException()

            val data = createOperation(Operation.Type.PULL)
            val executor = getExecutor(data)
            executor.run()

            services.operations.getOperation(vs).state shouldBe Operation.State.ABORTED

            verify {
                context.deactivateVolume(data.operation.id, "x-scratch", mapOf("mountpoint" to "/scratch"))
                context.deactivateVolume(data.operation.id, "volume", mapOf("mountpoint" to "/mountpoint"))
                context.deleteVolume(data.operation.id, "x-scratch", mapOf("mountpoint" to "/scratch"))
                context.syncVolumes(any(), any(), listOf(Volume("volume", emptyMap(), mapOf("mountpoint" to "/mountpoint"))),
                        Volume("x-scratch", emptyMap(), mapOf("mountpoint" to "/scratch")))
            }

            shouldThrow<NoSuchObjectException> {
                services.commits.getCommit("foo", "id")
            }
        }

        "failed operation is marked failed" {
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.createVolume(any(), "x-scratch") } returns mapOf("mountpoint" to "/scratch")
            every { context.syncVolumes(any(), any(), any(), any()) } throws Exception()

            val data = createOperation(Operation.Type.PULL)
            val executor = getExecutor(data)
            executor.run()

            services.operations.getOperation(vs).state shouldBe Operation.State.FAILED

            verify {
                context.deactivateVolume(data.operation.id, "x-scratch", mapOf("mountpoint" to "/scratch"))
                context.deactivateVolume(data.operation.id, "volume", mapOf("mountpoint" to "/mountpoint"))
                context.deleteVolume(data.operation.id, "x-scratch", mapOf("mountpoint" to "/scratch"))
                context.syncVolumes(any(), any(), listOf(Volume("volume", emptyMap(), mapOf("mountpoint" to "/mountpoint"))),
                        Volume("x-scratch", emptyMap(), mapOf("mountpoint" to "/scratch")))
            }

            shouldThrow<NoSuchObjectException> {
                services.commits.getCommit("foo", "id")
            }
        }
    }
}
