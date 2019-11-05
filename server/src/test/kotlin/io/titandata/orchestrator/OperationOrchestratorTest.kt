/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.orchestrator

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
import io.mockk.verify
import io.titandata.ProviderModule
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Repository
import io.titandata.operation.OperationExecutor
import io.titandata.remote.engine.EngineParameters
import io.titandata.remote.nop.NopParameters
import io.titandata.remote.nop.NopRemote
import io.titandata.remote.nop.NopRemoteProvider
import io.titandata.storage.OperationData
import io.titandata.storage.zfs.ZfsStorageProvider
import org.jetbrains.exposed.sql.transactions.transaction

class OperationOrchestratorTest : StringSpec() {

    @MockK
    lateinit var zfsStorageProvider: ZfsStorageProvider

    @SpyK
    var nopRemoteProvider = NopRemoteProvider()

    @InjectMockKs
    @OverrideMockKs
    var providers = ProviderModule("test")

    @InjectMockKs
    @OverrideMockKs
    lateinit var provider: OperationOrchestrator

    override fun beforeSpec(spec: Spec) {
        providers.metadata.init()
    }

    override fun beforeTest(testCase: TestCase) {
        provider = OperationOrchestrator(providers)
        providers.metadata.clear()
        transaction {
            providers.metadata.createRepository(Repository(name = "foo", properties = emptyMap()))
            providers.metadata.createVolumeSet("foo", true)
        }
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    fun addOperation(type: Operation.Type = Operation.Type.PULL) {
        provider.addOperation(OperationExecutor(providers,
                provider.buildOperation(type, "id", "remote", "commit"),
                "foo", NopRemote(name = "remote"),
                NopParameters()))
    }

    init {
        "list operations for non-existent repository fails" {
            shouldThrow<NoSuchObjectException> {
                provider.listOperations("bar")
            }
        }

        "list operations returns empty list" {
            val result = provider.listOperations("foo")
            result.size shouldBe 0
        }

        "list operations returns list of current operations" {
            addOperation()
            val result = provider.listOperations("foo")
            result.size shouldBe 1
            result[0].id shouldBe "id"
            result[0].remote shouldBe "remote"
            result[0].commitId shouldBe "commit"
        }

        "get operation fails for unknown operation" {
            shouldThrow<NoSuchObjectException> {
                provider.getOperation("repo", "id")
            }
        }

        "get operation should fail if known id but wrong repo" {
            addOperation()
            shouldThrow<NoSuchObjectException> {
                provider.getOperation("bar", "id")
            }
        }

        "get operation succeeds" {
            addOperation()
            val result = provider.getOperation("foo", "id")
            result.id shouldBe "id"
            result.remote shouldBe "remote"
            result.commitId shouldBe "commit"
        }

        "abort operation fails for unknown id" {
            shouldThrow<NoSuchObjectException> {
                provider.abortOperation("foo", "id")
            }
        }

        "abort operation succeeds" {
            addOperation()
            provider.getOperation("foo", "id")
            provider.abortOperation("foo", "id")
        }

        "pull for non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                provider.startPull("foo", "remote", "commit", NopParameters())
            }
        }

        "pull fails for mismatched remote fails" {
            transaction {
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            shouldThrow<IllegalArgumentException> {
                provider.startPull("foo", "remote", "commit", EngineParameters())
            }
        }

        "pull fails for non-existent remote commit" {
            transaction {
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            every { nopRemoteProvider.validateOperation(any(), any(), any(), any(), any()) } throws NoSuchObjectException("")
            shouldThrow<NoSuchObjectException> {
                provider.startPull("foo", "remote", "commit", NopParameters())
            }
        }

        "pull fails if local commit exists" {
            transaction {
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            every { zfsStorageProvider.getCommit(any(), any()) } returns Commit(id = "commit", properties = mapOf())
            shouldThrow<ObjectExistsException> {
                provider.startPull("foo", "remote", "commit", NopParameters())
            }
        }

        "pull fails if local commit does not exist and metadata only set" {
            transaction {
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            every { zfsStorageProvider.getCommit(any(), any()) } throws NoSuchObjectException("")
            shouldThrow<ObjectExistsException> {
                provider.startPull("foo", "remote", "commit", NopParameters(), true)
            }
        }

        "pull succeeds" {
            transaction {
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            every { zfsStorageProvider.getCommit(any(), any()) } throws NoSuchObjectException("")
            every { zfsStorageProvider.createOperation("foo", any(), any()) } just Runs
            every { zfsStorageProvider.commitOperation("foo", any(), any()) } just Runs
            every { zfsStorageProvider.createOperationScratch("foo", any()) } returns ""
            every { zfsStorageProvider.mountOperationVolumes("foo", any(), any()) } returns ""
            every { zfsStorageProvider.unmountOperationVolumes("foo", any(), any()) } just Runs
            every { zfsStorageProvider.destroyOperationScratch("foo", any()) } just Runs
            var op = provider.startPull("foo", "remote", "commit", NopParameters())
            op.commitId shouldBe "commit"
            op.type shouldBe Operation.Type.PULL
            op.remote shouldBe "remote"
            provider.getExecutor("foo", op.id).join()
            op = provider.getOperation("foo", op.id)
            op.state shouldBe Operation.State.COMPLETE

            val progress = provider.getProgress("foo", op.id)
            progress.size shouldBe 4
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[1].type shouldBe ProgressEntry.Type.START
            progress[1].message shouldBe "Running operation"
            progress[2].type shouldBe ProgressEntry.Type.END
            progress[3].type shouldBe ProgressEntry.Type.COMPLETE

            shouldThrow<NoSuchObjectException> {
                provider.getOperation("foo", op.id)
            }
        }

        "error during pull is reported correctly" {
            transaction {
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            every { zfsStorageProvider.getCommit(any(), any()) } throws NoSuchObjectException("")
            every { nopRemoteProvider.startOperation(any()) } throws Exception("error")
            every { zfsStorageProvider.createOperation("foo", any(), any()) } just Runs
            every { zfsStorageProvider.discardOperation("foo", any()) } just Runs
            every { zfsStorageProvider.createOperationScratch("foo", any()) } returns ""
            every { zfsStorageProvider.mountOperationVolumes("foo", any(), any()) } returns ""
            every { zfsStorageProvider.unmountOperationVolumes("foo", any(), any()) } just Runs
            every { zfsStorageProvider.destroyOperationScratch("foo", any()) } just Runs
            var op = provider.startPull("foo", "remote", "commit", NopParameters())
            provider.getExecutor("foo", op.id).join()
            op = provider.getOperation("foo", op.id)
            op.state shouldBe Operation.State.FAILED

            val progress = provider.getProgress("foo", op.id)
            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Pulling commit from 'remote'"
            progress[1].type shouldBe ProgressEntry.Type.FAILED
            progress[1].message shouldBe "error"
        }

        "interrupt during pull is reported correctly" {
            transaction {
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            every { zfsStorageProvider.getCommit(any(), any()) } throws NoSuchObjectException("")
            every { nopRemoteProvider.startOperation(any()) } throws InterruptedException("error")
            every { zfsStorageProvider.createOperation("foo", any(), any()) } just Runs
            every { zfsStorageProvider.discardOperation("foo", any()) } just Runs
            every { zfsStorageProvider.createOperationScratch("foo", any()) } returns ""
            every { zfsStorageProvider.mountOperationVolumes("foo", any(), any()) } returns ""
            every { zfsStorageProvider.unmountOperationVolumes("foo", any(), any()) } just Runs
            every { zfsStorageProvider.destroyOperationScratch("foo", any()) } just Runs
            var op = provider.startPull("foo", "remote", "commit", NopParameters())
            provider.getExecutor("foo", op.id).join()
            op = provider.getOperation("foo", op.id)
            op.state shouldBe Operation.State.ABORTED

            val progress = provider.getProgress("foo", op.id)
            progress.size shouldBe 2
            progress[1].type shouldBe ProgressEntry.Type.ABORT
        }

        "pull fails if conflicting operation is in progress" {
            addOperation()
            transaction {
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            every { zfsStorageProvider.getCommit(any(), any()) } throws NoSuchObjectException("")
            shouldThrow<ObjectExistsException> {
                provider.startPull("foo", "remote", "commit", NopParameters())
            }
        }

        "pull succeeds if non-conflicting operation is in progress" {
            addOperation(type = Operation.Type.PUSH)
            transaction {
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            every { zfsStorageProvider.getCommit(any(), any()) } throws NoSuchObjectException("")
            every { zfsStorageProvider.createOperation("foo", any(), any()) } just Runs
            every { zfsStorageProvider.commitOperation("foo", any(), any()) } just Runs
            every { zfsStorageProvider.createOperationScratch("foo", any()) } returns ""
            every { zfsStorageProvider.mountOperationVolumes("foo", any(), any()) } returns ""
            every { zfsStorageProvider.unmountOperationVolumes("foo", any(), any()) } just Runs
            every { zfsStorageProvider.destroyOperationScratch("foo", any()) } just Runs
            var op = provider.startPull("foo", "remote", "commit2", NopParameters())
            provider.getExecutor("foo", op.id).join()
            provider.getOperation("foo", op.id)
        }

        "push for non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                provider.startPush("foo", "remote", "commit", NopParameters())
            }
        }

        "push fails for mismatched remote" {
            transaction {
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            shouldThrow<IllegalArgumentException> {
                provider.startPush("foo", "remote", "commit", EngineParameters())
            }
        }

        "push fails if local commit cannot be found" {
            transaction {
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            every { zfsStorageProvider.getCommit(any(), any()) } throws NoSuchObjectException("")
            shouldThrow<NoSuchObjectException> {
                provider.startPush("foo", "remote", "commit", NopParameters())
            }
        }

        "push fails if remote commit exists" {
            transaction {
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            every { zfsStorageProvider.getCommit(any(), any()) } returns Commit(id = "commit", properties = mapOf())
            every { nopRemoteProvider.validateOperation(any(), any(), any(), any(), any()) } throws ObjectExistsException("")
            shouldThrow<ObjectExistsException> {
                provider.startPush("foo", "remote", "commit", NopParameters())
            }
        }

        "push succeeds" {
            transaction {
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            every { zfsStorageProvider.getCommit(any(), any()) } returns Commit(id = "commit", properties = mapOf())
            every { zfsStorageProvider.createOperation("foo", any(), any()) } just Runs
            every { zfsStorageProvider.discardOperation("foo", any()) } just Runs
            every { zfsStorageProvider.createOperationScratch("foo", any()) } returns ""
            every { zfsStorageProvider.mountOperationVolumes("foo", any(), any()) } returns ""
            every { zfsStorageProvider.unmountOperationVolumes("foo", any(), any()) } just Runs
            every { zfsStorageProvider.destroyOperationScratch("foo", any()) } just Runs
            var op = provider.startPush("foo", "remote", "commit", NopParameters())
            op.commitId shouldBe "commit"
            op.type shouldBe Operation.Type.PUSH
            op.remote shouldBe "remote"
            provider.getExecutor("foo", op.id).join()
            op = provider.getOperation("foo", op.id)
            op.state shouldBe Operation.State.COMPLETE

            val progress = provider.getProgress("foo", op.id)
            progress.size shouldBe 4
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Pushing commit to 'remote'"
            progress[1].type shouldBe ProgressEntry.Type.START
            progress[1].message shouldBe "Running operation"
            progress[2].type shouldBe ProgressEntry.Type.END
            progress[3].type shouldBe ProgressEntry.Type.COMPLETE

            shouldThrow<NoSuchObjectException> {
                provider.getOperation("foo", op.id)
            }
        }

        "error during push is reported correctly" {
            transaction {
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            every { zfsStorageProvider.getCommit(any(), any()) } returns Commit(id = "commit", properties = mapOf())
            every { nopRemoteProvider.startOperation(any()) } throws Exception("error")
            every { zfsStorageProvider.createOperation("foo", any(), any()) } just Runs
            every { zfsStorageProvider.discardOperation("foo", "id") } just Runs
            every { zfsStorageProvider.createOperationScratch("foo", any()) } returns ""
            every { zfsStorageProvider.mountOperationVolumes("foo", any(), any()) } returns ""
            every { zfsStorageProvider.unmountOperationVolumes("foo", any(), any()) } just Runs
            every { zfsStorageProvider.destroyOperationScratch("foo", any()) } just Runs
            var op = provider.startPush("foo", "remote", "commit", NopParameters())
            provider.getExecutor("foo", op.id).join()
            op = provider.getOperation("foo", op.id)
            op.state shouldBe Operation.State.FAILED

            val progress = provider.getProgress("foo", op.id)
            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Pushing commit to 'remote'"
            progress[1].type shouldBe ProgressEntry.Type.FAILED
            progress[1].message shouldBe "error"
        }

        "interrupt during push is reported correctly" {
            transaction {
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            every { zfsStorageProvider.getCommit(any(), any()) } returns Commit(id = "commit", properties = mapOf())
            every { nopRemoteProvider.startOperation(any()) } throws InterruptedException("error")
            every { zfsStorageProvider.createOperation("foo", any(), any()) } just Runs
            every { zfsStorageProvider.discardOperation("foo", "id") } just Runs
            every { zfsStorageProvider.createOperationScratch("foo", any()) } returns ""
            every { zfsStorageProvider.mountOperationVolumes("foo", any(), any()) } returns ""
            every { zfsStorageProvider.unmountOperationVolumes("foo", any(), any()) } just Runs
            every { zfsStorageProvider.destroyOperationScratch("foo", any()) } just Runs
            var op = provider.startPush("foo", "remote", "commit", NopParameters())
            provider.getExecutor("foo", op.id).join()
            op = provider.getOperation("foo", op.id)
            op.state shouldBe Operation.State.ABORTED

            val progress = provider.getProgress("foo", op.id)
            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[1].type shouldBe ProgressEntry.Type.ABORT
        }

        "push fails if conflicting operation is in progress" {
            addOperation(type = Operation.Type.PUSH)
            transaction {
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            every { zfsStorageProvider.getCommit(any(), any()) } returns Commit(id = "commit", properties = mapOf())
            shouldThrow<ObjectExistsException> {
                provider.startPush("foo", "remote", "commit", NopParameters())
            }
        }

        "push succeeds if non-conflicting operation is in progress" {
            addOperation(type = Operation.Type.PUSH)
            transaction {
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            every { zfsStorageProvider.getCommit(any(), any()) } returns Commit(id = "commit2", properties = mapOf())
            every { zfsStorageProvider.discardOperation("foo", any()) } just Runs
            every { zfsStorageProvider.createOperation("foo", any(), any()) } just Runs
            every { zfsStorageProvider.createOperationScratch("foo", any()) } returns ""
            every { zfsStorageProvider.mountOperationVolumes("foo", any(), any()) } returns ""
            every { zfsStorageProvider.unmountOperationVolumes("foo", any(), any()) } just Runs
            every { zfsStorageProvider.destroyOperationScratch("foo", any()) } just Runs
            var op = provider.startPush("foo", "remote", "commit2", NopParameters())
            provider.getExecutor("foo", op.id).join()
            op = provider.getOperation("foo", op.id)
            op.state shouldBe Operation.State.COMPLETE

            verify {
                zfsStorageProvider.discardOperation("foo", op.id)
            }
        }

        "loading completed operations populates operation list" {
            every { zfsStorageProvider.listOperations("foo") } returns listOf(
                    OperationData(operation = Operation(id = "op1", type = Operation.Type.PULL, state = Operation.State.COMPLETE,
                            remote = "remote", commitId = "commit1"), params = NopParameters()),
                    OperationData(operation = Operation(id = "op2", type = Operation.Type.PULL, state = Operation.State.COMPLETE,
                            remote = "remote", commitId = "commit2"), params = NopParameters())
            )
            transaction {
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            provider.loadState()

            val ops = provider.listOperations("foo")
            ops.size shouldBe 2
            ops[0].id shouldBe "op1"
            ops[0].commitId shouldBe "commit1"
            ops[0].state shouldBe Operation.State.COMPLETE
            ops[0].remote shouldBe "remote"
            ops[1].id shouldBe "op2"
            ops[1].commitId shouldBe "commit2"
            ops[1].state shouldBe Operation.State.COMPLETE
            ops[1].remote shouldBe "remote"
        }

        "loading running operation restarts operation" {
            every { zfsStorageProvider.listOperations("foo") } returns listOf(
                    OperationData(operation = Operation(id = "id", type = Operation.Type.PUSH, state = Operation.State.RUNNING,
                            remote = "remote", commitId = "commit"), params = NopParameters())
            )
            transaction {
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            every { zfsStorageProvider.createOperationScratch("foo", any()) } returns ""
            every { zfsStorageProvider.mountOperationVolumes("foo", any(), any()) } returns ""
            every { zfsStorageProvider.unmountOperationVolumes("foo", any(), any()) } just Runs
            every { zfsStorageProvider.destroyOperationScratch("foo", any()) } just Runs
            every { zfsStorageProvider.getCommit("foo", any()) } returns Commit(id = "hash", properties = mapOf())
            provider.loadState()

            var op = provider.getOperation("foo", "id")

            op.id shouldBe "id"
            op.commitId shouldBe "commit"
            op.type shouldBe Operation.Type.PUSH
            op.remote shouldBe "remote"
            provider.getExecutor("foo", op.id).join()
            op = provider.getOperation("foo", op.id)
            op.state shouldBe Operation.State.COMPLETE

            val progress = provider.getProgress("foo", op.id)
            progress.size shouldBe 4
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Retrying operation after restart"
            progress[1].type shouldBe ProgressEntry.Type.START
            progress[1].message shouldBe "Running operation"
            progress[2].type shouldBe ProgressEntry.Type.END
            progress[3].type shouldBe ProgressEntry.Type.COMPLETE
        }
    }
}
