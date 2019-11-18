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
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Repository
import io.titandata.models.Volume
import io.titandata.remote.nop.server.NopRemoteServer
import io.titandata.remote.s3.server.S3RemoteServer
import io.titandata.storage.OperationData
import io.titandata.storage.zfs.ZfsStorageProvider
import java.util.UUID
import org.jetbrains.exposed.sql.transactions.transaction

class OperationOrchestratorTest : StringSpec() {

    @MockK
    lateinit var zfsStorageProvider: ZfsStorageProvider

    @SpyK
    var s3Provider = S3RemoteServer()

    @SpyK
    var nopProvider = NopRemoteServer()

    lateinit var vs: String

    @InjectMockKs
    @OverrideMockKs
    var providers = ProviderModule("test")

    override fun beforeSpec(spec: Spec) {
        providers.metadata.init()
    }

    override fun beforeTest(testCase: TestCase) {
        providers.metadata.clear()
        vs = transaction {
            providers.metadata.createRepository(Repository("foo"))
            val vs = providers.metadata.createVolumeSet("foo", null, true)
            providers.metadata.createVolume(vs, Volume("volume", config = mapOf("mountpoint" to "/mountpoint")))
            vs
        }
        MockKAnnotations.init(this)
        providers.setRemoteProvider("nop", nopProvider)
        providers.setRemoteProvider("s3", s3Provider)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    val params = RemoteParameters("nop")

    fun buildOperation(type: Operation.Type = Operation.Type.PULL): OperationData {
        return OperationData(operation = Operation(
                id = vs,
                type = type,
                state = Operation.State.RUNNING,
                remote = "remote",
                commitId = "id"
        ), params = params, metadataOnly = false)
    }

    init {
        "get operation succeeds" {
            transaction {
                providers.metadata.createOperation("foo", vs, buildOperation())
                val op = providers.operations.getOperation("foo", vs)
                op.id shouldBe vs
                op.remote shouldBe "remote"
                op.commitId shouldBe "id"
            }
        }

        "get operation fails if incorrect repo specified" {
            transaction {
                providers.metadata.createOperation("foo", vs, buildOperation())
                providers.metadata.createRepository(Repository(name = "bar"))
            }
            shouldThrow<NoSuchObjectException> {
                providers.operations.getOperation("bar", vs)
            }
        }

        "get operation fails for invalid operation id" {
            shouldThrow<IllegalArgumentException> {
                providers.operations.getOperation("foo", "bad/id")
            }
        }

        "get operation fails for invalid repository name" {
            shouldThrow<IllegalArgumentException> {
                providers.operations.getOperation("bad/repo", UUID.randomUUID().toString())
            }
        }

        "get operation fails for non-existent repository" {
            shouldThrow<NoSuchObjectException> {
                providers.operations.getOperation("bar", UUID.randomUUID().toString())
            }
        }

        "get operation fails for non-existent operation" {
            shouldThrow<NoSuchObjectException> {
                providers.operations.getOperation("repo", UUID.randomUUID().toString())
            }
        }

        "list operations for non-existent repository fails" {
            shouldThrow<NoSuchObjectException> {
                providers.operations.listOperations("bar")
            }
        }

        "list operations fails with invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                providers.operations.listOperations("bad/repo")
            }
        }

        "list operations returns empty list" {
            val result = providers.operations.listOperations("foo")
            result.size shouldBe 0
        }

        "list operations succeeds" {
            transaction {
                providers.metadata.createOperation("foo", vs, buildOperation())
            }
            val result = providers.operations.listOperations("foo")
            result.size shouldBe 1
            result[0].id shouldBe vs
            result[0].commitId shouldBe "id"
        }

        "pull fails for invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                providers.operations.startPull("bad/repo", "remote", "id", params)
            }
        }

        "pull fails for invalid remote name" {
            shouldThrow<IllegalArgumentException> {
                providers.operations.startPull("foo", "bad/remote", "id", params)
            }
        }

        "pull fails for invalid commit id" {
            shouldThrow<IllegalArgumentException> {
                providers.operations.startPull("foo", "remote", "bad/id", params)
            }
        }

        "pull fails for non-existent repo" {
            shouldThrow<NoSuchObjectException> {
                providers.operations.startPull("bar", "remote", "id", params)
            }
        }

        "pull for non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                providers.operations.startPull("foo", "remote", "id", params)
            }
        }

        "pull fails for mismatched remote fails" {
            providers.remotes.addRemote("foo", Remote("nop", "remote"))
            shouldThrow<IllegalArgumentException> {
                providers.operations.startPull("foo", "remote", "commit", RemoteParameters("s3"))
            }
        }

        "pull fails for invalid remote parameters" {
            providers.remotes.addRemote("foo", Remote("nop", "remote"))
            shouldThrow<IllegalArgumentException> {
                providers.operations.startPull("foo", "remote", "commit", RemoteParameters("nop", mapOf("a" to "b")))
            }
        }

        "pull fails for non-existent remote commit" {
            providers.remotes.addRemote("foo", Remote("s3", "remote", mapOf("bucket" to "bucket")))
            every { s3Provider.getCommit(any(), any(), any()) } throws NoSuchObjectException("")
            shouldThrow<NoSuchObjectException> {
                providers.operations.startPull("foo", "remote", "id", RemoteParameters("s3", mapOf("accessKey" to "key", "secretKey" to "key")))
            }
        }

        "pull fails if local commit exists" {
            providers.remotes.addRemote("foo", Remote("nop", "remote"))
            transaction {
                providers.metadata.createCommit("foo", providers.metadata.getActiveVolumeSet("foo"), Commit(id = "id"))
            }
            shouldThrow<ObjectExistsException> {
                providers.operations.startPull("foo", "remote", "id", params)
            }
        }

        "pull fails if local commit does not exist and metadata only set" {
            providers.remotes.addRemote("foo", Remote("nop", "remote"))
            shouldThrow<NoSuchObjectException> {
                providers.operations.startPull("foo", "remote", "id", params, true)
            }
        }

        "push fails for invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                providers.operations.startPush("bad/repo", "remote", "id", params)
            }
        }

        "push fails for invalid remote name" {
            shouldThrow<IllegalArgumentException> {
                providers.operations.startPush("foo", "bad/remote", "id", params)
            }
        }

        "push fails for invalid commit id" {
            shouldThrow<IllegalArgumentException> {
                providers.operations.startPush("foo", "remote", "bad/id", params)
            }
        }

        "push fails for non-existent repo" {
            shouldThrow<NoSuchObjectException> {
                providers.operations.startPush("bar", "remote", "id", params)
            }
        }

        "push for non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                providers.operations.startPush("foo", "remote", "id", params)
            }
        }

        "push fails for mismatched remote fails" {
            providers.remotes.addRemote("foo", Remote("nop", "remote"))
            shouldThrow<IllegalArgumentException> {
                providers.operations.startPush("foo", "remote", "id", RemoteParameters("s3"))
            }
        }

        "push fails for invalid remote configuration" {
            providers.remotes.addRemote("foo", Remote("nop", "remote"))
            shouldThrow<IllegalArgumentException> {
                providers.operations.startPush("foo", "remote", "id", RemoteParameters("nop", mapOf("foo" to "bar")))
            }
        }

        "push fails if local commit cannot be found" {
            providers.remotes.addRemote("foo", Remote("nop", "remote"))
            shouldThrow<NoSuchObjectException> {
                providers.operations.startPush("foo", "remote", "id", params)
            }
        }

        "push fails if remote commit exists" {
            providers.remotes.addRemote("foo", Remote("s3", "remote", mapOf("bucket" to "bucket")))
            transaction {
                providers.metadata.createCommit("foo", providers.metadata.getActiveVolumeSet("foo"), Commit(id = "id"))
            }
            every { s3Provider.getCommit(any(), any(), any()) } returns emptyMap()
            shouldThrow<ObjectExistsException> {
                providers.operations.startPush("foo", "remote", "id", RemoteParameters("s3", mapOf("accessKey" to "key", "secretKey" to "key")))
            }
        }

        "abort operation fails for bad repository name" {
            shouldThrow<IllegalArgumentException> {
                providers.operations.abortOperation("bad/repo", vs)
            }
        }

        "abort operation fails for bad operation name" {
            shouldThrow<IllegalArgumentException> {
                providers.operations.abortOperation("foo", "badid")
            }
        }

        "abort operation fails for unknown repository" {
            shouldThrow<NoSuchObjectException> {
                providers.operations.abortOperation("bar", vs)
            }
        }

        "abort operation fails for unknown operation" {
            shouldThrow<NoSuchObjectException> {
                providers.operations.abortOperation("bar", UUID.randomUUID().toString())
            }
        }

        "create storage with no commit creates new volume set" {
            every { zfsStorageProvider.createVolumeSet(any()) } just Runs
            every { zfsStorageProvider.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint2")

            val newVolumeSet = transaction {
                val vs = providers.metadata.createVolumeSet("foo")
                providers.metadata.createVolume(vs, Volume(name = "volume"))
                vs
            }

            providers.operations.createStorage("foo", newVolumeSet, null)

            val vol = transaction {
                providers.metadata.getVolume(newVolumeSet, "volume")
            }
            vol.config["mountpoint"] shouldBe "/mountpoint2"

            verify {
                zfsStorageProvider.createVolumeSet(newVolumeSet)
                zfsStorageProvider.createVolume(newVolumeSet, "volume")
            }
        }

        "create storage with existing commit clones volume set" {
            every { zfsStorageProvider.cloneVolumeSet(any(), any(), any()) } just Runs
            every { zfsStorageProvider.cloneVolume(any(), any(), any(), any()) } returns mapOf("mountpoint" to "/mountpoint2")

            val (commitVs, commit) = transaction {
                val cvs = providers.metadata.createVolumeSet("foo")
                providers.metadata.createVolume(cvs, Volume("volume"))
                providers.metadata.createCommit("foo", cvs, Commit("id"))
                providers.metadata.getCommit("foo", "id")
            }

            val newVs = transaction {
                val vs = providers.metadata.createVolumeSet("foo")
                providers.metadata.createVolume(vs, Volume("volume"))
                vs
            }

            providers.operations.createStorage("foo", newVs, commit.id)

            val vol = transaction {
                providers.metadata.getVolume(newVs, "volume")
            }
            vol.config["mountpoint"] shouldBe "/mountpoint2"

            verify {
                zfsStorageProvider.cloneVolumeSet(commitVs, commit.id, newVs)
                zfsStorageProvider.cloneVolume(commitVs, commit.id, newVs, "volume")
            }
        }

        "create metadata succeeds" {
            transaction {
                providers.metadata.createCommit("foo", vs, Commit("sourceCommit"))
            }
            val (opVs, op) = providers.operations.createMetadata("foo", Operation.Type.PULL,
                    "origin", "id", true, params, "sourceCommit")

            transaction {
                val volumes = providers.metadata.listVolumes(opVs)
                volumes.size shouldBe 1
                volumes[0].name shouldBe "volume"

                providers.metadata.getCommitSource(opVs) shouldBe "sourceCommit"

                op.id shouldBe opVs
                op.commitId shouldBe "id"
                op.remote shouldBe "origin"
            }
        }

        "find local commit returns commit id for push" {
            val commit = providers.operations.findLocalCommit(Operation.Type.PUSH, "foo", Remote("nop", "remote"),
                    params, "id")
            commit shouldBe "id"
        }

        "find local commit returns null if no remote commits exist" {
            every { nopProvider.getCommit(any(), any(), any()) } returns null
            val commit = providers.operations.findLocalCommit(Operation.Type.PULL, "foo", Remote("nop", "remote"),
                    params, "id")
            commit shouldBe null
        }

        "find local commit returns source of remote commit" {
            transaction {
                providers.metadata.createCommit("foo", vs, Commit(id = "one"))
                providers.metadata.createCommit("foo", vs, Commit(id = "two"))
            }
            every { nopProvider.getCommit(any(), any(), "three") } returns mapOf("tags" to mapOf(
                    "source" to "two"))
            every { nopProvider.getCommit(any(), any(), "two") } returns mapOf("tags" to mapOf(
                    "source" to "one"))
            every { nopProvider.getCommit(any(), any(), "one") } returns null
            val commit = providers.operations.findLocalCommit(Operation.Type.PULL, "foo", Remote("nop", "remote"),
                    params, "three")
            commit shouldBe "two"
        }

        "find local commit follows remote chain if local commit is missing" {
            transaction {
                providers.metadata.createCommit("foo", vs, Commit(id = "one"))
            }
            every { nopProvider.getCommit(any(), any(), "three") } returns mapOf("tags" to mapOf(
                    "source" to "two"))
            every { nopProvider.getCommit(any(), any(), "two") } returns mapOf("tags" to mapOf(
                    "source" to "one"))
            every { nopProvider.getCommit(any(), any(), "one") } returns null
            val commit = providers.operations.findLocalCommit(Operation.Type.PULL, "foo", Remote("nop", "remote"),
                    params, "three")
            commit shouldBe "one"
        }

        "find local commit returns null if remote commit doesn't have source tag" {
            every { nopProvider.getCommit(any(), any(), "three") } returns emptyMap()
            val commit = providers.operations.findLocalCommit(Operation.Type.PULL, "foo", Remote("nop", "remote"),
                    params, "three")
            commit shouldBe null
        }

        "pull succeeds" {
            transaction {
                providers.metadata.addRemote("foo", Remote("nop", "remote"))
            }
            every { zfsStorageProvider.activateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.deactivateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { zfsStorageProvider.deleteVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createCommit(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createVolumeSet(any()) } just Runs
            every { nopProvider.syncVolume(any(), any(), any(), any(), any()) } just Runs

            var op = providers.operations.startPull("foo", "remote", "commit", params)
            op.commitId shouldBe "commit"
            op.type shouldBe Operation.Type.PULL
            op.remote shouldBe "remote"
            providers.operations.getExecutor("foo", op.id).join()

            op = providers.operations.getOperation("foo", op.id)
            op.state shouldBe Operation.State.COMPLETE

            val progress = providers.operations.getProgress("foo", op.id)
            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[1].type shouldBe ProgressEntry.Type.COMPLETE

            shouldThrow<NoSuchObjectException> {
                providers.operations.getOperation("foo", op.id)
            }
        }

        "error during pull is reported correctly" {
            transaction {
                providers.metadata.addRemote("foo", Remote("nop", "remote"))
            }
            every { zfsStorageProvider.createVolumeSet(any()) } just Runs
            every { zfsStorageProvider.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { nopProvider.startOperation(any()) } throws Exception("error")

            var op = providers.operations.startPull("foo", "remote", "commit", params)
            providers.operations.getExecutor("foo", op.id).join()

            op = providers.operations.getOperation("foo", op.id)
            op.state shouldBe Operation.State.FAILED

            val progress = providers.operations.getProgress("foo", op.id)
            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Pulling commit from 'remote'"
            progress[1].type shouldBe ProgressEntry.Type.FAILED
            progress[1].message shouldBe "error"
        }

        "interrupt during pull is reported correctly" {
            transaction {
                providers.metadata.addRemote("foo", Remote("nop", "remote"))
            }
            every { zfsStorageProvider.createVolumeSet(any()) } just Runs
            every { zfsStorageProvider.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { nopProvider.startOperation(any()) } throws InterruptedException()

            var op = providers.operations.startPull("foo", "remote", "commit", params)
            providers.operations.getExecutor("foo", op.id).join()

            op = providers.operations.getOperation("foo", op.id)
            op.state shouldBe Operation.State.ABORTED

            val progress = providers.operations.getProgress("foo", op.id)
            progress.size shouldBe 2
            progress[1].type shouldBe ProgressEntry.Type.ABORT
        }

        "pull fails if conflicting operation is in progress" {
            transaction {
                providers.metadata.addRemote("foo", Remote("nop", "remote"))
                providers.metadata.createOperation("foo", vs, buildOperation())
            }
            shouldThrow<ObjectExistsException> {
                providers.operations.startPull("foo", "remote", "id", params)
            }
        }

        "pull succeeds if non-conflicting operation is in progress" {
            transaction {
                providers.metadata.addRemote("foo", Remote("nop", "remote"))
                providers.metadata.createOperation("foo", vs, buildOperation(Operation.Type.PUSH))
            }
            every { zfsStorageProvider.activateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.deactivateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { zfsStorageProvider.deleteVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createCommit(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createVolumeSet(any()) } just Runs

            var op = providers.operations.startPull("foo", "remote", "commit", params)
            providers.operations.getExecutor("foo", op.id).join()
            op = providers.operations.getOperation("foo", op.id)
            op.state shouldBe Operation.State.COMPLETE
        }

        "push succeeds" {
            transaction {
                providers.metadata.addRemote("foo", Remote("nop", "remote"))
                providers.metadata.createCommit("foo", vs, Commit("id"))
            }

            every { zfsStorageProvider.activateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.deactivateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { zfsStorageProvider.deleteVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createCommit(any(), any(), any()) } just Runs
            every { zfsStorageProvider.cloneVolumeSet(any(), any(), any()) } just Runs
            every { zfsStorageProvider.cloneVolume(any(), any(), any(), any()) } returns mapOf("mountpoint" to "/mountpoint")

            var op = providers.operations.startPush("foo", "remote", "id", params)
            op.commitId shouldBe "id"
            op.type shouldBe Operation.Type.PUSH
            op.remote shouldBe "remote"
            providers.operations.getExecutor("foo", op.id).join()

            op = providers.operations.getOperation("foo", op.id)
            op.state shouldBe Operation.State.COMPLETE

            val progress = providers.operations.getProgress("foo", op.id)
            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Pushing id to 'remote'"
            progress[1].type shouldBe ProgressEntry.Type.COMPLETE

            shouldThrow<NoSuchObjectException> {
                providers.operations.getOperation("foo", op.id)
            }
        }

        "error during push is reported correctly" {
            transaction {
                providers.metadata.addRemote("foo", Remote("nop", "remote"))
                providers.metadata.createCommit("foo", vs, Commit("id"))
            }
            every { zfsStorageProvider.cloneVolumeSet(any(), any(), any()) } just Runs
            every { zfsStorageProvider.cloneVolume(any(), any(), any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { nopProvider.startOperation(any()) } throws Exception("error")

            var op = providers.operations.startPush("foo", "remote", "id", params)
            providers.operations.getExecutor("foo", op.id).join()

            op = providers.operations.getOperation("foo", op.id)
            op.state shouldBe Operation.State.FAILED

            val progress = providers.operations.getProgress("foo", op.id)
            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Pushing id to 'remote'"
            progress[1].type shouldBe ProgressEntry.Type.FAILED
            progress[1].message shouldBe "error"
        }

        "interrupt during push is reported correctly" {
            transaction {
                providers.metadata.addRemote("foo", Remote("nop", "remote"))
                providers.metadata.createCommit("foo", vs, Commit("id"))
            }
            every { zfsStorageProvider.cloneVolumeSet(any(), any(), any()) } just Runs
            every { zfsStorageProvider.cloneVolume(any(), any(), any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { nopProvider.startOperation(any()) } throws InterruptedException()

            var op = providers.operations.startPush("foo", "remote", "id", params)
            providers.operations.getExecutor("foo", op.id).join()

            op = providers.operations.getOperation("foo", op.id)
            op.state shouldBe Operation.State.ABORTED

            val progress = providers.operations.getProgress("foo", op.id)
            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Pushing id to 'remote'"
            progress[1].type shouldBe ProgressEntry.Type.ABORT
        }

        "push fails if conflicting operation is in progress" {
            transaction {
                providers.metadata.addRemote("foo", Remote("nop", "remote"))
                providers.metadata.createOperation("foo", vs, buildOperation(Operation.Type.PUSH))
                providers.metadata.createCommit("foo", vs, Commit("id"))
            }
            shouldThrow<ObjectExistsException> {
                providers.operations.startPush("foo", "remote", "id", params)
            }
        }

        "push succeeds if non-conflicting operation is in progress" {
            transaction {
                providers.metadata.addRemote("foo", Remote("nop", "remote"))
                providers.metadata.createOperation("foo", vs, buildOperation())
                providers.metadata.createCommit("foo", vs, Commit("id"))
            }

            every { zfsStorageProvider.activateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.deactivateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { zfsStorageProvider.deleteVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createCommit(any(), any(), any()) } just Runs
            every { zfsStorageProvider.cloneVolumeSet(any(), any(), any()) } just Runs
            every { zfsStorageProvider.cloneVolume(any(), any(), any(), any()) } returns mapOf("mountpoint" to "/mountpoint")

            var op = providers.operations.startPush("foo", "remote", "id", params)
            providers.operations.getExecutor("foo", op.id).join()
            op = providers.operations.getOperation("foo", op.id)
            op.state shouldBe Operation.State.COMPLETE
        }

        "load state restarts operation" {
            transaction {
                providers.metadata.addRemote("foo", Remote("nop", "remote"))
                providers.metadata.createOperation("foo", vs, buildOperation(Operation.Type.PUSH))
                providers.metadata.createCommit("foo", vs, Commit("id"))
            }

            every { zfsStorageProvider.activateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.deactivateVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { zfsStorageProvider.deleteVolume(any(), any(), any()) } just Runs
            every { zfsStorageProvider.createCommit(any(), any(), any()) } just Runs
            every { zfsStorageProvider.cloneVolumeSet(any(), any(), any()) } just Runs
            every { zfsStorageProvider.cloneVolume(any(), any(), any(), any()) } returns mapOf("mountpoint" to "/mountpoint")

            providers.operations.loadState()
            providers.operations.getExecutor("foo", vs).join()

            var op = providers.operations.getOperation("foo", vs)
            op.state shouldBe Operation.State.COMPLETE

            val progress = providers.operations.getProgress("foo", op.id)
            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Retrying operation after restart"
            progress[1].type shouldBe ProgressEntry.Type.COMPLETE
        }
    }
}
