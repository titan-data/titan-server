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
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.impl.annotations.OverrideMockKs
import io.mockk.impl.annotations.SpyK
import io.mockk.mockk
import io.mockk.verify
import io.titandata.ServiceLocator
import io.titandata.context.docker.DockerZfsContext
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.metadata.OperationData
import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Repository
import io.titandata.models.Volume
import io.titandata.remote.nop.server.NopRemoteServer
import io.titandata.remote.s3.server.S3RemoteServer
import java.util.UUID
import org.jetbrains.exposed.sql.transactions.transaction

class OperationOrchestratorTest : StringSpec() {

    @MockK(relaxUnitFun = true)
    lateinit var context: DockerZfsContext

    @SpyK
    var s3Provider = S3RemoteServer()

    @SpyK
    var nopProvider = NopRemoteServer()

    lateinit var vs: String

    @InjectMockKs
    @OverrideMockKs
    var services = ServiceLocator(mockk())

    override fun beforeSpec(spec: Spec) {
        services.metadata.init()
    }

    override fun beforeTest(testCase: TestCase) {
        services.metadata.clear()
        vs = transaction {
            services.metadata.createRepository(Repository("foo"))
            val vs = services.metadata.createVolumeSet("foo", null, true)
            services.metadata.createVolume(vs, Volume("volume", config = mapOf("mountpoint" to "/mountpoint")))
            vs
        }
        MockKAnnotations.init(this)
        services.setRemoteProvider("nop", nopProvider)
        services.setRemoteProvider("s3", s3Provider)
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
        ), params = params, metadataOnly = false, repo = "foo")
    }

    init {
        "get operation succeeds" {
            transaction {
                services.metadata.createOperation("foo", vs, buildOperation())
                val op = services.operations.getOperation(vs)
                op.id shouldBe vs
                op.remote shouldBe "remote"
                op.commitId shouldBe "id"
            }
        }

        "get operation fails for invalid operation id" {
            shouldThrow<IllegalArgumentException> {
                services.operations.getOperation("bad/id")
            }
        }

        "get operation fails for non-existent repository" {
            shouldThrow<NoSuchObjectException> {
                services.operations.getOperation(UUID.randomUUID().toString())
            }
        }

        "get operation fails for non-existent operation" {
            shouldThrow<NoSuchObjectException> {
                services.operations.getOperation(UUID.randomUUID().toString())
            }
        }

        "list operations for non-existent repository fails" {
            shouldThrow<NoSuchObjectException> {
                services.operations.listOperations("bar")
            }
        }

        "list operations fails with invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                services.operations.listOperations("bad/repo")
            }
        }

        "list operations returns empty list" {
            val result = services.operations.listOperations("foo")
            result.size shouldBe 0
        }

        "list operations succeeds" {
            transaction {
                services.metadata.createOperation("foo", vs, buildOperation())
            }
            val result = services.operations.listOperations("foo")
            result.size shouldBe 1
            result[0].id shouldBe vs
            result[0].commitId shouldBe "id"
        }

        "pull fails for invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                services.operations.startPull("bad/repo", "remote", "id", params)
            }
        }

        "pull fails for invalid remote name" {
            shouldThrow<IllegalArgumentException> {
                services.operations.startPull("foo", "bad/remote", "id", params)
            }
        }

        "pull fails for invalid commit id" {
            shouldThrow<IllegalArgumentException> {
                services.operations.startPull("foo", "remote", "bad/id", params)
            }
        }

        "pull fails for non-existent repo" {
            shouldThrow<NoSuchObjectException> {
                services.operations.startPull("bar", "remote", "id", params)
            }
        }

        "pull for non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                services.operations.startPull("foo", "remote", "id", params)
            }
        }

        "pull fails for mismatched remote fails" {
            services.remotes.addRemote("foo", Remote("nop", "remote"))
            shouldThrow<IllegalArgumentException> {
                services.operations.startPull("foo", "remote", "commit", RemoteParameters("s3"))
            }
        }

        "pull fails for invalid remote parameters" {
            services.remotes.addRemote("foo", Remote("nop", "remote"))
            shouldThrow<IllegalArgumentException> {
                services.operations.startPull("foo", "remote", "commit", RemoteParameters("nop", mapOf("a" to "b")))
            }
        }

        "pull fails for non-existent remote commit" {
            services.remotes.addRemote("foo", Remote("s3", "remote", mapOf("bucket" to "bucket")))
            every { s3Provider.getCommit(any(), any(), any()) } throws NoSuchObjectException("")
            shouldThrow<NoSuchObjectException> {
                services.operations.startPull("foo", "remote", "id", RemoteParameters("s3", mapOf("accessKey" to "key", "secretKey" to "key")))
            }
        }

        "pull fails if local commit exists" {
            services.remotes.addRemote("foo", Remote("nop", "remote"))
            transaction {
                services.metadata.createCommit("foo", services.metadata.getActiveVolumeSet("foo"), Commit(id = "id"))
            }
            shouldThrow<ObjectExistsException> {
                services.operations.startPull("foo", "remote", "id", params)
            }
        }

        "pull fails if local commit does not exist and metadata only set" {
            services.remotes.addRemote("foo", Remote("nop", "remote"))
            shouldThrow<NoSuchObjectException> {
                services.operations.startPull("foo", "remote", "id", params, true)
            }
        }

        "push fails for invalid repo name" {
            shouldThrow<IllegalArgumentException> {
                services.operations.startPush("bad/repo", "remote", "id", params)
            }
        }

        "push fails for invalid remote name" {
            shouldThrow<IllegalArgumentException> {
                services.operations.startPush("foo", "bad/remote", "id", params)
            }
        }

        "push fails for invalid commit id" {
            shouldThrow<IllegalArgumentException> {
                services.operations.startPush("foo", "remote", "bad/id", params)
            }
        }

        "push fails for non-existent repo" {
            shouldThrow<NoSuchObjectException> {
                services.operations.startPush("bar", "remote", "id", params)
            }
        }

        "push for non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                services.operations.startPush("foo", "remote", "id", params)
            }
        }

        "push fails for mismatched remote fails" {
            services.remotes.addRemote("foo", Remote("nop", "remote"))
            shouldThrow<IllegalArgumentException> {
                services.operations.startPush("foo", "remote", "id", RemoteParameters("s3"))
            }
        }

        "push fails for invalid remote configuration" {
            services.remotes.addRemote("foo", Remote("nop", "remote"))
            shouldThrow<IllegalArgumentException> {
                services.operations.startPush("foo", "remote", "id", RemoteParameters("nop", mapOf("foo" to "bar")))
            }
        }

        "push fails if local commit cannot be found" {
            services.remotes.addRemote("foo", Remote("nop", "remote"))
            shouldThrow<NoSuchObjectException> {
                services.operations.startPush("foo", "remote", "id", params)
            }
        }

        "push fails if remote commit exists" {
            services.remotes.addRemote("foo", Remote("s3", "remote", mapOf("bucket" to "bucket")))
            transaction {
                services.metadata.createCommit("foo", services.metadata.getActiveVolumeSet("foo"), Commit(id = "id"))
            }
            every { s3Provider.getCommit(any(), any(), any()) } returns emptyMap()
            shouldThrow<ObjectExistsException> {
                services.operations.startPush("foo", "remote", "id", RemoteParameters("s3", mapOf("accessKey" to "key", "secretKey" to "key")))
            }
        }

        "abort operation fails for bad operation name" {
            shouldThrow<IllegalArgumentException> {
                services.operations.abortOperation("badid")
            }
        }

        "abort operation fails for unknown operation" {
            shouldThrow<NoSuchObjectException> {
                services.operations.abortOperation(UUID.randomUUID().toString())
            }
        }

        "create storage with no commit creates new volume set" {
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint2")

            val newVolumeSet = transaction {
                val vs = services.metadata.createVolumeSet("foo")
                services.metadata.createVolume(vs, Volume(name = "volume"))
                vs
            }

            services.operations.createStorage("foo", newVolumeSet, null)

            val vol = transaction {
                services.metadata.getVolume(newVolumeSet, "volume")
            }
            vol.config["mountpoint"] shouldBe "/mountpoint2"

            verify {
                context.createVolumeSet(newVolumeSet)
                context.createVolume(newVolumeSet, "volume")
            }
        }

        "create storage with existing commit clones volume set" {
            every { context.cloneVolume(any(), any(), any(), any(), any()) } returns mapOf("mountpoint" to "/mountpoint2")

            val (commitVs, commit) = transaction {
                val cvs = services.metadata.createVolumeSet("foo")
                services.metadata.createVolume(cvs, Volume("volume"))
                services.metadata.createCommit("foo", cvs, Commit("id"))
                services.metadata.getCommit("foo", "id")
            }

            val newVs = transaction {
                val vs = services.metadata.createVolumeSet("foo")
                services.metadata.createVolume(vs, Volume("volume"))
                vs
            }

            services.operations.createStorage("foo", newVs, commit.id)

            val vol = transaction {
                services.metadata.getVolume(newVs, "volume")
            }
            vol.config["mountpoint"] shouldBe "/mountpoint2"

            verify {
                context.cloneVolumeSet(commitVs, commit.id, newVs)
                context.cloneVolume(commitVs, commit.id, newVs, "volume", emptyMap())
            }
        }

        "create metadata succeeds" {
            transaction {
                services.metadata.createCommit("foo", vs, Commit("sourceCommit"))
            }
            val (opVs, op) = services.operations.createMetadata("foo", Operation.Type.PULL,
                    "origin", "id", true, params, "sourceCommit")

            transaction {
                val volumes = services.metadata.listVolumes(opVs)
                volumes.size shouldBe 1
                volumes[0].name shouldBe "volume"

                services.metadata.getCommitSource(opVs) shouldBe "sourceCommit"

                op.id shouldBe opVs
                op.commitId shouldBe "id"
                op.remote shouldBe "origin"
            }
        }

        "find local commit returns commit id for push" {
            val commit = services.operations.findLocalCommit(Operation.Type.PUSH, "foo", Remote("nop", "remote"),
                    params, "id")
            commit shouldBe "id"
        }

        "find local commit returns null if no remote commits exist" {
            every { nopProvider.getCommit(any(), any(), any()) } returns null
            val commit = services.operations.findLocalCommit(Operation.Type.PULL, "foo", Remote("nop", "remote"),
                    params, "id")
            commit shouldBe null
        }

        "find local commit returns source of remote commit" {
            transaction {
                services.metadata.createCommit("foo", vs, Commit(id = "one"))
                services.metadata.createCommit("foo", vs, Commit(id = "two"))
            }
            every { nopProvider.getCommit(any(), any(), "three") } returns mapOf("tags" to mapOf(
                    "source" to "two"))
            every { nopProvider.getCommit(any(), any(), "two") } returns mapOf("tags" to mapOf(
                    "source" to "one"))
            every { nopProvider.getCommit(any(), any(), "one") } returns null
            val commit = services.operations.findLocalCommit(Operation.Type.PULL, "foo", Remote("nop", "remote"),
                    params, "three")
            commit shouldBe "two"
        }

        "find local commit follows remote chain if local commit is missing" {
            transaction {
                services.metadata.createCommit("foo", vs, Commit(id = "one"))
            }
            every { nopProvider.getCommit(any(), any(), "three") } returns mapOf("tags" to mapOf(
                    "source" to "two"))
            every { nopProvider.getCommit(any(), any(), "two") } returns mapOf("tags" to mapOf(
                    "source" to "one"))
            every { nopProvider.getCommit(any(), any(), "one") } returns null
            val commit = services.operations.findLocalCommit(Operation.Type.PULL, "foo", Remote("nop", "remote"),
                    params, "three")
            commit shouldBe "one"
        }

        "find local commit returns null if remote commit doesn't have source tag" {
            every { nopProvider.getCommit(any(), any(), "three") } returns emptyMap()
            val commit = services.operations.findLocalCommit(Operation.Type.PULL, "foo", Remote("nop", "remote"),
                    params, "three")
            commit shouldBe null
        }

        "pull succeeds" {
            transaction {
                services.metadata.addRemote("foo", Remote("nop", "remote"))
            }
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")

            var op = services.operations.startPull("foo", "remote", "commit", params)
            op.commitId shouldBe "commit"
            op.type shouldBe Operation.Type.PULL
            op.remote shouldBe "remote"
            services.operations.waitForComplete(op.id)

            op = services.operations.getOperation(op.id)
            op.state shouldBe Operation.State.COMPLETE

            val progress = services.operations.getProgress(op.id)
            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[1].type shouldBe ProgressEntry.Type.COMPLETE
        }

        "error during pull is reported correctly" {
            transaction {
                services.metadata.addRemote("foo", Remote("nop", "remote"))
            }
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.syncVolumes(any(), any(), any(), any()) } throws Exception("error")

            var op = services.operations.startPull("foo", "remote", "commit", params)
            services.operations.waitForComplete(op.id)

            op = services.operations.getOperation(op.id)
            op.state shouldBe Operation.State.FAILED

            val progress = services.operations.getProgress(op.id)
            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Pulling commit from 'remote'"
            progress[1].type shouldBe ProgressEntry.Type.FAILED
            progress[1].message shouldBe "error"
        }

        "interrupt during pull is reported correctly" {
            transaction {
                services.metadata.addRemote("foo", Remote("nop", "remote"))
            }
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.syncVolumes(any(), any(), any(), any()) } throws InterruptedException()

            var op = services.operations.startPull("foo", "remote", "commit", params)
            services.operations.waitForComplete(op.id)

            op = services.operations.getOperation(op.id)
            op.state shouldBe Operation.State.ABORTED

            val progress = services.operations.getProgress(op.id)
            progress.size shouldBe 2
            progress[1].type shouldBe ProgressEntry.Type.ABORT
        }

        "pull fails if conflicting operation is in progress" {
            transaction {
                services.metadata.addRemote("foo", Remote("nop", "remote"))
                services.metadata.createOperation("foo", vs, buildOperation())
            }
            shouldThrow<ObjectExistsException> {
                services.operations.startPull("foo", "remote", "id", params)
            }
        }

        "pull succeeds if non-conflicting operation is in progress" {
            transaction {
                services.metadata.addRemote("foo", Remote("nop", "remote"))
                services.metadata.createOperation("foo", vs, buildOperation(Operation.Type.PUSH))
            }
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")

            var op = services.operations.startPull("foo", "remote", "commit", params)
            services.operations.waitForComplete(op.id)
            op = services.operations.getOperation(op.id)
            op.state shouldBe Operation.State.COMPLETE
        }

        "push succeeds" {
            transaction {
                services.metadata.addRemote("foo", Remote("nop", "remote"))
                services.metadata.createCommit("foo", vs, Commit("id"))
            }

            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.cloneVolume(any(), any(), any(), any(), any()) } returns mapOf("mountpoint" to "/mountpoint")

            var op = services.operations.startPush("foo", "remote", "id", params)
            op.commitId shouldBe "id"
            op.type shouldBe Operation.Type.PUSH
            op.remote shouldBe "remote"
            services.operations.waitForComplete(op.id)

            op = services.operations.getOperation(op.id)
            op.state shouldBe Operation.State.COMPLETE

            val progress = services.operations.getProgress(op.id)
            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Pushing id to 'remote'"
            progress[1].type shouldBe ProgressEntry.Type.COMPLETE
        }

        "error during push is reported correctly" {
            transaction {
                services.metadata.addRemote("foo", Remote("nop", "remote"))
                services.metadata.createCommit("foo", vs, Commit("id"))
            }
            every { context.cloneVolume(any(), any(), any(), any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.syncVolumes(any(), any(), any(), any()) } throws Exception("error")
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint2")

            var op = services.operations.startPush("foo", "remote", "id", params)
            services.operations.waitForComplete(op.id)

            op = services.operations.getOperation(op.id)
            op.state shouldBe Operation.State.FAILED

            val progress = services.operations.getProgress(op.id)
            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Pushing id to 'remote'"
            progress[1].type shouldBe ProgressEntry.Type.FAILED
            progress[1].message shouldBe "error"
        }

        "interrupt during push is reported correctly" {
            transaction {
                services.metadata.addRemote("foo", Remote("nop", "remote"))
                services.metadata.createCommit("foo", vs, Commit("id"))
            }
            every { context.cloneVolume(any(), any(), any(), any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.syncVolumes(any(), any(), any(), any()) } throws InterruptedException()
            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint2")

            var op = services.operations.startPush("foo", "remote", "id", params)
            services.operations.waitForComplete(op.id)
            op = services.operations.getOperation(op.id)
            op.state shouldBe Operation.State.ABORTED

            val progress = services.operations.getProgress(op.id)
            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Pushing id to 'remote'"
            progress[1].type shouldBe ProgressEntry.Type.ABORT
        }

        "push fails if conflicting operation is in progress" {
            transaction {
                services.metadata.addRemote("foo", Remote("nop", "remote"))
                services.metadata.createOperation("foo", vs, buildOperation(Operation.Type.PUSH))
                services.metadata.createCommit("foo", vs, Commit("id"))
            }
            shouldThrow<ObjectExistsException> {
                services.operations.startPush("foo", "remote", "id", params)
            }
        }

        "push succeeds if non-conflicting operation is in progress" {
            transaction {
                services.metadata.addRemote("foo", Remote("nop", "remote"))
                services.metadata.createOperation("foo", vs, buildOperation())
                services.metadata.createCommit("foo", vs, Commit("id"))
            }

            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.cloneVolume(any(), any(), any(), any(), any()) } returns mapOf("mountpoint" to "/mountpoint")

            var op = services.operations.startPush("foo", "remote", "id", params)
            services.operations.waitForComplete(op.id)
            op = services.operations.getOperation(op.id)
            op.state shouldBe Operation.State.COMPLETE
        }

        "load state restarts operation" {
            transaction {
                services.metadata.addRemote("foo", Remote("nop", "remote"))
                services.metadata.createOperation("foo", vs, buildOperation(Operation.Type.PUSH))
                services.metadata.createCommit("foo", vs, Commit("id"))
            }

            every { context.createVolume(any(), any()) } returns mapOf("mountpoint" to "/mountpoint")
            every { context.cloneVolume(any(), any(), any(), any(), any()) } returns mapOf("mountpoint" to "/mountpoint")

            services.operations.loadState()
            services.operations.waitForComplete(vs)

            var op = services.operations.getOperation(vs)
            op.state shouldBe Operation.State.COMPLETE

            val progress = services.operations.getProgress(op.id)
            progress.size shouldBe 2
            progress[0].type shouldBe ProgressEntry.Type.MESSAGE
            progress[0].message shouldBe "Retrying operation after restart"
            progress[1].type shouldBe ProgressEntry.Type.COMPLETE
        }
    }
}
