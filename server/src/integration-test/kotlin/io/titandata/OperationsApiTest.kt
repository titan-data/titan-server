/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.TestApplicationEngine
import io.ktor.server.testing.contentType
import io.ktor.server.testing.createTestEnvironment
import io.ktor.server.testing.handleRequest
import io.ktor.server.testing.setBody
import io.ktor.util.KtorExperimentalAPI
import io.mockk.MockKAnnotations
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.impl.annotations.OverrideMockKs
import io.titandata.models.Error
import io.titandata.models.Operation
import io.titandata.models.Repository
import io.titandata.remote.nop.NopParameters
import io.titandata.remote.nop.NopRemote
import io.titandata.serialization.ModelTypeAdapters
import io.titandata.storage.OperationData
import io.titandata.storage.zfs.ZfsStorageProvider
import io.titandata.util.CommandExecutor
import io.titandata.util.GuidGenerator
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.time.delay
import org.jetbrains.exposed.sql.transactions.transaction

@UseExperimental(KtorExperimentalAPI::class)
class OperationsApiTest : StringSpec() {

    @MockK
    lateinit var executor: CommandExecutor

    @MockK
    lateinit var generator: GuidGenerator

    @InjectMockKs
    @OverrideMockKs
    var zfsStorageProvider = ZfsStorageProvider("test")

    @InjectMockKs
    @OverrideMockKs
    var providers = ProviderModule("test")

    var engine = TestApplicationEngine(createTestEnvironment())

    val gson = ModelTypeAdapters.configure(GsonBuilder()).create()

    override fun beforeSpec(spec: Spec) {
        with(engine) {
            start()
            providers.metadata.init()
            application.mainProvider(providers)
        }
    }

    override fun afterSpec(spec: Spec) {
        engine.stop(0L, 0L, TimeUnit.MILLISECONDS)
    }

    override fun beforeTest(testCase: TestCase) {
        providers.operations.clearState()
        providers.metadata.clear()
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    fun loadOperations(repo: String, vararg operations: OperationData) {
        transaction {
            providers.metadata.createRepository(Repository(name = "foo", properties = mapOf()))
            providers.metadata.addRemote("foo", NopRemote(name = "remote"))
        }
        every { executor.exec(*anyVararg()) } returns ""
        every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:metadata",
                "test/repo/$repo") } returns "test/repo/$repo\t{}"
        every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:metadata",
                "-d", "1", "test/repo") } returns "test/repo/$repo\t{}"
        val lines = operations.map { o -> "test/repo/$repo/${o.operation.id}\t" + gson.toJson(o) }
        every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:operation",
                "-d", "1", "test/repo/foo") } returns lines.joinToString("\n")
        for (o in operations) {
            every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:operation", "test/repo/$repo/${o.operation.id}") } returns
                    "test/repo/$repo/${o.operation.id}\t" + gson.toJson(o)
        }
        every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot",
                "-d", "2", "test/repo/$repo")
        } returns "test/repo/$repo/guid@initial\toff\ntest/repo/$repo/guid@commit\toff\n"
        every { executor.exec("zfs", "list", "-rHo", "name,io.titan-data:metadata", "test/repo/$repo/guid") } returns
                arrayOf("test/repo/$repo/guid\t-", "test/repo/$repo/guid/v0\t{}", "test/repo/$repo/guid/v1\t{}").joinToString("\n")
        every { executor.exec("zfs", "list", "-Ho", "io.titan-data:metadata",
                any()) } returns "{\"a\":\"b\"}\n"
        providers.operations.loadState()
    }

    fun loadTestOperations() {
        val op1 = OperationData(operation = Operation(id = "id1",
                type = Operation.Type.PUSH,
                state = Operation.State.COMPLETE, remote = "remote", commitId = "commit1"),
                params = NopParameters())
        val op2 = OperationData(operation = Operation(id = "id2",
                type = Operation.Type.PULL,
                state = Operation.State.COMPLETE, remote = "remote", commitId = "commit2"),
                params = NopParameters())
        loadOperations("foo", op1, op2)
    }

    init {
        "list empty operations succeeds" {
            transaction {
                providers.metadata.createRepository(Repository(name = "foo", properties = mapOf()))
            }
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/operations")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[]"
            }
        }

        "list operations succeeds" {
            loadTestOperations()
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/operations")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[{\"id\":\"id1\",\"type\":\"PUSH\"," +
                        "\"state\":\"COMPLETE\",\"remote\":\"remote\",\"commitId\":\"commit1\"}," +
                        "{\"id\":\"id2\",\"type\":\"PULL\",\"state\":\"COMPLETE\"," +
                        "\"remote\":\"remote\",\"commitId\":\"commit2\"}]"
            }
        }

        "get operation fails for non-existent operation" {
            every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:metadata",
                    "test/repo/foo") } returns "test/repo/foo\t{}"
            every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:metadata",
                    "-d", "1", "test/repo") } returns "test/repo/foo\t{}"
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/operations/id")) {
                response.status() shouldBe HttpStatusCode.NotFound
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "NoSuchObjectException"
                error.message shouldBe "no such operation 'id' in repository 'foo'"
            }
        }

        "get operation succeeds" {
            loadTestOperations()
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/operations/id1")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"id\":\"id1\",\"type\":\"PUSH\"," +
                    "\"state\":\"COMPLETE\",\"remote\":\"remote\",\"commitId\":\"commit1\"}"
            }
        }

        "abort in-progress operation results in aborted state" {
            loadOperations("foo", OperationData(Operation(id = "id",
                    type = Operation.Type.PUSH, commitId = "commit",
                    state = Operation.State.RUNNING, remote = "remote"),
                    params = NopParameters(delay = 10)))
            with(engine.handleRequest(HttpMethod.Delete, "/v1/repositories/foo/operations/id")) {
                response.status() shouldBe HttpStatusCode.NoContent
            }

            delay(Duration.ofMillis(500))

            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/operations/id")) {
                response.status() shouldBe HttpStatusCode.OK
                val op = gson.fromJson(response.content, Operation::class.java)
                op.state shouldBe Operation.State.ABORTED
            }
        }

        "abort completed operation doesn't alter operation" {
            loadTestOperations()
            with(engine.handleRequest(HttpMethod.Delete, "/v1/repositories/foo/operations/id1")) {
                response.status() shouldBe HttpStatusCode.NoContent
            }

            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/operations/id1")) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "{\"id\":\"id1\",\"type\":\"PUSH\"," +
                        "\"state\":\"COMPLETE\",\"remote\":\"remote\",\"commitId\":\"commit1\"}"
            }
        }

        "get progress returns correct state" {
            loadOperations("foo", OperationData(Operation(id = "id",
                    type = Operation.Type.PUSH, commitId = "commit",
                    state = Operation.State.RUNNING, remote = "remote"),
                    params = NopParameters(delay = 10)))
            delay(Duration.ofMillis(500))
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/operations/id/progress")) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "[{\"type\":\"MESSAGE\",\"message\":\"Retrying operation after restart\"},{\"type\":\"START\",\"message\":\"Running operation\"}]"
            }
        }

        "get progress of completed operation removes operation" {
            loadTestOperations()
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/operations/id1/progress")) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "[]"
            }

            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/operations/id1")) {
                response.status() shouldBe HttpStatusCode.NotFound
            }
        }

        "push starts operation" {
            transaction {
                providers.metadata.createRepository(Repository(name = "foo", properties = mapOf()))
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:metadata",
                    "test/repo/foo") } returns "test/repo/foo\t{}"
            every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:metadata",
                    "-d", "1", "test/repo") } returns "test/repo/foo\t{}"
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t",
                    "snapshot", "-d", "2", "test/repo/foo") } returns
                    "test/repo/foo/guid@commit\toff"
            every { executor.exec("zfs", "list", "-Ho", "io.titan-data:metadata",
                    "test/repo/foo/guid@commit") } returns "{}"

            val result = engine.handleRequest(HttpMethod.Post, "/v1/repositories/foo/remotes/remote/commits/commit/push") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"nop\"}")
            }
            result.response.status() shouldBe HttpStatusCode.OK
            val operation = gson.fromJson(result.response.content, Operation::class.java)

            operation.commitId shouldBe "commit"
            operation.remote shouldBe "remote"
            operation.type shouldBe Operation.Type.PUSH
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/operations/${operation.id}")) {
                response.status() shouldBe HttpStatusCode.OK
            }
        }

        "pull starts operation" {
            transaction {
                providers.metadata.createRepository(Repository(name = "foo", properties = mapOf()))
                providers.metadata.addRemote("foo", NopRemote(name = "remote"))
            }
            every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:metadata",
                    "test/repo/foo") } returns "test/repo/foo\t{}"
            every { executor.exec("zfs", "list", "-Ho", "name,io.titan-data:metadata",
                    "-d", "1", "test/repo") } returns "test/repo/foo\t{}"
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t",
                    "snapshot", "-d", "2", "test/repo/foo") } returns ""

            val result = engine.handleRequest(HttpMethod.Post, "/v1/repositories/foo/remotes/remote/commits/commit/pull") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"nop\"}")
            }
            result.response.status() shouldBe HttpStatusCode.OK
            val operation = gson.fromJson(result.response.content, Operation::class.java)

            operation.commitId shouldBe "commit"
            operation.remote shouldBe "remote"
            operation.type shouldBe Operation.Type.PULL

            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/operations/${operation.id}")) {
                response.status() shouldBe HttpStatusCode.OK
            }
        }
    }
}
