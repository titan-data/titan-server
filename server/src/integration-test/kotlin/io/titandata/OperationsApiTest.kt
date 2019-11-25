/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
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
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.impl.annotations.OverrideMockKs
import io.mockk.just
import io.mockk.mockk
import io.titandata.context.docker.DockerZfsContext
import io.titandata.metadata.OperationData
import io.titandata.models.Commit
import io.titandata.models.Error
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Repository
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.time.delay
import org.jetbrains.exposed.sql.transactions.transaction

@UseExperimental(KtorExperimentalAPI::class)
class OperationsApiTest : StringSpec() {

    @MockK
    var context = DockerZfsContext("test")

    lateinit var vs1: String
    lateinit var vs2: String

    @InjectMockKs
    @OverrideMockKs
    var services = ServiceLocator(mockk())

    var engine = TestApplicationEngine(createTestEnvironment())

    val gson = GsonBuilder().create()

    override fun beforeSpec(spec: Spec) {
        with(engine) {
            start()
            services.metadata.init()
            application.mainProvider(services)
        }
    }

    override fun afterSpec(spec: Spec) {
        engine.stop(0L, 0L, TimeUnit.MILLISECONDS)
    }

    override fun beforeTest(testCase: TestCase) {
        services.operations.clearState()
        services.metadata.clear()
        transaction {
            services.metadata.createRepository(Repository(name = "foo", properties = mapOf()))
            services.metadata.addRemote("foo", Remote("nop", "remote"))
            vs1 = services.metadata.createVolumeSet("foo", null, true)
            vs2 = services.metadata.createVolumeSet("foo")
        }
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    fun loadOperation(data: OperationData) {
        transaction {
            services.metadata.createOperation("foo", data.operation.id, data)
        }
    }

    fun loadTestOperations() {
        loadOperation(OperationData(operation = Operation(id = vs1,
                type = Operation.Type.PUSH,
                state = Operation.State.RUNNING, remote = "remote", commitId = "commit1"),
                params = RemoteParameters("nop"), repo = "foo"))
        loadOperation(OperationData(operation = Operation(id = vs2,
                type = Operation.Type.PULL,
                state = Operation.State.RUNNING, remote = "remote", commitId = "commit2"),
                params = RemoteParameters("nop"), repo = "foo"))
    }

    init {
        "list empty operations succeeds" {
            with(engine.handleRequest(HttpMethod.Get, "/v1/operations")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[]"
            }
        }

        "list operations succeeds" {
            loadTestOperations()
            with(engine.handleRequest(HttpMethod.Get, "/v1/operations")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[{\"id\":\"$vs1\",\"type\":\"PUSH\"," +
                        "\"state\":\"RUNNING\",\"remote\":\"remote\",\"commitId\":\"commit1\"}," +
                        "{\"id\":\"$vs2\",\"type\":\"PULL\",\"state\":\"RUNNING\"," +
                        "\"remote\":\"remote\",\"commitId\":\"commit2\"}]"
            }
        }

        "list operations by repo succeeds" {
            loadTestOperations()
            with(engine.handleRequest(HttpMethod.Get, "/v1/operations?repository=foo")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[{\"id\":\"$vs1\",\"type\":\"PUSH\"," +
                        "\"state\":\"RUNNING\",\"remote\":\"remote\",\"commitId\":\"commit1\"}," +
                        "{\"id\":\"$vs2\",\"type\":\"PULL\",\"state\":\"RUNNING\"," +
                        "\"remote\":\"remote\",\"commitId\":\"commit2\"}]"
            }
        }

        "list operations by repo returns empty list" {
            loadTestOperations()
            transaction {
                services.metadata.createRepository(Repository(name = "bar", properties = mapOf()))
            }
            with(engine.handleRequest(HttpMethod.Get, "/v1/operations?repository=bar")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[]"
            }
        }

        "get operation fails for non-existent operation" {
            val id = UUID.randomUUID().toString()
            with(engine.handleRequest(HttpMethod.Get, "/v1/operations/$id")) {
                response.status() shouldBe HttpStatusCode.NotFound
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "NoSuchObjectException"
                error.message shouldBe "no such operation '$id'"
            }
        }

        "get operation succeeds" {
            loadTestOperations()
            with(engine.handleRequest(HttpMethod.Get, "/v1/operations/$vs1")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"id\":\"$vs1\",\"type\":\"PUSH\"," +
                    "\"state\":\"RUNNING\",\"remote\":\"remote\",\"commitId\":\"commit1\"}"
            }
        }

        "abort in-progress operation results in aborted state" {
            transaction {
                services.metadata.createCommit("foo", vs1, Commit("commit"))
            }
            loadOperation(OperationData(Operation(id = vs2,
                    type = Operation.Type.PUSH, commitId = "commit",
                    state = Operation.State.RUNNING, remote = "remote"), repo = "foo",
                    params = RemoteParameters("nop", mapOf("delay" to 10))))
            services.operations.loadState()
            with(engine.handleRequest(HttpMethod.Delete, "/v1/operations/$vs2")) {
                response.status() shouldBe HttpStatusCode.NoContent
            }

            delay(Duration.ofMillis(500))

            with(engine.handleRequest(HttpMethod.Get, "/v1/operations/$vs2")) {
                response.status() shouldBe HttpStatusCode.OK
                val op = gson.fromJson(response.content, Operation::class.java)
                op.state shouldBe Operation.State.ABORTED
            }
        }

        "abort completed operation doesn't alter operation" {
            transaction {
                services.metadata.createCommit("foo", vs1, Commit("commit"))
            }
            loadOperation(OperationData(Operation(id = vs1,
                    type = Operation.Type.PUSH, commitId = "commit",
                    state = Operation.State.COMPLETE, remote = "remote"), repo = "foo",
                    params = RemoteParameters("nop", mapOf("delay" to 10))))
            services.operations.loadState()
            with(engine.handleRequest(HttpMethod.Delete, "/v1/operations/$vs1")) {
                response.status() shouldBe HttpStatusCode.NoContent
            }

            with(engine.handleRequest(HttpMethod.Get, "/v1/operations/$vs1")) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "{\"id\":\"$vs1\",\"type\":\"PUSH\"," +
                        "\"state\":\"COMPLETE\",\"remote\":\"remote\",\"commitId\":\"commit\"}"
            }
        }

        "get resumed progress returns correct state" {
            transaction {
                services.metadata.createCommit("foo", vs1, Commit("commit"))
            }
            loadOperation(OperationData(Operation(id = vs2,
                    type = Operation.Type.PUSH, commitId = "commit",
                    state = Operation.State.RUNNING, remote = "remote"), repo = "foo",
                    params = RemoteParameters("nop", mapOf("delay" to 10))))
            services.operations.loadState()
            delay(Duration.ofMillis(500))
            with(engine.handleRequest(HttpMethod.Get, "/v1/operations/$vs2/progress")) {
                response.status() shouldBe HttpStatusCode.OK
                val entries: List<ProgressEntry> = gson.fromJson(response.content, object : TypeToken<List<ProgressEntry>>() { }.type)
                entries.size shouldBe 1
                entries[0].type shouldBe ProgressEntry.Type.MESSAGE
                entries[0].message shouldBe "Retrying operation after restart"
            }
        }

        "push starts operation" {
            transaction {
                services.metadata.createCommit("foo", vs1, Commit("commit"))
            }

            every { context.cloneVolumeSet(any(), any(), any()) } just Runs
            every { context.cloneVolume(any(), any(), any(), any(), any()) } returns emptyMap()

            val result = engine.handleRequest(HttpMethod.Post, "/v1/repositories/foo/remotes/remote/commits/commit/push") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"nop\",\"properties\":{}}")
            }
            result.response.status() shouldBe HttpStatusCode.OK
            val operation = gson.fromJson(result.response.content, Operation::class.java)

            operation.commitId shouldBe "commit"
            operation.remote shouldBe "remote"
            operation.type shouldBe Operation.Type.PUSH
            with(engine.handleRequest(HttpMethod.Get, "/v1/operations/${operation.id}")) {
                response.status() shouldBe HttpStatusCode.OK
            }
        }

        "pull starts operation" {
            every { context.createVolumeSet(any()) } just Runs

            val result = engine.handleRequest(HttpMethod.Post, "/v1/repositories/foo/remotes/remote/commits/commit/pull") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"nop\",\"properties\":{}}")
            }
            result.response.status() shouldBe HttpStatusCode.OK
            val operation = gson.fromJson(result.response.content, Operation::class.java)

            operation.commitId shouldBe "commit"
            operation.remote shouldBe "remote"
            operation.type shouldBe Operation.Type.PULL

            with(engine.handleRequest(HttpMethod.Get, "/v1/operations/${operation.id}")) {
                response.status() shouldBe HttpStatusCode.OK
            }
        }
    }
}
