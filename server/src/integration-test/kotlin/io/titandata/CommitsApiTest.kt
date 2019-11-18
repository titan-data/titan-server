/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata

import com.google.gson.Gson
import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
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
import io.mockk.verify
import io.titandata.context.docker.DockerZfsContext
import io.titandata.exception.NoSuchObjectException
import io.titandata.models.Commit
import io.titandata.models.CommitStatus
import io.titandata.models.Error
import io.titandata.models.Repository
import java.util.concurrent.TimeUnit
import org.jetbrains.exposed.sql.transactions.transaction

@UseExperimental(KtorExperimentalAPI::class)
class CommitsApiTest : StringSpec() {

    lateinit var vs: String

    @MockK
    lateinit var dockerZfsContext: DockerZfsContext

    @InjectMockKs
    @OverrideMockKs
    var providers = ProviderModule("test")

    var engine = TestApplicationEngine(createTestEnvironment())

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
        providers.metadata.clear()
        vs = transaction {
            providers.metadata.createRepository(Repository("foo"))
            providers.metadata.createVolumeSet("foo", null, true)
        }
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    init {
        "list empty commits succeeds" {
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/commits")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[]"
            }
        }

        "list commits succeeds" {
            transaction {
                providers.metadata.createCommit("foo", vs, Commit(id = "hash1", properties = mapOf("a" to "b", "timestamp" to "2019-09-20T13:45:38Z")))
                providers.metadata.createCommit("foo", vs, Commit(id = "hash2", properties = mapOf("c" to "d", "timestamp" to "2019-09-20T13:45:37Z")))
            }
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/commits")) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "[{\"id\":\"hash1\",\"properties\":{\"a\":\"b\",\"timestamp\":\"2019-09-20T13:45:38Z\"}},{\"id\":\"hash2\",\"properties\":{\"c\":\"d\",\"timestamp\":\"2019-09-20T13:45:37Z\"}}]"
            }
        }
        "list commits filters result with exact match" {
            transaction {
                providers.metadata.createCommit("foo", vs, Commit(id = "hash1", properties = mapOf("tags" to mapOf("a" to "b"))))
                providers.metadata.createCommit("foo", vs, Commit(id = "hash2", properties = mapOf("tags" to mapOf("c" to "d"))))
            }
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/commits?tag=a=b")) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "[{\"id\":\"hash1\",\"properties\":{\"tags\":{\"a\":\"b\"}}}]"
            }
        }

        "list commits filters result with exists match" {
            transaction {
                providers.metadata.createCommit("foo", vs, Commit(id = "hash1", properties = mapOf("tags" to mapOf("a" to "b"))))
                providers.metadata.createCommit("foo", vs, Commit(id = "hash2", properties = mapOf("tags" to mapOf("c" to "d"))))
            }
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/commits?tag=a")) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "[{\"id\":\"hash1\",\"properties\":{\"tags\":{\"a\":\"b\"}}}]"
            }
        }

        "list commits filters result with compound match" {
            transaction {
                providers.metadata.createCommit("foo", vs, Commit(id = "hash1", properties = mapOf("tags" to mapOf("a" to "b", "c" to "d"))))
                providers.metadata.createCommit("foo", vs, Commit(id = "hash2", properties = mapOf("tags" to mapOf("c" to "d"))))
            }
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/commits?tag=a=b&tag=c=d")) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "[{\"id\":\"hash1\",\"properties\":{\"tags\":{\"a\":\"b\",\"c\":\"d\"}}}]"
            }
        }

        "list commits fails with non existent repository" {
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/repo/commits")) {
                response.status() shouldBe HttpStatusCode.NotFound
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "NoSuchObjectException"
                error.message shouldBe "no such repository 'repo'"
            }
        }

        "get commit succeeds" {
            transaction {
                providers.metadata.createCommit("foo", vs, Commit(id = "hash", properties = mapOf("a" to "b")))
            }
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/commits/hash")) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "{\"id\":\"hash\",\"properties\":{\"a\":\"b\"}}"
            }
        }

        "get commit status succeeds" {
            transaction {
                providers.metadata.createCommit("foo", vs, Commit("hash"))
            }
            every { dockerZfsContext.getCommitStatus(any(), any(), any()) } returns CommitStatus(logicalSize = 3, actualSize = 6, uniqueSize = 9)
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/commits/hash/status")) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "{\"logicalSize\":3,\"actualSize\":6,\"uniqueSize\":9}"
            }
        }

        "get commit from non-existent repo returns no such object" {
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/bar/commits/hash")) {
                response.status() shouldBe HttpStatusCode.NotFound
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "NoSuchObjectException"
                error.message shouldBe "no such repository 'bar'"
            }
        }

        "get non-existent commit returns no such object" {
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/commits/hash")) {
                response.status() shouldBe HttpStatusCode.NotFound
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "NoSuchObjectException"
                error.message shouldBe "no such commit 'hash' in repository 'foo'"
            }
        }

        "get bad commit id returns bad request" {
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/commits/bad@hash")) {
                response.status() shouldBe HttpStatusCode.BadRequest
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "IllegalArgumentException"
                error.message shouldBe "invalid commit id, can only contain alphanumeric characters, '-', ':', '.', or '_'"
            }
        }

        "update commit succeeds" {
            transaction {
                providers.metadata.createCommit("foo", vs, Commit("hash"))
            }
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/foo/commits/hash") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"id\":\"hash\",\"properties\":{\"a\":\"b\"}}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                transaction {
                    val commit = providers.metadata.getCommit("foo", "hash").second
                    commit.properties["a"] shouldBe "b"
                }
            }
        }

        "delete commit succeeds" {
            transaction {
                providers.metadata.createCommit("foo", vs, Commit("hash"))
            }
            with(engine.handleRequest(HttpMethod.Delete, "/v1/repositories/foo/commits/hash")) {
                response.status() shouldBe HttpStatusCode.NoContent
                shouldThrow<NoSuchObjectException> {
                    transaction {
                        providers.metadata.getCommit("foo", "hash")
                    }
                }
            }
        }

        "delete commit from non-existent repo returns no such object" {
            with(engine.handleRequest(HttpMethod.Delete, "/v1/repositories/bar/commits/hash")) {
                response.status() shouldBe HttpStatusCode.NotFound
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "NoSuchObjectException"
                error.message shouldBe "no such repository 'bar'"
            }
        }

        "delete non-existent commit returns no such object" {
            with(engine.handleRequest(HttpMethod.Delete, "/v1/repositories/foo/commits/hash")) {
                response.status() shouldBe HttpStatusCode.NotFound
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "NoSuchObjectException"
                error.message shouldBe "no such commit 'hash' in repository 'foo'"
            }
        }

        "create commit succeeds" {
            every { dockerZfsContext.createCommit(any(), any(), any()) } just Runs
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/foo/commits") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"id\":\"hash\",\"properties\":{\"a\":\"b\",\"timestamp\":\"2019-04-28T23:04:06Z\"}}")
            }) {
                response.status() shouldBe HttpStatusCode.Created
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"id\":\"hash\",\"properties\":{\"a\":\"b\",\"timestamp\":\"2019-04-28T23:04:06Z\"}}"
                verify {
                    dockerZfsContext.createCommit(vs, "hash", emptyList())
                }
            }
        }

        "create commit in non-existent repo returns no such object" {
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/bar/commits") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"id\":\"hash\",\"properties\":{\"a\":\"b\"}}")
            }) {
                response.status() shouldBe HttpStatusCode.NotFound
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "NoSuchObjectException"
                error.message shouldBe "no such repository 'bar'"
            }
        }

        "checkout commit succeeds" {
            transaction {
                providers.metadata.createCommit("foo", vs, Commit("hash"))
            }

            every { dockerZfsContext.cloneVolumeSet(any(), any(), any()) } just Runs
            every { dockerZfsContext.cloneVolume(any(), any(), any(), any()) } returns emptyMap()

            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/foo/commits/hash/checkout")) {
                response.status() shouldBe HttpStatusCode.NoContent
                val activeVs = transaction {
                    providers.metadata.getActiveVolumeSet("foo")
                }
                activeVs shouldNotBe vs
                verify {
                    dockerZfsContext.cloneVolumeSet(vs, "hash", activeVs)
                }
            }
        }
    }
}
