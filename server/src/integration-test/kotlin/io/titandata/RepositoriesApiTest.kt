/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata

import com.google.gson.Gson
import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.matchers.string.shouldContain
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
import io.titandata.models.Error
import io.titandata.models.Repository
import org.jetbrains.exposed.sql.transactions.transaction

@UseExperimental(KtorExperimentalAPI::class)
class RepositoriesApiTest : StringSpec() {

    @MockK
    var context = DockerZfsContext(mapOf("pool" to "test"))

    @InjectMockKs
    @OverrideMockKs
    var services = ServiceLocator(mockk())

    var engine = TestApplicationEngine(createTestEnvironment())

    override fun beforeSpec(spec: Spec) {
        with(engine) {
            start()
            services.metadata.init()
            application.mainProvider(services)
        }
    }

    override fun afterSpec(spec: Spec) {
        engine.stop(0L, 0L)
    }

    override fun beforeTest(testCase: TestCase) {
        services.metadata.clear()
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    init {
        "list empty repositories succeeds" {
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[]"
            }
        }

        "list repositories succeeds" {
            transaction {
                services.metadata.createRepository(Repository(name = "repo1", properties = mapOf("a" to "b")))
                services.metadata.createRepository(Repository(name = "repo2", properties = mapOf()))
            }
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories")) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "[{\"name\":\"repo1\",\"properties\":{\"a\":\"b\"}}," +
                "{\"name\":\"repo2\",\"properties\":{}}]"
            }
        }

        "get repository succeeds" {
            transaction {
                services.metadata.createRepository(Repository(name = "repo", properties = mapOf("a" to "b")))
            }
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/repo")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"name\":\"repo\",\"properties\":{\"a\":\"b\"}}"
            }
        }

        "get unknown repository returns not found" {
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/repo")) {
                response.status() shouldBe HttpStatusCode.NotFound
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "NoSuchObjectException"
                error.message shouldBe "no such repository 'repo'"
            }
        }

        "get bad repository name returns bad request" {
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/bad@name")) {
                response.status() shouldBe HttpStatusCode.BadRequest
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "IllegalArgumentException"
                error.message shouldContain "invalid repository name"
            }
        }

        "get repository status succeeds" {
            transaction {
                services.metadata.createRepository(Repository(name = "foo", properties = emptyMap()))
                services.metadata.createVolumeSet("foo", null, true)
            }
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/status")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{}"
            }
        }

        "delete repository succeeds" {
            transaction {
                services.metadata.createRepository(Repository("repo"))
            }
            with(engine.handleRequest(HttpMethod.Delete, "/v1/repositories/repo")) {
                response.status() shouldBe HttpStatusCode.NoContent
                response.content shouldBe null
            }
        }

        "delete bad repository name returns bad request" {
            with(engine.handleRequest(HttpMethod.Delete, "/v1/repositories/bad@name")) {
                response.status() shouldBe HttpStatusCode.BadRequest
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "IllegalArgumentException"
                error.message shouldContain "invalid repository name"
            }
        }

        "create repository succeeds" {
            every { context.createVolumeSet(any()) } just Runs
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"name\":\"repo\",\"properties\":{\"a\":\"b\"}}")
            }) {
                response.status() shouldBe HttpStatusCode.Created
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"name\":\"repo\",\"properties\":{\"a\":\"b\"}}"
            }
        }

        "create repository fails with bad name" {
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"name\":\"bad@name\",\"properties\":{\"a\":\"b\"}}")
            }) {
                response.status() shouldBe HttpStatusCode.BadRequest
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "IllegalArgumentException"
                error.message shouldContain "invalid repository name"
            }
        }

        "create repository fails with bad json" {
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("-")
            }) {
                response.status() shouldBe HttpStatusCode.BadRequest
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "JsonSyntaxException"
            }
        }

        "update repository succeeds" {
            transaction {
                services.metadata.createRepository(Repository(name = "repo1", properties = mapOf("a" to "b")))
            }
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo1") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"name\":\"repo2\",\"properties\":{\"a\":\"b\"}}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"name\":\"repo2\",\"properties\":{\"a\":\"b\"}}"
            }
        }

        "update repository fails with bad json" {
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("-")
            }) {
                response.status() shouldBe HttpStatusCode.BadRequest
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "JsonSyntaxException"
            }
        }

        "update repository fails with bad name" {
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"name\":\"bad@name\",\"properties\":{\"a\":\"b\"}}")
            }) {
                response.status() shouldBe HttpStatusCode.BadRequest
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "IllegalArgumentException"
                error.message shouldContain "invalid repository name"
            }
        }
    }
}
