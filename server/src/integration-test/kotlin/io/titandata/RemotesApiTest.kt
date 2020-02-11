/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata

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
import io.mockk.mockk
import io.titandata.models.Remote
import io.titandata.models.Repository
import org.jetbrains.exposed.sql.transactions.transaction

@UseExperimental(KtorExperimentalAPI::class)
class RemotesApiTest : StringSpec() {

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

    /*
     * Note that docker explicitly doesn't set the Content-Type header to application/json, so
     * we want to make sure that we respond correctly even when this header isn't set.
     */
    init {
        "get empty remote list succeeds" {
            transaction {
                services.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
            }
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/repo/remotes")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[]"
            }
        }

        "get remote list succeeds" {
            transaction {
                services.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
                services.metadata.addRemote("repo", Remote("nop", "foo"))
                services.metadata.addRemote("repo", Remote("engine", "bar", mapOf("address" to "a", "username" to "u", "password" to "p", "repository" to "r")))
            }
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/repo/remotes")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[{\"provider\":\"nop\",\"name\":\"foo\",\"properties\":{}}," +
                        "{\"provider\":\"engine\",\"name\":\"bar\",\"properties\":{\"address\":\"a\"," +
                        "\"username\":\"u\",\"password\":\"p\",\"repository\":\"r\"}}]"
            }
        }

        "create remote succeeds" {
            transaction {
                services.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
            }
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"nop\",\"name\":\"a\",\"properties\":{}}")
            }) {
                response.status() shouldBe HttpStatusCode.Created
                response.content shouldBe "{\"provider\":\"nop\",\"name\":\"a\",\"properties\":{}}"
            }
        }

        "add duplicate remote fails" {
            transaction {
                services.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
                services.metadata.addRemote("repo", Remote("nop", "a"))
            }
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"nop\",\"name\":\"a\",\"properties\":{}}")
            }) {
                response.status() shouldBe HttpStatusCode.Conflict
            }
        }

        "update non-existent remote fails" {
            transaction {
                services.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
            }
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes/foo") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"nop\",\"name\":\"a\",\"properties\":{}}")
            }) {
                response.status() shouldBe HttpStatusCode.NotFound
            }
        }

        "update remote succeeds" {
            transaction {
                services.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
                services.metadata.addRemote("repo", Remote("nop", "foo"))
                services.metadata.addRemote("repo", Remote("s3", "bar", mapOf("bucket" to "bucket")))
            }
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes/bar") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"s3\",\"name\":\"bar\",\"properties\":{\"bucket\":\"bocket\"}}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "{\"provider\":\"s3\",\"name\":\"bar\",\"properties\":{\"bucket\":\"bocket\"}}"
            }
        }

        "rename remote succeeds" {
            transaction {
                services.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
                services.metadata.addRemote("repo", Remote("nop", "foo"))
                services.metadata.addRemote("repo", Remote("nop", "bar"))
            }
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes/bar") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"nop\",\"name\":\"baz\",\"properties\":{}}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
            }
        }

        "rename remote to existing name fails" {
            transaction {
                services.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
                services.metadata.addRemote("repo", Remote("nop", "foo"))
                services.metadata.addRemote("repo", Remote("nop", "bar"))
            }
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes/bar") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"nop\",\"name\":\"foo\",\"properties\":{}}")
            }) {
                response.status() shouldBe HttpStatusCode.Conflict
            }
        }

        "delete remote succeeds" {
            transaction {
                services.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
                services.metadata.addRemote("repo", Remote("nop", "foo"))
                services.metadata.addRemote("repo", Remote("nop", "bar"))
            }
            with(engine.handleRequest(HttpMethod.Delete, "/v1/repositories/repo/remotes/bar")) {
                response.status() shouldBe HttpStatusCode.NoContent
            }
        }

        "list remote commits succeeds" {
            transaction {
                services.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
                services.metadata.addRemote("repo", Remote("nop", "foo"))
            }
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/repo/remotes/foo/commits") {
                addHeader("titan-remote-parameters", "{\"provider\":\"nop\",\"properties\":{}}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[]"
            }
        }

        "get remote commit succeeds" {
            transaction {
                services.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
                services.metadata.addRemote("repo", Remote("nop", "foo"))
            }
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/repo/remotes/foo/commits/c") {
                addHeader("titan-remote-parameters", "{\"provider\":\"nop\",\"properties\":{}}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"id\":\"c\",\"properties\":{}}"
            }
        }
    }
}
