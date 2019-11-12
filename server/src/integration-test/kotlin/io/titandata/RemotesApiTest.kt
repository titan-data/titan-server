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
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.OverrideMockKs
import io.titandata.models.Remote
import io.titandata.models.Repository
import io.titandata.remote.engine.EngineRemote
import java.util.concurrent.TimeUnit
import org.jetbrains.exposed.sql.transactions.transaction

@UseExperimental(KtorExperimentalAPI::class)
class RemotesApiTest : StringSpec() {

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
                providers.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
            }
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/repo/remotes")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[]"
            }
        }

        "get remote list succeeds" {
            transaction {
                providers.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
                providers.metadata.addRemote("repo", Remote("nop", "foo"))
                providers.metadata.addRemote("repo", EngineRemote(name = "bar", address = "a", username = "u", password = "p", repository = "r"))
            }
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/repo/remotes")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[{\"provider\":\"nop\",\"name\":\"foo\"}," +
                        "{\"provider\":\"engine\",\"name\":\"bar\",\"address\":\"a\"," +
                        "\"username\":\"u\",\"password\":\"p\",\"repository\":\"r\"}]"
            }
        }

        "create remote succeeds" {
            transaction {
                providers.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
            }
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"nop\",\"name\":\"a\"}")
            }) {
                response.status() shouldBe HttpStatusCode.Created
                response.content shouldBe "{\"provider\":\"nop\",\"name\":\"a\"}"
            }
        }

        "add remote succeeds" {
            transaction {
                providers.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
            }
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"engine\",\"name\":\"bar\",\"address\":\"a\"," +
                        "\"username\":\"u\",\"password\":\"p\"}")
            }) {
                response.status() shouldBe HttpStatusCode.Created
            }
        }

        "add duplicate remote fails" {
            transaction {
                providers.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
                providers.metadata.addRemote("repo", Remote("nop", "a"))
            }
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"nop\",\"name\":\"a\"}")
            }) {
                response.status() shouldBe HttpStatusCode.Conflict
            }
        }

        "update non-existent remote fails" {
            transaction {
                providers.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
            }
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes/foo") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"nop\",\"name\":\"a\"}")
            }) {
                response.status() shouldBe HttpStatusCode.NotFound
            }
        }

        "update remote succeeds" {
            transaction {
                providers.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
                providers.metadata.addRemote("repo", Remote("nop", "foo"))
                providers.metadata.addRemote("repo", EngineRemote(name = "bar", address = "a", username = "u", password = "p", repository = "r"))
            }
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes/bar") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"engine\",\"name\":\"bar\",\"address\":\"b\"," +
                        "\"username\":\"u\",\"password\":\"p\"}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "{\"provider\":\"engine\",\"name\":\"bar\",\"address\":\"b\"," +
                        "\"username\":\"u\",\"password\":\"p\"}"
            }
        }

        "rename remote succeeds" {
            transaction {
                providers.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
                providers.metadata.addRemote("repo", Remote("nop", "foo"))
                providers.metadata.addRemote("repo", EngineRemote(name = "bar", address = "a", username = "u", password = "p", repository = "r"))
            }
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes/bar") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"engine\",\"name\":\"baz\",\"address\":\"b\"," +
                        "\"username\":\"u\",\"password\":\"p\"}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
            }
        }

        "rename remote to existing name fails" {
            transaction {
                providers.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
                providers.metadata.addRemote("repo", Remote("nop", "foo"))
                providers.metadata.addRemote("repo", EngineRemote(name = "bar", address = "a", username = "u", password = "p", repository = "r"))
            }
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes/bar") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"engine\",\"name\":\"foo\",\"address\":\"b\"," +
                        "\"username\":\"u\",\"password\":\"p\"}")
            }) {
                response.status() shouldBe HttpStatusCode.Conflict
            }
        }

        "delete remote succeeds" {
            transaction {
                providers.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
                providers.metadata.addRemote("repo", Remote("nop", "foo"))
                providers.metadata.addRemote("repo", EngineRemote(name = "bar", address = "a", username = "u", password = "p", repository = "r"))
            }
            with(engine.handleRequest(HttpMethod.Delete, "/v1/repositories/repo/remotes/bar")) {
                response.status() shouldBe HttpStatusCode.NoContent
            }
        }

        "list remote commits succeeds" {
            transaction {
                providers.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
                providers.metadata.addRemote("repo", Remote("nop", "foo"))
            }
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/repo/remotes/foo/commits") {
                addHeader("titan-remote-parameters", "{\"provider\":\"nop\"}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[]"
            }
        }

        "get remote commit succeeds" {
            transaction {
                providers.metadata.createRepository(Repository(name = "repo", properties = mapOf()))
                providers.metadata.addRemote("repo", Remote("nop", "foo"))
            }
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/repo/remotes/foo/commits/c") {
                addHeader("titan-remote-parameters", "{\"provider\":\"nop\"}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"id\":\"c\",\"properties\":{}}"
            }
        }
    }
}
