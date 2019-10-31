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
import io.titandata.exception.CommandException
import io.titandata.models.Error
import io.titandata.storage.zfs.ZfsStorageProvider
import io.titandata.util.CommandExecutor
import java.util.concurrent.TimeUnit

@UseExperimental(KtorExperimentalAPI::class)
class RepositoriesApiTest : StringSpec() {

    @MockK
    lateinit var executor: CommandExecutor

    @InjectMockKs
    @OverrideMockKs
    var zfsStorageProvider = ZfsStorageProvider("test")

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
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    init {
        "list empty repositories succeeds" {
            every { executor.exec(*anyVararg()) } returns ""
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[]"
            }
        }

        "list repositories succeeds" {
            every { executor.exec(*anyVararg()) } returns
            "test\t-\n" +
            "test/repo/repo1\t{\"a\":\"b\"}\n" +
            "test/repo/repo2\t{}\n"
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories")) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "[{\"name\":\"repo1\",\"properties\":{\"a\":\"b\"}}," +
                "{\"name\":\"repo2\",\"properties\":{}}]"
            }
        }

        "get repository succeeds" {
            every { executor.exec(*anyVararg()) } returns "test/repo/repo\t{\"a\": \"b\"}"
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/repo")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"name\":\"repo\",\"properties\":{\"a\":\"b\"}}"
            }
        }

        "get unknown repository returns not found" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "dataset does not exist")
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
                error.message shouldBe "invalid repository name, can only contain alphanumeric characters, '-', ':', '.', or '_'"
            }
        }

        "get repository status succeeds" {
            every { executor.exec(*anyVararg()) } returns ""
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy,io.titan-data:metadata", "-t", "snapshot",
                    "-d", "2", "test/repo/foo") } returns "test/repo/foo/guid@hash\toff\t{}\n"
            every { executor.exec("zfs", "list", "-Ho", "io.titan-data:metadata",
                    "test/repo/foo/guid@hash") } returns "{\"a\":\"b\"}\n"
            every { executor.exec("zfs", "list", "-Hpo", "io.titan-data:active",
                    "test/repo/foo") } returns "guid"
            every { executor.exec("zfs", "list", "-pHo", "logicalused,used", "test/repo/foo/guid") } returns "40\t20\n"
            every { executor.exec("zfs", "list", "-Ho", "origin", "-r", "test/repo/foo/guid") } returns
                    "test/repo/foo/guidtwo/v0@sourcehash\n"
            every { executor.exec("zfs", "list", "-d", "1",
                    "-pHo", "name,logicalreferenced,referenced,io.titan-data:metadata", "test/repo/foo/guid") } returns arrayOf(
                    "test/repo/foo/guid\t4\t6\t{}",
                    "test/repo/foo/guid/v0\t5\t10\t{\"path\":\"/var/a\"}",
                    "test/repo/foo/guid/v1\t8\t16\t{\"path\":\"/var/b\"}"
            ).joinToString("\n")
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/status")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"logicalSize\":40,\"actualSize\":20," +
                    "\"lastCommit\":\"hash\",\"sourceCommit\":\"sourcehash\",\"volumeStatus\":[" +
                "{\"name\":\"v0\",\"logicalSize\":5,\"actualSize\":10,\"properties\":{\"path\":\"/var/a\"}}," +
                "{\"name\":\"v1\",\"logicalSize\":8,\"actualSize\":16,\"properties\":{\"path\":\"/var/b\"}}]}"
            }
        }

        "delete repository succeeds" {
            every { executor.exec(*anyVararg()) } returns ""
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
                error.message shouldBe "invalid repository name, can only contain alphanumeric characters, '-', ':', '.', or '_'"
            }
        }

        "create repository succeeds" {
            every { executor.exec(*anyVararg()) } returns ""
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
            every { executor.exec(*anyVararg()) } returns ""
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"name\":\"bad@name\",\"properties\":{\"a\":\"b\"}}")
            }) {
                response.status() shouldBe HttpStatusCode.BadRequest
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "IllegalArgumentException"
                error.message shouldBe "invalid repository name, can only contain alphanumeric characters, '-', ':', '.', or '_'"
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
            every { executor.exec(*anyVararg()) } returns ""
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
            every { executor.exec(*anyVararg()) } returns ""
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"name\":\"bad@name\",\"properties\":{\"a\":\"b\"}}")
            }) {
                response.status() shouldBe HttpStatusCode.BadRequest
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "IllegalArgumentException"
                error.message shouldBe "invalid repository name, can only contain alphanumeric characters, '-', ':', '.', or '_'"
            }
        }
    }
}
