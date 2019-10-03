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
import io.titandata.operation.OperationProvider
import io.titandata.storage.zfs.ZfsStorageProvider
import io.titandata.util.CommandExecutor
import io.titandata.util.GuidGenerator
import java.util.concurrent.TimeUnit

@UseExperimental(KtorExperimentalAPI::class)
class CommitsApiTest : StringSpec() {

    @MockK
    lateinit var executor: CommandExecutor

    @MockK
    lateinit var generator: GuidGenerator

    @MockK(relaxed = true)
    lateinit var operationProvider: OperationProvider

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
        "list empty commits succeeds" {
            every { executor.exec(*anyVararg()) } returns ""
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/repo/commits")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[]"
            }
        }

        "list commits succeeds" {
            every { executor.exec(*anyVararg()) } returns arrayOf(
                    "test/repo/foo@ignore\toff\t{}",
                    "test/repo/foo/guid1@hash1\toff\t{\"a\":\"b\"}",
                    "test/repo/foo/guid2@hash2\toff\t{\"c\":\"d\"}"
            ).joinToString("\n")
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/commits")) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "[{\"id\":\"hash1\",\"properties\":{\"a\":\"b\"}},{\"id\":\"hash2\",\"properties\":{\"c\":\"d\"}}]"
            }
        }

        "list commits fails with non existent repository" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "does not exist")
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/repo/commits")) {
                response.status() shouldBe HttpStatusCode.NotFound
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "NoSuchObjectException"
                error.message shouldBe "no such repository 'repo'"
            }
        }

        "get commit succeeds" {
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot", "-d", "2", "test/repo/foo") } returns "test/repo/foo/guid@hash\toff"
            every { executor.exec("zfs", "list", "-Ho", "io.titan-data:metadata", "test/repo/foo/guid@hash") } returns "{\"a\":\"b\"}"
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/commits/hash")) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "{\"id\":\"hash\",\"properties\":{\"a\":\"b\"}}"
            }
        }

        "get commit status succeeds" {
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot", "-d", "2", "test/repo/foo") } returns "test/repo/foo/guid@hash\toff"
            every { executor.exec("zfs", "list", "-Ho", "io.titan-data:metadata", "test/repo/foo/guid@hash") } returns "{\"a\":\"b\"}"
            every { executor.exec("zfs", "list", "-Hpo", "name,logicalreferenced,referenced,used", "-t",
                    "snapshot", "-r", "test/repo/foo/guid") } returns arrayOf(
                    "test/repo/foo/guid@hash\t1\t1\t1",
                    "test/repo/foo/guid/v0@hash\t1\t2\t3",
                    "test/repo/foo/guid/v0@otherhash\t2\t2\t2",
                    "test/repo/foo/guid/v1@hash\t2\t4\t6"
            ).joinToString("\n")
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/commits/hash/status")) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "{\"logicalSize\":3,\"actualSize\":6,\"uniqueSize\":9}"
            }
        }

        "get commit from non-existent repo returns no such object" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "does not exist")
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/commits/hash")) {
                response.status() shouldBe HttpStatusCode.NotFound
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "NoSuchObjectException"
                error.message shouldBe "no such repository 'foo'"
            }
        }

        "get non-existent commit returns no such object" {
            every { executor.exec(*anyVararg()) } returns ""
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/commits/hash")) {
                response.status() shouldBe HttpStatusCode.NotFound
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "NoSuchObjectException"
                error.message shouldBe "no such commit 'hash' in repository 'foo'"
            }
        }

        "get bad commit id returns bad request" {
            every { executor.exec(*anyVararg()) } returns ""
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/commits/bad@hash")) {
                response.status() shouldBe HttpStatusCode.BadRequest
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "IllegalArgumentException"
                error.message shouldBe "invalid commit name, can only contain alphanumeric characters, '-', ':', '.', or '_'"
            }
        }

        "delete commit succeeds" {
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot", "-d", "2", "test/repo/foo") } returns "test/repo/foo/guid@hash\toff"
            every { executor.exec("zfs", "destroy", "-rd", "test/repo/foo/guid@hash") } returns ""
            every { executor.exec("zfs", "list", "-Hpo", "io.titan-data:active", "test/repo/foo") } returns "guid"
            with(engine.handleRequest(HttpMethod.Delete, "/v1/repositories/foo/commits/hash")) {
                response.status() shouldBe HttpStatusCode.NoContent
            }
        }

        "delete commit from non-existent repo returns no such object" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "does not exist")
            with(engine.handleRequest(HttpMethod.Delete, "/v1/repositories/foo/commits/hash")) {
                response.status() shouldBe HttpStatusCode.NotFound
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "NoSuchObjectException"
                error.message shouldBe "no such repository 'foo'"
            }
        }

        "delete non-existent commit returns no such object" {
            every { executor.exec(*anyVararg()) } returns ""
            with(engine.handleRequest(HttpMethod.Delete, "/v1/repositories/foo/commits/hash")) {
                response.status() shouldBe HttpStatusCode.NotFound
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "NoSuchObjectException"
                error.message shouldBe "no such commit 'hash' in repository 'foo'"
            }
        }

        "create commit succeeds" {
            every { executor.exec("zfs", "list", "-Hpo", "io.titan-data:active", "test/repo/foo") } returns "guid"
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot", "-d", "2", "test/repo/foo") } returns ""
            every { executor.exec("zfs", "snapshot", "-r", "-o", "io.titan-data:metadata={\"a\":\"b\"}", "test/repo/foo/guid@hash") } returns ""
            every { executor.exec("zfs", "list", "-Hpo", "creation", "test/repo/foo/guid@hash") } returns "1556492646"
            every { executor.exec("zfs", "set", "io.titan-data:metadata={\"a\":\"b\",\"timestamp\":\"2019-04-28T23:04:06Z\"}", "test/repo/foo/guid@hash") } returns ""
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/foo/commits") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"id\":\"hash\",\"properties\":{\"a\":\"b\"}}")
            }) {
                response.status() shouldBe HttpStatusCode.Created
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"id\":\"hash\",\"properties\":{\"a\":\"b\",\"timestamp\":\"2019-04-28T23:04:06Z\"}}"
            }
        }

        "create commit in non-existen repo returns no such object" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "does not exist")
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/foo/commits") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"id\":\"hash\",\"properties\":{\"a\":\"b\"}}")
            }) {
                response.status() shouldBe HttpStatusCode.NotFound
                val error = Gson().fromJson(response.content, Error::class.java)
                error.code shouldBe "NoSuchObjectException"
                error.message shouldBe "no such repository 'foo'"
            }
        }

        "checkout commit succeeds" {
            every { generator.get() } returns "newguid"
            every { executor.exec("zfs", "list", "-Ho", "name,defer_destroy", "-t", "snapshot",
                    "-d", "2", "test/repo/foo")
            } returns "test/repo/foo/guid@hash\toff"
            every { executor.exec("zfs", "list", "-rHo", "name,io.titan-data:metadata", "test/repo/foo/guid") } returns
                    arrayOf("test/repo/foo/guid\t-", "test/repo/foo/guid/v0\t{}", "test/repo/foo/guid/v1\t{}").joinToString("\n")
            every { executor.exec("zfs", "create", "test/repo/foo/newguid") } returns ""
            every { executor.exec("zfs", "clone", "-o", "io.titan-data:metadata={}", "test/repo/foo/guid/v0@hash",
                    "test/repo/foo/newguid/v0") } returns ""
            every { executor.exec("zfs", "clone", "-o", "io.titan-data:metadata={}", "test/repo/foo/guid/v1@hash",
                    "test/repo/foo/newguid/v1") } returns ""
            every { executor.exec("zfs", "set", "io.titan-data:active=newguid",
                    "test/repo/foo") } returns ""
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/foo/commits/hash/checkout")) {
                response.status() shouldBe HttpStatusCode.NoContent
            }
        }
    }
}
