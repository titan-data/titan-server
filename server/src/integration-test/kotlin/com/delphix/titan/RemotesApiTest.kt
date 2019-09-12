/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan

import com.delphix.titan.remote.engine.EngineRemoteProvider
import com.delphix.titan.storage.zfs.ZfsStorageProvider
import com.delphix.titan.util.CommandExecutor
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
import io.mockk.verify
import java.util.concurrent.TimeUnit

@UseExperimental(KtorExperimentalAPI::class)
class RemotesApiTest : StringSpec() {

    @MockK
    lateinit var executor: CommandExecutor

    @InjectMockKs
    @OverrideMockKs
    var zfsStorageProvider = ZfsStorageProvider("test")

    @MockK
    lateinit var engineRemoteProvider: EngineRemoteProvider

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

    /*
     * Note that docker explicitly doesn't set the Content-Type header to application/json, so
     * we want to make sure that we respond correctly even when this header isn't set.
     */
    init {
        "get empty remote list succeeds" {
            every { executor.exec(*anyVararg()) } returns "-"
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/repo/remotes")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[]"
            }
        }

        "get remote list succeeds" {
            every { executor.exec(*anyVararg()) } returns
                    "[{\"provider\":\"nop\",\"name\":\"foo\"}," +
                    "{\"provider\":\"engine\",\"name\":\"bar\",\"address\":\"a\"," +
                    "\"username\":\"u\",\"password\":\"p\"}]"
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/repo/remotes")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[{\"provider\":\"nop\",\"name\":\"foo\"}," +
                        "{\"provider\":\"engine\",\"name\":\"bar\",\"address\":\"a\"," +
                        "\"username\":\"u\",\"password\":\"p\"}]"
            }
        }

        "create remote succeeds" {
            every { executor.exec(*anyVararg()) } returns "-"
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"nop\",\"name\":\"a\"}")
            }) {
                response.status() shouldBe HttpStatusCode.Created
                response.content shouldBe "{\"provider\":\"nop\",\"name\":\"a\"}"

                verify {
                    executor.exec("zfs", "set",
                            "com.delphix.titan:remotes=[{\"provider\":\"nop\",\"name\":\"a\"}]",
                            "test/repo/repo")
                }
            }
        }

        "add remote succeeds" {
            every { executor.exec(*anyVararg()) } returns "[{\"provider\":\"nop\",\"name\":\"a\"}]"
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"engine\",\"name\":\"bar\",\"address\":\"a\"," +
                        "\"username\":\"u\",\"password\":\"p\"}")
            }) {
                response.status() shouldBe HttpStatusCode.Created

                verify {
                    executor.exec("zfs", "set",
                            "com.delphix.titan:remotes=[{\"provider\":\"nop\",\"name\":\"a\"}," +
                                    "{\"provider\":\"engine\",\"name\":\"bar\",\"address\":\"a\"," +
                                    "\"username\":\"u\",\"password\":\"p\"}]",
                            "test/repo/repo")
                }
            }
        }

        "add duplicate remote fails" {
            every { executor.exec(*anyVararg()) } returns "[{\"provider\":\"nop\",\"name\":\"a\"}]"
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"nop\",\"name\":\"a\"}")
            }) {
                response.status() shouldBe HttpStatusCode.Conflict
            }
        }

        "update non-existent remote fails" {
            every { executor.exec(*anyVararg()) } returns "[]"
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes/foo") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"nop\",\"name\":\"a\"}")
            }) {
                response.status() shouldBe HttpStatusCode.NotFound
            }
        }

        "update remote succeeds" {
            every { executor.exec(*anyVararg()) } returns
                    "[{\"provider\":\"nop\",\"name\":\"foo\"}," +
                    "{\"provider\":\"engine\",\"name\":\"bar\",\"address\":\"a\"," +
                    "\"username\":\"u\",\"password\":\"p\"}]"
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes/bar") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"engine\",\"name\":\"bar\",\"address\":\"b\"," +
                        "\"username\":\"u\",\"password\":\"p\"}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "{\"provider\":\"engine\",\"name\":\"bar\",\"address\":\"b\"," +
                        "\"username\":\"u\",\"password\":\"p\"}"
                verify {
                    executor.exec("zfs", "set",
                            "com.delphix.titan:remotes=[{\"provider\":\"nop\",\"name\":\"foo\"}," +
                                    "{\"provider\":\"engine\",\"name\":\"bar\",\"address\":\"b\"," +
                                    "\"username\":\"u\",\"password\":\"p\"}]",
                            "test/repo/repo")
                }
            }
        }

        "rename remote succeeds" {
            every { executor.exec(*anyVararg()) } returns
                    "[{\"provider\":\"nop\",\"name\":\"foo\"}," +
                    "{\"provider\":\"engine\",\"name\":\"bar\",\"address\":\"a\"," +
                    "\"username\":\"u\",\"password\":\"p\"}]"
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes/bar") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"engine\",\"name\":\"baz\",\"address\":\"b\"," +
                        "\"username\":\"u\",\"password\":\"p\"}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                verify {
                    executor.exec("zfs", "set",
                            "com.delphix.titan:remotes=[{\"provider\":\"nop\",\"name\":\"foo\"}," +
                                    "{\"provider\":\"engine\",\"name\":\"baz\",\"address\":\"b\"," +
                                    "\"username\":\"u\",\"password\":\"p\"}]",
                            "test/repo/repo")
                }
            }
        }

        "rename remote to existing name fails" {
            every { executor.exec(*anyVararg()) } returns
                    "[{\"provider\":\"nop\",\"name\":\"foo\"}," +
                    "{\"provider\":\"engine\",\"name\":\"bar\",\"address\":\"a\"," +
                    "\"username\":\"u\",\"password\":\"p\"}]"
            with(engine.handleRequest(HttpMethod.Post, "/v1/repositories/repo/remotes/bar") {
                addHeader(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{\"provider\":\"engine\",\"name\":\"foo\",\"address\":\"b\"," +
                        "\"username\":\"u\",\"password\":\"p\"}")
            }) {
                response.status() shouldBe HttpStatusCode.Conflict
            }
        }

        "delete remote succeeds" {
            every { executor.exec(*anyVararg()) } returns
                    "[{\"provider\":\"nop\",\"name\":\"foo\"}," +
                    "{\"provider\":\"engine\",\"name\":\"bar\",\"address\":\"a\"," +
                    "\"username\":\"u\",\"password\":\"p\"}]"
            with(engine.handleRequest(HttpMethod.Delete, "/v1/repositories/repo/remotes/bar")) {
                response.status() shouldBe HttpStatusCode.NoContent

                verify {
                    executor.exec("zfs", "set",
                            "com.delphix.titan:remotes=[{\"provider\":\"nop\",\"name\":\"foo\"}]",
                            "test/repo/repo")
                }
            }
        }

        "list remote commits succeeds" {
            every { executor.exec(*anyVararg()) } returns "[{\"provider\":\"nop\",\"name\":\"foo\"}]"
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/repo/remotes/foo/commits") {
                addHeader("titan-remote-parameters", "{\"provider\":\"nop\"}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[]"
            }
        }

        "get remote commit succeeds" {
            every { executor.exec(*anyVararg()) } returns "[{\"provider\":\"nop\",\"name\":\"foo\"}]"
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
