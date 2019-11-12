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
import io.titandata.models.Repository
import io.titandata.models.Volume
import io.titandata.storage.zfs.ZfsStorageProvider
import java.util.concurrent.TimeUnit
import org.apache.commons.text.StringEscapeUtils
import org.jetbrains.exposed.sql.transactions.transaction

@UseExperimental(KtorExperimentalAPI::class)
class DockerVolumesApiTest : StringSpec() {

    lateinit var vs: String

    @MockK
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
        providers.metadata.clear()
        vs = transaction {
            providers.metadata.createRepository(Repository(name = "foo", properties = emptyMap()))
            providers.metadata.createVolumeSet("foo", null, true)
        }
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
        "create volume succeeds" {
            every { zfsStorageProvider.createVolume(any(), any()) } returns emptyMap()
            with(engine.handleRequest(HttpMethod.Post, "/VolumeDriver.Create") {
                setBody("{\"Name\":\"foo/vol\",\"Opts\":{\"a\":\"b\"}}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"Err\":\"\"}"

                verify {
                    zfsStorageProvider.createVolume(vs, "vol")
                }
            }
        }

        "create volume for unknown repo fails" {
            with(engine.handleRequest(HttpMethod.Post, "/VolumeDriver.Create") {
                setBody("{\"Name\":\"bar/vol\",\"Opts\":{\"a\":\"b\"}}")
            }) {
                response.status() shouldBe HttpStatusCode.NotFound
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                StringEscapeUtils.unescapeJson(response.content) shouldBe "{\"Err\":\"no such repository 'bar'\"}"
            }
        }

        "setting docker accept header succeeds" {
            with(engine.handleRequest(HttpMethod.Post, "/VolumeDriver.Capabilities") {
                addHeader(HttpHeaders.Accept, "application/vnd.docker.plugins.v1.2+json")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/vnd.docker.plugins.v1.2+json; charset=UTF-8"
                response.content shouldBe "{\"Capabilities\":{\"Scope\":\"local\"}}"
            }
        }

        "get capabilities succeeds" {
            with(engine.handleRequest(HttpMethod.Post, "/VolumeDriver.Capabilities")) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "{\"Capabilities\":{\"Scope\":\"local\"}}"
            }
        }

        "plugin activate succeeds" {
            with(engine.handleRequest(HttpMethod.Post, "/Plugin.Activate")) {
                response.status() shouldBe HttpStatusCode.OK
                response.content shouldBe "{\"Implements\":[\"VolumeDriver\"]}"
            }
        }

        "remove volume succeeds" {
            transaction {
                providers.metadata.createVolume(vs, Volume("vol"))
            }
            with(engine.handleRequest(HttpMethod.Post, "/VolumeDriver.Remove") {
                setBody("{\"Name\":\"foo/vol\"}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"Err\":\"\"}"
            }
        }

        "mount volume succeeds" {
            transaction {
                providers.metadata.createVolume(vs, Volume(name = "vol", config = mapOf("mountpoint" to "/mountpoint")))
            }
            every { zfsStorageProvider.activateVolume(any(), any(), any()) } just Runs
            with(engine.handleRequest(HttpMethod.Post, "/VolumeDriver.Mount") {
                setBody("{\"Name\":\"foo/vol\"}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"Err\":\"\",\"Mountpoint\":\"/mountpoint\"}"

                verify {
                    zfsStorageProvider.activateVolume(vs, "vol", mapOf("mountpoint" to "/mountpoint"))
                }
            }
        }

        "unmount volume succeeds" {
            transaction {
                providers.metadata.createVolume(vs, Volume("vol"))
            }
            every { zfsStorageProvider.inactivateVolume(any(), any(), any()) } just Runs
            with(engine.handleRequest(HttpMethod.Post, "/VolumeDriver.Unmount") {
                setBody("{\"Name\":\"foo/vol\"}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"Err\":\"\"}"

                verify {
                    zfsStorageProvider.inactivateVolume(vs, "vol", emptyMap())
                }
            }
        }

        "get path succeeds" {
            transaction {
                providers.metadata.createVolume(vs, Volume(name = "vol", config = mapOf("mountpoint" to "/mountpoint")))
            }
            with(engine.handleRequest(HttpMethod.Post, "/VolumeDriver.Path") {
                setBody("{\"Name\":\"foo/vol\"}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"Err\":\"\",\"Mountpoint\":\"/mountpoint\"}"
            }
        }

        "get volume succeeds" {
            transaction {
                providers.metadata.createVolume(vs, Volume(name = "vol", properties = mapOf("a" to "b"), config = mapOf("mountpoint" to "/mountpoint")))
            }
            with(engine.handleRequest(HttpMethod.Post, "/VolumeDriver.Get") {
                setBody("{\"Name\":\"foo/vol\"}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"Err\":\"\"," +
                        "\"Volume\":{" +
                        "\"Name\":\"foo/vol\"," +
                        "\"Mountpoint\":\"/mountpoint\"," +
                        "\"Status\":{}," +
                        "\"properties\":{\"a\":\"b\"}" +
                        "}}"
            }
        }

        "list volumes succeeds" {
            transaction {
                providers.metadata.createVolume(vs, Volume(name = "one", properties = mapOf("a" to "b"), config = mapOf("mountpoint" to "/mountpoint")))
                providers.metadata.createVolume(vs, Volume(name = "two", properties = mapOf("c" to "d"), config = mapOf("mountpoint" to "/mountpoint")))
            }

            with(engine.handleRequest(HttpMethod.Post, "/VolumeDriver.List")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"Err\":\"\"," +
                        "\"Volumes\":[{" +
                        "\"Name\":\"foo/one\"," +
                        "\"Mountpoint\":\"/mountpoint\"," +
                        "\"Status\":{}," +
                        "\"properties\":{\"a\":\"b\"}" +
                        "},{" +
                        "\"Name\":\"foo/two\"," +
                        "\"Mountpoint\":\"/mountpoint\"," +
                        "\"Status\":{}," +
                        "\"properties\":{\"c\":\"d\"}" +
                        "}]}"
            }
        }
    }
}
