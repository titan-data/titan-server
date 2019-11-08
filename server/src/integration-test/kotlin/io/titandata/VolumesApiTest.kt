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
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.impl.annotations.OverrideMockKs
import io.mockk.impl.annotations.SpyK
import io.mockk.verify
import io.titandata.exception.CommandException
import io.titandata.metadata.MetadataProvider
import io.titandata.models.Repository
import io.titandata.models.Volume
import io.titandata.storage.zfs.ZfsStorageProvider
import io.titandata.util.CommandExecutor
import java.util.concurrent.TimeUnit
import org.apache.commons.text.StringEscapeUtils
import org.jetbrains.exposed.sql.transactions.transaction

@UseExperimental(KtorExperimentalAPI::class)
class VolumesApiTest : StringSpec() {

    @MockK
    lateinit var executor: CommandExecutor

    @InjectMockKs
    @OverrideMockKs
    var zfsStorageProvider = ZfsStorageProvider("test")

    @SpyK
    var metadata = MetadataProvider()

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
        "create volume succeeds" {
            val guid = transaction {
                providers.metadata.createRepository(Repository(name = "foo", properties = emptyMap()))
                providers.metadata.createVolumeSet("foo", null, true)
            }
            every { executor.exec("zfs", "create", "-o",
                    "io.titan-data:metadata={\"a\":\"b\"}", "test/repo/foo/$guid/vol") } returns ""
            every { executor.exec("zfs", "snapshot", "test/repo/foo/$guid/vol@initial") } returns ""
            every { executor.exec("mkdir", "-p", "/var/lib/test/mnt/foo/vol") } returns ""
            with(engine.handleRequest(HttpMethod.Post, "/VolumeDriver.Create") {
                setBody("{\"Name\":\"foo/vol\",\"Opts\":{\"a\":\"b\"}}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"Err\":\"\"}"

                verify {
                    executor.exec("zfs", "create", "-o",
                            "io.titan-data:metadata={\"a\":\"b\"}", "test/repo/foo/$guid/vol")
                    executor.exec("zfs", "snapshot", "test/repo/foo/$guid/vol@initial")
                    executor.exec("mkdir", "-p", "/var/lib/test/mnt/foo/vol")
                }
            }
        }

        "create volume for unknown repo fails" {
            every { executor.exec(*anyVararg()) } throws CommandException("", 1, "does not exist")
            with(engine.handleRequest(HttpMethod.Post, "/VolumeDriver.Create") {
                setBody("{\"Name\":\"foo/vol\",\"Opts\":{\"a\":\"b\"}}")
            }) {
                response.status() shouldBe HttpStatusCode.NotFound
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                StringEscapeUtils.unescapeJson(response.content) shouldBe "{\"Err\":\"no such repository 'foo'\"}"
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
            val guid = transaction {
                providers.metadata.createRepository(Repository(name = "foo", properties = emptyMap()))
                val vs = providers.metadata.createVolumeSet("foo", null, true)
                providers.metadata.createVolume(vs, Volume(name = "vol"))
                vs
            }
            every { executor.exec("zfs", "destroy", "-R", "test/repo/foo/$guid/vol") } returns ""
            every { executor.exec("rmdir", "/var/lib/test/mnt/foo/vol") } returns ""
            with(engine.handleRequest(HttpMethod.Post, "/VolumeDriver.Remove") {
                setBody("{\"Name\":\"foo/vol\"}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"Err\":\"\"}"
                verify {
                    executor.exec("zfs", "destroy", "-R", "test/repo/foo/$guid/vol")
                }
            }
        }

        "mount volume succeeds" {
            val guid = transaction {
                providers.metadata.createRepository(Repository(name = "foo", properties = emptyMap()))
                val vs = providers.metadata.createVolumeSet("foo", null, true)
                providers.metadata.createVolume(vs, Volume(name = "vol"))
                vs
            }
            every { executor.exec("mount", "-t", "zfs", "test/repo/foo/$guid/vol",
                    "/var/lib/test/mnt/foo/vol") } returns ""
            with(engine.handleRequest(HttpMethod.Post, "/VolumeDriver.Mount") {
                setBody("{\"Name\":\"foo/vol\"}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"Err\":\"\",\"Mountpoint\":\"/var/lib/test/mnt/foo/vol\"}"

                verify {
                    executor.exec("mount", "-t", "zfs", "test/repo/foo/$guid/vol",
                            "/var/lib/test/mnt/foo/vol")
                }
            }
        }

        "unmount volume succeeds" {
            every { executor.exec("umount", "/var/lib/test/mnt/foo/vol") } returns ""
            with(engine.handleRequest(HttpMethod.Post, "/VolumeDriver.Unmount") {
                setBody("{\"Name\":\"foo/vol\"}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"Err\":\"\"}"

                verify {
                    executor.exec("umount", "/var/lib/test/mnt/foo/vol")
                }
            }
        }

        "get path succeeds" {
            transaction {
                providers.metadata.createRepository(Repository(name = "foo", properties = emptyMap()))
                val vs = providers.metadata.createVolumeSet("foo", null, true)
                providers.metadata.createVolume(vs, Volume(name = "vol", properties = emptyMap()))
            }
            with(engine.handleRequest(HttpMethod.Post, "/VolumeDriver.Path") {
                setBody("{\"Name\":\"foo/vol\"}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"Err\":\"\",\"Mountpoint\":\"/var/lib/test/mnt/foo/vol\"}"
            }
        }

        "get volume succeeds" {
            transaction {
                providers.metadata.createRepository(Repository(name = "foo", properties = emptyMap()))
                val vs = providers.metadata.createVolumeSet("foo", null, true)
                providers.metadata.createVolume(vs, Volume("vol", properties = mapOf("a" to "b")))
            }
            with(engine.handleRequest(HttpMethod.Post, "/VolumeDriver.Get") {
                setBody("{\"Name\":\"foo/vol\"}")
            }) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"Err\":\"\"," +
                        "\"Volume\":{" +
                        "\"Name\":\"foo/vol\"," +
                        "\"Mountpoint\":\"/var/lib/test/mnt/foo/vol\"," +
                        "\"Status\":{}," +
                        "\"properties\":{\"a\":\"b\"}" +
                        "}}"
            }
        }

        "list volumes succeeds" {
            transaction {
                providers.metadata.createRepository(Repository(name = "foo", properties = emptyMap()))
                val vs = providers.metadata.createVolumeSet("foo", null, true)
                providers.metadata.createVolume(vs, Volume(name = "one", properties = mapOf("a" to "b")))
                providers.metadata.createVolume(vs, Volume(name = "two", properties = mapOf("c" to "d")))
            }

            with(engine.handleRequest(HttpMethod.Post, "/VolumeDriver.List")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "{\"Err\":\"\"," +
                        "\"Volumes\":[{" +
                        "\"Name\":\"foo/one\"," +
                        "\"Mountpoint\":\"/var/lib/test/mnt/foo/one\"," +
                        "\"Status\":{}," +
                        "\"properties\":{\"a\":\"b\"}" +
                        "},{" +
                        "\"Name\":\"foo/two\"," +
                        "\"Mountpoint\":\"/var/lib/test/mnt/foo/two\"," +
                        "\"Status\":{}," +
                        "\"properties\":{\"c\":\"d\"}" +
                        "}]}"
            }
        }
    }
}
