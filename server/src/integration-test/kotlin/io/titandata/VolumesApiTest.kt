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
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.TestApplicationEngine
import io.ktor.server.testing.contentType
import io.ktor.server.testing.createTestEnvironment
import io.ktor.server.testing.handleRequest
import io.ktor.util.KtorExperimentalAPI
import io.mockk.MockKAnnotations
import io.mockk.clearAllMocks
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.impl.annotations.OverrideMockKs
import io.mockk.mockk
import io.titandata.context.docker.DockerZfsContext
import io.titandata.models.Repository
import java.util.concurrent.TimeUnit
import org.jetbrains.exposed.sql.transactions.transaction

@UseExperimental(KtorExperimentalAPI::class)
class VolumesApiTest : StringSpec() {

    lateinit var vs: String

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
        vs = transaction {
            services.metadata.createRepository(Repository(name = "foo", properties = emptyMap()))
            services.metadata.createVolumeSet("foo", null, true)
        }
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    init {
        "list volumes succeeds" {
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/foo/volumes")) {
                response.status() shouldBe HttpStatusCode.OK
                response.contentType().toString() shouldBe "application/json; charset=UTF-8"
                response.content shouldBe "[]"
            }
        }

        "list volumes for non-existent repo fails" {
            with(engine.handleRequest(HttpMethod.Get, "/v1/repositories/bar/volumes")) {
                response.status() shouldBe HttpStatusCode.NotFound
            }
        }
    }
}
