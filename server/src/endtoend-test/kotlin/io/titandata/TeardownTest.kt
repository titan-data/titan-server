/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata

import io.kotlintest.Spec
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.titandata.client.infrastructure.ClientException
import io.titandata.models.Repository

class TeardownTest : EndToEndTest() {

    override fun beforeSpec(spec: Spec) {
        dockerUtil.stopServer()
        dockerUtil.startServer()
        dockerUtil.waitForServer()
    }

    override fun afterSpec(spec: Spec) {
        dockerUtil.stopServer()
    }

    init {
        "create new repository succeeds" {
            val repo = Repository(
                    name = "foo",
                    properties = mapOf("a" to "b")
            )
            repoApi.createRepository(repo)
        }

        "get created repository succeeds" {
            val repo = repoApi.getRepository("foo")
            repo.name shouldBe "foo"
        }

        "restarting server leaves repository intact" {
            dockerUtil.restartServer()
            dockerUtil.waitForServer()
            val repo = repoApi.getRepository("foo")
            repo.name shouldBe "foo"
        }

        "teardown and restart server should leave no repositories" {
            dockerUtil.stopServer(ignoreExceptions = false)
            dockerUtil.startServer()
            dockerUtil.waitForServer()
            shouldThrow<ClientException> {
                repoApi.getRepository("foo")
            }
        }
    }
}
