/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan

import com.delphix.titan.client.infrastructure.ClientException
import com.delphix.titan.models.Repository
import io.kotlintest.Spec
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow

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
