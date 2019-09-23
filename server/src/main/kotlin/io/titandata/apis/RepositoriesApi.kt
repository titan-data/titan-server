/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.apis

import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.request.receive
import io.ktor.response.respond
import io.ktor.routing.Route
import io.ktor.routing.delete
import io.ktor.routing.get
import io.ktor.routing.post
import io.ktor.routing.route
import io.titandata.ProviderModule
import io.titandata.models.Repository

fun Route.RepositoriesApi(providers: ProviderModule) {
    route("/v1/repositories") {
        post {
            val repo = call.receive(Repository::class)
            providers.storage.createRepository(repo)
            call.respond(HttpStatusCode.Created, repo)
        }

        get {
            call.respond(providers.storage.listRepositories())
        }
    }

    route("/v1/repositories/{repositoryName}") {
        get {
            val name = call.parameters["repositoryName"] ?: throw IllegalArgumentException("missing repositoryName parameter")
            call.respond(providers.storage.getRepository(name))
        }

        post {
            val name = call.parameters["repositoryName"] ?: throw IllegalArgumentException("missing repositoryName parameter")
            val repo = call.receive(Repository::class)
            providers.storage.updateRepository(name, repo)
            call.respond(repo)
        }

        delete {
            val name = call.parameters["repositoryName"] ?: throw IllegalArgumentException("missing repositoryName parameter")
            providers.storage.deleteRepository(name)
            call.respond(HttpStatusCode.NoContent)
        }
    }
}
