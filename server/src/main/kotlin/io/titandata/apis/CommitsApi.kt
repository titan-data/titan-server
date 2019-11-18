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
import io.titandata.ServiceLocator
import io.titandata.models.Commit

/**
 * Handler for all commit related APIs. These are simplistic wrappers around the underlying storage
 * provider.
 */
fun Route.CommitsApi(services: ServiceLocator) {
    route("/v1/repositories/{repositoryName}/commits") {
        post {
            val repo = call.parameters["repositoryName"] ?: throw IllegalArgumentException("missing repository name parameter")
            val commit = call.receive(Commit::class)
            val created = services.commits.createCommit(repo, commit)
            call.respond(HttpStatusCode.Created, created)
        }

        get {
            val repo = call.parameters["repositoryName"] ?: throw IllegalArgumentException("missing repository name parameter")
            val tags = call.request.queryParameters.getAll("tag")
            call.respond(services.commits.listCommits(repo, tags))
        }
    }

    route("/v1/repositories/{repositoryName}/commits/{commitId}") {
        delete {
            val repo = call.parameters["repositoryName"] ?: throw IllegalArgumentException("missing repository name parameter")
            val commit = call.parameters["commitId"] ?: throw IllegalArgumentException("missing commit id parameter")
            services.commits.deleteCommit(repo, commit)
            call.respond(HttpStatusCode.NoContent)
        }

        get {
            val repo = call.parameters["repositoryName"] ?: throw IllegalArgumentException("missing repository name parameter")
            val commit = call.parameters["commitId"] ?: throw IllegalArgumentException("missing commit id parameter")
            call.respond(services.commits.getCommit(repo, commit))
        }

        post {
            val repo = call.parameters["repositoryName"] ?: throw IllegalArgumentException("missing repository name parameter")
            val commitId = call.parameters["commitId"] ?: throw IllegalArgumentException("missing commit id parameter")
            val commit = call.receive(Commit::class)
            commit.id = commitId
            services.commits.updateCommit(repo, commit)
            call.respond(commit)
        }
    }

    route("/v1/repositories/{repositoryName}/commits/{commitId}/status") {
        get {
            val repo = call.parameters["repositoryName"] ?: throw IllegalArgumentException("missing repository name parameter")
            val commit = call.parameters["commitId"] ?: throw IllegalArgumentException("missing commit id parameter")
            call.respond(services.commits.getCommitStatus(repo, commit))
        }
    }

    route("/v1/repositories/{repositoryName}/commits/{commitId}/checkout") {
        post {
            val repo = call.parameters["repositoryName"] ?: throw IllegalArgumentException("missing repository name parameter")
            val commit = call.parameters["commitId"] ?: throw IllegalArgumentException("missing commit id parameter")
            services.commits.checkoutCommit(repo, commit)
            call.respond(HttpStatusCode.NoContent)
        }
    }
}
