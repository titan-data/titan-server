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
import io.titandata.models.RemoteParameters

fun Route.OperationsApi(@Suppress("UNUSED_PARAMETER") providers: ProviderModule) {

    route("/v1/repositories/{repositoryName}/operations") {
        get {
            val repo = call.parameters["repositoryName"] ?: throw IllegalArgumentException("missing repository name parameter")
            call.respond(providers.operations.listOperations(repo))
        }
    }

    route("/v1/repositories/{repositoryName}/operations/{operationId}") {
        delete {
            val repo = call.parameters["repositoryName"] ?: throw IllegalArgumentException("missing repository name parameter")
            val operation = call.parameters["operationId"] ?: throw IllegalArgumentException("missing operation id parameter")
            providers.operations.abortOperation(repo, operation)
            call.respond(HttpStatusCode.NoContent)
        }

        get {
            val repo = call.parameters["repositoryName"] ?: throw IllegalArgumentException("missing repository name parameter")
            val operation = call.parameters["operationId"] ?: throw IllegalArgumentException("missing operation id parameter")
            call.respond(providers.operations.getOperation(repo, operation))
        }
    }

    route("/v1/repositories/{repositoryName}/operations/{operationId}/progress") {
        get {
            val repo = call.parameters["repositoryName"] ?: throw IllegalArgumentException("missing repository name parameter")
            val operation = call.parameters["operationId"] ?: throw IllegalArgumentException("missing operation id parameter")
            call.respond(providers.operations.getProgress(repo, operation))
        }
    }

    route("/v1/repositories/{repositoryName}/remotes/{remoteName}/commits/{commitId}/pull") {
        post {
            val repo = call.parameters["repositoryName"] ?: throw IllegalArgumentException("missing repository name parameter")
            val remote = call.parameters["remoteName"] ?: throw IllegalArgumentException("missing remote name parameter")
            val commitId = call.parameters["commitId"] ?: throw IllegalArgumentException("missing commit id parameter")
            val params = call.receive(RemoteParameters::class)
            val metadataOnly = if (call.request.queryParameters.contains("metadataOnly")) {
                call.request.queryParameters.get("metadataOnly")!!.toBoolean()
            } else {
                false
            }
            call.respond(providers.operations.startPull(repo, remote, commitId, params, metadataOnly))
        }
    }

    route("/v1/repositories/{repositoryName}/remotes/{remoteName}/commits/{commitId}/push") {
        post {
            val repo = call.parameters["repositoryName"] ?: throw IllegalArgumentException("missing repository name parameter")
            val remote = call.parameters["remoteName"] ?: throw IllegalArgumentException("missing remote name parameter")
            val commitId = call.parameters["commitId"] ?: throw IllegalArgumentException("missing commit id parameter")
            val params = call.receive(RemoteParameters::class)
            val metadataOnly = if (call.request.queryParameters.contains("metadataOnly")) {
                call.request.queryParameters.get("metadataOnly")!!.toBoolean()
            } else {
                false
            }
            call.respond(providers.operations.startPush(repo, remote, commitId, params, metadataOnly))
        }
    }
}
