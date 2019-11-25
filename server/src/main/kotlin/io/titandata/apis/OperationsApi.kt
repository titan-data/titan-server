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
import io.titandata.models.RemoteParameters

fun Route.OperationsApi(services: ServiceLocator) {

    route("/v1/operations") {
        get {
            val repo = call.request.queryParameters["repositoryName"]
            call.respond(services.operations.listOperations(repo))
        }
    }

    route("/v1/operations/{operationId}") {
        delete {
            val operation = call.parameters["operationId"] ?: throw IllegalArgumentException("missing operation id parameter")
            services.operations.abortOperation(operation)
            call.respond(HttpStatusCode.NoContent)
        }

        get {
            val operation = call.parameters["operationId"] ?: throw IllegalArgumentException("missing operation id parameter")
            call.respond(services.operations.getOperation(operation))
        }
    }

    route("/v1/operations/{operationId}/progress") {
        get {
            val operation = call.parameters["operationId"] ?: throw IllegalArgumentException("missing operation id parameter")
            val lastId = call.request.queryParameters.get("lastId")?.toInt() ?: 0
            call.respond(services.operations.getProgress(operation, lastId))
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
            call.respond(services.operations.startPull(repo, remote, commitId, params, metadataOnly))
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
            call.respond(services.operations.startPush(repo, remote, commitId, params, metadataOnly))
        }
    }
}
