/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.apis

import io.ktor.application.ApplicationCall
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
import io.titandata.models.Volume

fun Route.VolumesApi(services: ServiceLocator) {

    fun getRepoName(call: ApplicationCall): String {
        return call.parameters["repositoryName"] ?: throw IllegalArgumentException("missing repositoryName parameter")
    }

    fun getVolumeName(call: ApplicationCall): String {
        return call.parameters["volumeName"] ?: throw IllegalArgumentException("missing volumeName parameter")
    }

    route("/v1/repositories/{repositoryName}/volumes") {
        get {
            call.respond(services.volumes.listVolumes(getRepoName(call)))
        }

        post {
            val repo = getRepoName(call)
            val volume = call.receive(Volume::class)
            val result = services.volumes.createVolume(repo, volume)
            call.respond(HttpStatusCode.Created, result)
        }
    }

    route("/v1/repositories/{repositoryName}/volumes/{volumeName}") {
        get {
            val repo = getRepoName(call)
            val volumeName = getVolumeName(call)
            call.respond(services.volumes.getVolume(repo, volumeName))
        }

        delete {
            val repo = getRepoName(call)
            val volumeName = getVolumeName(call)
            services.volumes.deleteVolume(repo, volumeName)
            call.respond(HttpStatusCode.NoContent)
        }
    }

    route("/v1/repositories/{repositoryName}/volumes/{volumeName}/activate") {
        post {
            val repo = getRepoName(call)
            val volumeName = getVolumeName(call)
            services.volumes.activateVolume(repo, volumeName)
            call.respond(HttpStatusCode.OK)
        }
    }

    route("/v1/repositories/{repositoryName}/volumes/{volumeName}/deactivate") {
        post {
            val repo = getRepoName(call)
            val volumeName = getVolumeName(call)
            services.volumes.deactivateVolume(repo, volumeName)
            call.respond(HttpStatusCode.OK)
        }
    }

    route("/v1/repositories/{repositoryName}/volumes/{volumeName}/status") {
        get {
            val repo = getRepoName(call)
            val volumeName = getVolumeName(call)
            call.respond(services.volumes.getVolumeStatus(repo, volumeName))
        }
    }
}
