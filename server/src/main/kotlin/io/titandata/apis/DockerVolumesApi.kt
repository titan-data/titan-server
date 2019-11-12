/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.apis

import io.ktor.application.call
import io.ktor.request.receive
import io.ktor.response.respond
import io.ktor.routing.Route
import io.ktor.routing.post
import io.ktor.routing.route
import io.titandata.ProviderModule
import io.titandata.models.PluginDescription
import io.titandata.models.docker.DockerVolumeCapabilities
import io.titandata.models.docker.DockerVolumeCapabilitiesResponse
import io.titandata.models.docker.DockerVolumeCreateRequest
import io.titandata.models.docker.DockerVolumeGetResponse
import io.titandata.models.docker.DockerVolumeListResponse
import io.titandata.models.docker.DockerVolumeMountRequest
import io.titandata.models.docker.DockerVolumePathResponse
import io.titandata.models.docker.DockerVolumeRequest
import io.titandata.models.docker.DockerVolumeResponse

fun Route.DockerVolumeApi(providers: ProviderModule) {

    /**
     * Volumes names are expressed as "repo/vol". This is a helper method to separate the
     * repository name and the volume name, throwing an exception if it's not well formed.
     */
    fun getVolumeName(name: String?): Pair<String, String> {
        name ?: throw IllegalArgumentException("volume name must be specified")
        val components = name.split("/")
        if (components.size != 2) {
            throw IllegalArgumentException("volume must name be of the form 'repository/volume'")
        }
        return Pair(components[0], components[1])
    }

    route("/VolumeDriver.Create") {
        post {
            val request = call.receive(DockerVolumeCreateRequest::class)
            val opts = request.opts ?: mapOf()
            val (repo, volname) = getVolumeName(request.name)
            providers.volumes.createVolume(repo, volname, opts)
            call.respond(DockerVolumeResponse())
        }
    }

    route("/VolumeDriver.Capabilities") {
        post {
            call.respond(DockerVolumeCapabilitiesResponse(capabilities = DockerVolumeCapabilities(scope = "local")))
        }
    }

    route("/Plugin.Activate") {
        post {
            call.respond(PluginDescription())
        }
    }

    route("/VolumeDriver.Get") {
        post {
            val request = call.receive(DockerVolumeRequest::class)
            val (repo, volname) = getVolumeName(request.name)
            val result = providers.volumes.getVolume(repo, volname)
            call.respond(DockerVolumeGetResponse(volume = result.copy(name = "$repo/$volname")))
        }
    }

    route("/VolumeDriver.Path") {
        post {
            val request = call.receive(DockerVolumeRequest::class)
            val (repo, volname) = getVolumeName(request.name)
            val result = providers.volumes.getVolume(repo, volname)
            call.respond(DockerVolumePathResponse(mountpoint = result.mountpoint))
        }
    }

    route("/VolumeDriver.List") {
        post {
            val result = providers.volumes.listAllVolumes()
            call.respond(DockerVolumeListResponse(volumes = result.toTypedArray()))
        }
    }

    route("/VolumeDriver.Mount") {
        post {
            val request = call.receive(DockerVolumeMountRequest::class)
            val (repo, volname) = getVolumeName(request.name)
            providers.volumes.mountVolume(repo, volname)
            val vol = providers.volumes.getVolume(repo, volname)
            call.respond(DockerVolumePathResponse(mountpoint = vol.mountpoint))
        }
    }

    route("/VolumeDriver.Remove") {
        post {
            val request = call.receive(DockerVolumeRequest::class)
            val (repo, volname) = getVolumeName(request.name)
            providers.volumes.deleteVolume(repo, volname)
            call.respond(DockerVolumeResponse())
        }
    }

    route("/VolumeDriver.Unmount") {
        post {
            val request = call.receive(DockerVolumeMountRequest::class)
            val (repo, volname) = getVolumeName(request.name)
            providers.volumes.unmountVolume(repo, volname)
            call.respond(DockerVolumeResponse())
        }
    }
}
