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
import io.titandata.ProviderModule
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters

/**
 * The remotes API is slightly more substantial because while we expose a more complete CRUD
 * interface over the REST API, we implement it at the provider level as a single list object
 * that is set as a unit. Most of the time we'll only have a single remote, and pushing the
 * complexity of managing different remotes by name down to the storage provider is not really
 * worth it.
 */
fun Route.RemotesApi(providers: ProviderModule) {

    fun getRepoName(call: ApplicationCall): String {
        return call.parameters["repositoryName"] ?: throw IllegalArgumentException("missing repositoryName parameter")
    }

    fun getRemoteName(call: ApplicationCall): String {
        return call.parameters["remoteName"] ?: throw IllegalArgumentException("missing remoteName parameter")
    }

    fun getCommitId(call: ApplicationCall): String {
        return call.parameters["commitId"] ?: throw IllegalArgumentException("missing commitId parameter")
    }

    route("/v1/repositories/{repositoryName}/remotes") {
        get {
            call.respond(providers.remotes.listRemotes(getRepoName(call)))
        }

        post {
            val repo = getRepoName(call)
            val remote = call.receive(Remote::class)
            providers.remotes.addRemote(repo, remote)
            call.respond(HttpStatusCode.Created, remote)
        }
    }

    route("/v1/repositories/{repositoryName}/remotes/{remoteName}") {
        get {
            val repo = getRepoName(call)
            val remoteName = getRemoteName(call)
            call.respond(providers.remotes.getRemote(repo, remoteName))
        }

        delete {
            val repo = getRepoName(call)
            val remoteName = getRemoteName(call)
            providers.remotes.removeRemote(repo, remoteName)
            call.respond(HttpStatusCode.NoContent)
        }

        post {
            val repo = getRepoName(call)
            val remoteName = getRemoteName(call)
            val remote = call.receive(Remote::class)
            providers.remotes.updateRemote(repo, remoteName, remote)
            call.respond(remote)
        }
    }

    route("/v1/repositories/{repositoryName}/remotes/{remoteName}/commits") {
        get {
            val repo = getRepoName(call)
            val remoteName = getRemoteName(call)
            val params = providers.gson.fromJson(call.request.headers["titan-remote-parameters"],
                    RemoteParameters::class.java)
            val tags = call.request.queryParameters.getAll("tag")
            call.respond(providers.remotes.listRemoteCommits(repo, remoteName, params, tags))
        }
    }

    route("/v1/repositories/{repositoryName}/remotes/{remoteName}/commits/{commitId}") {
        get {
            val repo = getRepoName(call)
            val remoteName = getRemoteName(call)
            val commitId = getCommitId(call)
            val params = providers.gson.fromJson(call.request.headers["titan-remote-parameters"],
                    RemoteParameters::class.java)
            call.respond(providers.remotes.getRemoteCommit(repo, remoteName, params, commitId))
        }
    }
}
