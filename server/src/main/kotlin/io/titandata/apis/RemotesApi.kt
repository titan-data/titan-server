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
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
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

    fun findRemote(repo: String, remote: String): Pair<MutableList<Remote>, Int?> {
        val remotes = providers.storage.getRemotes(repo).toMutableList()
        remotes.forEachIndexed { idx, r ->
            if (r.name == remote) {
                return Pair(remotes, idx)
            }
        }
        return Pair(remotes, null)
    }

    route("/v1/repositories/{repositoryName}/remotes") {
        get {
            call.respond(providers.storage.getRemotes(getRepoName(call)))
        }

        post {
            val repo = getRepoName(call)
            val remote = call.receive(Remote::class)
            val (remotes, idx) = findRemote(repo, remote.name)
            if (idx != null) {
                throw ObjectExistsException("remote '${remote.name}' exists for repository '$repo'")
            }
            remotes.add(remote)
            providers.storage.updateRemotes(repo, remotes)
            call.respond(HttpStatusCode.Created, remote)
        }
    }

    route("/v1/repositories/{repositoryName}/remotes/{remoteName}") {
        get {
            val repo = getRepoName(call)
            val remoteName = getRemoteName(call)
            val (remotes, idx) = findRemote(repo, remoteName)
            if (idx == null) {
                throw NoSuchObjectException("no such remote '$remoteName' in repository '$repo'")
            }
            call.respond(remotes[idx])
        }

        delete {
            val repo = getRepoName(call)
            val remoteName = getRemoteName(call)
            val (remotes, idx) = findRemote(repo, remoteName)
            if (idx == null) {
                throw NoSuchObjectException("no such remote '$remoteName' in repository '$repo'")
            }
            // TODO cancel any in-flight operations
            remotes.removeAt(idx)
            providers.storage.updateRemotes(repo, remotes)
            call.respond(HttpStatusCode.NoContent)
        }

        post {
            val repo = getRepoName(call)
            val remoteName = getRemoteName(call)
            val remote = call.receive(Remote::class)

            val (remotes, idx) = findRemote(repo, remoteName)
            if (idx == null) {
                throw NoSuchObjectException("no such remote '$remoteName' in repository '$repo'")
            }
            if (remoteName != remote.name) {
                val (_, newIdx) = findRemote(repo, remote.name)
                if (newIdx != null) {
                    throw ObjectExistsException("remote '$remoteName' already exists for repository '$repo'")
                }
            }
            remotes[idx] = remote
            providers.storage.updateRemotes(repo, remotes)
            call.respond(remote)
        }
    }

    route("/v1/repositories/{repositoryName}/remotes/{remoteName}/commits") {
        get {
            val repo = getRepoName(call)
            val remoteName = getRemoteName(call)
            val (remotes, idx) = findRemote(repo, remoteName)
            if (idx == null) {
                throw NoSuchObjectException("no such remote '$remoteName' in repository '$repo'")
            }
            val remote = remotes[idx]
            val params = providers.gson.fromJson(call.request.headers["titan-remote-parameters"],
                    RemoteParameters::class.java)
            call.respond(providers.remote(remote.provider).listCommits(remote, params))
        }
    }

    route("/v1/repositories/{repositoryName}/remotes/{remoteName}/commits/{commitId}") {
        get {
            val repo = getRepoName(call)
            val remoteName = getRemoteName(call)
            val commitId = getCommitId(call)
            val (remotes, idx) = findRemote(repo, remoteName)
            if (idx == null) {
                throw NoSuchObjectException("no such remote '$remoteName' in repository '$repo'")
            }
            val remote = remotes[idx]
            val params = providers.gson.fromJson(call.request.headers["titan-remote-parameters"],
                    RemoteParameters::class.java)
            val commit = providers.remote(remote.provider).getCommit(remote, commitId, params)
            call.respond(commit)
        }
    }
}
