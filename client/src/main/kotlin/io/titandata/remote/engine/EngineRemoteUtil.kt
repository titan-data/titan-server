/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.engine

import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.remote.RemoteClient
import io.titandata.serialization.RemoteUtilProvider
import java.net.URI

/**
 * The URI syntax for engine remotes is:
 *
 *      engine://user[:password]@host/path
 *
 * For the purposes of the beta, we don't allow the configuration of the port, and always
 * connect over HTTP. If the password is not specified, then it will be up to the CLI to
 * provide the password as part of the properties.
 */
class EngineRemoteUtil : RemoteClient {
    private val console = System.console()
    private val util = RemoteUtilProvider()

    override fun getProvider(): String {
        return "engine"
    }

    override fun parseUri(uri: URI, additionalProperties: Map<String, String>): Map<String, Any> {
        val (username, password, host, port, path) = util.getConnectionInfo(uri)

        if (port != null) {
            throw IllegalArgumentException("Port cannot be specified for engine remotes")
        }

        if (username == null) {
            throw IllegalArgumentException("Missing username in engine remote")
        }

        if (path == null) {
            throw IllegalArgumentException("Missing repository name in engine remote")
        }

        if (host == null) {
            throw IllegalArgumentException("Missing host in engine remote")
        }

        // Chop leading slash
        val repo = path.substring(1)
        if (repo == "") {
            throw IllegalArgumentException("Missing repository name in engine remote")
        }

        for (p in additionalProperties.keys) {
            throw IllegalArgumentException("Invalid engine remote property '$p'")
        }

        val result = mutableMapOf("username" to username, "address" to host, "repository" to repo)
        if (password != null) {
            result["password"] = password
        }
        return result
    }

    override fun toUri(properties: Map<String, Any>): Pair<String, Map<String, String>> {
        var uri = "engine://${properties["username"]}"
        if (properties["password"] != null) {
            uri += ":${properties["password"]}"
        }
        uri += "@${properties["address"]}/${properties["repository"]}"

        return Pair(uri, mapOf())
    }

    override fun getParameters(remoteProperties: Map<String, Any>): Map<String, Any> {
        val result = mutableMapOf<String, String>()
        if (remoteProperties["password"] == null) {
            val input = console?.readPassword("password: ")
                    ?: throw IllegalArgumentException("password required but no console available")
            result["password"] = String(input)
        }

        return result
    }
}