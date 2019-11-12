/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.engine

import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
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
class EngineRemoteUtil : RemoteUtilProvider() {

    private val console = System.console()

    override fun parseUri(uri: URI, name: String, properties: Map<String, String>): Remote {
        val (username, password, host, port, path) = getConnectionInfo(uri)

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

        for (p in properties.keys) {
            throw IllegalArgumentException("Invalid engine remote property '$p'")
        }

        return Remote("engine", name, mapOf("username" to username, "password" to password, "address" to host,
                "repository" to repo))
    }

    override fun toUri(remote: Remote): Pair<String, Map<String, String>> {
        val props = remote.properties
        var uri = "engine://${props["username"]}"
        if (props["password"] != null) {
            uri += ":${props["password"]}"
        }
        uri += "@${props["address"]}/${props["repository"]}"

        return Pair(uri, mapOf())
    }

    override fun getParameters(remote: Remote): RemoteParameters {
        var password : String? = null
        if (remote.properties["password"] == null) {
            val input = console?.readPassword("password: ")
                    ?: throw IllegalArgumentException("password required but no console available")
            password = String(input)
        }

        return RemoteParameters("engine", mapOf("password" to password))
    }
}