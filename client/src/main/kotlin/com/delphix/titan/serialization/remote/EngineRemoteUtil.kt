package com.delphix.titan.serialization.remote

import com.delphix.titan.models.EngineParameters
import com.delphix.titan.models.EngineRemote
import com.delphix.titan.models.NopRemote
import com.delphix.titan.models.Remote
import com.delphix.titan.models.RemoteParameters
import com.delphix.titan.models.S3Parameters
import com.delphix.titan.models.SshParameters
import com.delphix.titan.models.SshRemote
import com.delphix.titan.serialization.RemoteUtil
import com.delphix.titan.serialization.RemoteUtilProvider
import java.io.File
import java.net.URI

/**
 * The URI syntax for engine remotes is:
 *
 *      engine://user[:password]@host/path
 *
 * For the purposes of the beta, we don't allow the configuration of the port, and always
 * connect over HTTP. If the password is not specified, then it will be up to the CLI to
 * provide the password as part of the EngineParameters object.
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

        return EngineRemote(name=name, username=username, password=password, address=host,
                repository=repo)
    }

    override fun toUri(remote: Remote): Pair<String, Map<String, String>> {
        remote as EngineRemote

        var uri = "engine://${remote.username}"
        if (remote.password != null) {
            uri += ":${remote.password}"
        }
        uri += "@${remote.address}/${remote.repository}"

        return Pair(uri, mapOf())
    }

    override fun getParameters(remote: Remote): RemoteParameters {
        remote as EngineRemote

        var password : String? = null
        if (remote.password == null) {
            val input = console?.readPassword("password: ")
                    ?: throw IllegalArgumentException("password required but no console available")
            password = String(input)
        }

        return EngineParameters(password = password)
    }
}