package com.delphix.titan.serialization.remote

import com.delphix.titan.models.Remote
import com.delphix.titan.models.RemoteParameters
import com.delphix.titan.models.SshParameters
import com.delphix.titan.models.SshRemote
import com.delphix.titan.serialization.RemoteUtil
import com.delphix.titan.serialization.RemoteUtilProvider
import java.io.File
import java.net.URI

/**
 * The URI syntax for SSH remotes is:
 *
 *      ssh://user[:password]@host[:port]/path
 *
 * If the password is not specified, then it is up to the client to prompt the user for it
 * and pass the value each time a request is made. The following properties are supported:
 *
 *      keyFile     Path, on the user's machine, of an SSH key file to use. If specified,
 *                  then the client must pass the contents of this file with each request.
 *                  This is mutually exclusive with a password.
 *
 * The URI form doesn't allow relative paths, but users may be used to scp and other tools
 * that support short-form "user@host:relative/path". While there is no official standard
 * for the URI form, the accepted form is typically "/~/relative/path". We handle that here
 * and convert it to a relative path.
 */
class SshRemoteUtil : RemoteUtilProvider() {

    private val console = System.console()

    override fun parseUri(uri: URI, name: String, properties: Map<String, String>): Remote {
        val (username, password, host, port, rawPath) = getConnectionInfo(uri)

        if (username == null) {
            throw IllegalArgumentException("Missing username in SSH remote")
        }

        if (rawPath == null) {
            throw IllegalArgumentException("Missing path in SSH remote")
        }

        if (host == null) {
            throw IllegalArgumentException("Missing host in SSH remote")
        }

        val path = when {
            rawPath.startsWith("/~/") -> rawPath.substring(3)
            else -> rawPath
        }

        val keyFile = properties["keyFile"]

        if (keyFile != null && password != null) {
            throw IllegalArgumentException("Both password and key file cannot be specified for SSH remote")
        }

        for (p in properties.keys) {
            if (p != "keyFile") {
                throw IllegalArgumentException("Invalid SSH remote property '$p'")
            }
        }

        return SshRemote(name=name, username=username, password=password, address=host,
                port=port, path=path, keyFile=keyFile)
    }

    override fun toUri(remote: Remote): Pair<String, Map<String, String>> {
        remote as SshRemote

        var uri = "ssh://${remote.username}"
        if (remote.password != null) {
            uri += ":*****"
        }
        uri += "@${remote.address}"
        if (remote.port != null) {
            uri += ":${remote.port}"
        }
        if (!remote.path.startsWith("/")) {
            uri += "/~/"
        }
        uri += "${remote.path}"

        val properties = mutableMapOf<String, String>()
        if (remote.keyFile != null) {
            properties["keyFile"] = remote.keyFile as String
        }

        return Pair(uri, properties)
    }

    override fun getParameters(remote: Remote): RemoteParameters {
        remote as SshRemote

        var key : String? = null
        if (remote.keyFile != null) {
            key = File(remote.keyFile).readText()
        }

        var password : String? = null
        if (remote.password == null && remote.keyFile == null) {
            val input = console?.readPassword("password: ")
                    ?: throw IllegalArgumentException("password required but no console available")
            password = String(input)
        }

        return SshParameters(key=key, password = password)
    }
}