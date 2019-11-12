/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.ssh

import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.serialization.RemoteUtilProvider
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

        return Remote("ssh", name, mapOf("username" to username, "password" to password, "address" to host,
                "port" to port, "path" to path, "keyFile" to keyFile))
    }

    override fun toUri(remote: Remote): Pair<String, Map<String, String>> {
        val props = remote.properties
        var uri = "ssh://${props["username"]}"
        if (props["password"] != null) {
            uri += ":*****"
        }
        uri += "@${props["address"]}"
        if (props["port"] != null) {
            uri += ":${props["port"]}"
        }
        if (!(props["path"] as String).startsWith("/")) {
            uri += "/~/"
        }
        uri += "${props["path"]}"

        val properties = mutableMapOf<String, String>()
        if (props["keyFile"] != null) {
            properties["keyFile"] = props["keyFile"] as String
        }

        return Pair(uri, properties)
    }

    override fun getParameters(remote: Remote): RemoteParameters {
        val props = remote.properties
        var key : String? = null
        if (props["keyFile"] != null) {
            key = File(props["keyFile"] as String).readText()
        }

        var password : String? = null
        if (props["password"] == null && props["keyFile"] == null) {
            val input = console?.readPassword("password: ")
                    ?: throw IllegalArgumentException("password required but no console available")
            password = String(input)
        }

        return RemoteParameters("ssh", mapOf("key" to key, "password" to password))
    }
}