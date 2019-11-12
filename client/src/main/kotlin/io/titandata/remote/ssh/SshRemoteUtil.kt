/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.ssh

import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.remote.RemoteClient
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
class SshRemoteUtil : RemoteClient {
    private val console = System.console()
    private val util = RemoteUtilProvider()

    override fun getProvider(): String {
        return "ssh"
    }

    override fun parseUri(uri: URI, additionalProperties: Map<String, String>): Map<String, Any> {
        val (username, password, host, port, rawPath) = util.getConnectionInfo(uri)

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

        val keyFile = additionalProperties["keyFile"]

        if (keyFile != null && password != null) {
            throw IllegalArgumentException("Both password and key file cannot be specified for SSH remote")
        }

        for (p in additionalProperties.keys) {
            if (p != "keyFile") {
                throw IllegalArgumentException("Invalid SSH remote property '$p'")
            }
        }

        val result = mutableMapOf<String, Any>("username" to username, "address" to host, "path" to path)
        if (password != null) {
            result["password"] = password
        }
        if (port != null) {
            result["port"] = port
        }
        if (keyFile != null) {
            result["keyFile"] = keyFile
        }
        return result
    }

    override fun toUri(properties: Map<String, Any>): Pair<String, Map<String, String>> {
        var uri = "ssh://${properties["username"]}"
        if (properties["password"] != null) {
            uri += ":*****"
        }
        uri += "@${properties["address"]}"
        if (properties["port"] != null) {
            uri += ":${properties["port"]}"
        }
        if (!(properties["path"] as String).startsWith("/")) {
            uri += "/~/"
        }
        uri += "${properties["path"]}"

        val props = mutableMapOf<String, String>()
        if (properties["keyFile"] != null) {
            props["keyFile"] = properties["keyFile"] as String
        }

        return Pair(uri, props)
    }

    override fun getParameters(remoteProperties: Map<String, Any>): Map<String, Any> {
        var key : String? = null
        if (remoteProperties["keyFile"] != null) {
            key = File(remoteProperties["keyFile"] as String).readText()
        }

        var password : String? = null
        if (remoteProperties["password"] == null && remoteProperties["keyFile"] == null) {
            val input = console?.readPassword("password: ")
                    ?: throw IllegalArgumentException("password required but no console available")
            password = String(input)
        }

        val result = mutableMapOf<String, String>()
        if (key != null) {
            result["key"] = key
        }
        if (password != null) {
            result["password"] = password
        }
        return result
    }
}