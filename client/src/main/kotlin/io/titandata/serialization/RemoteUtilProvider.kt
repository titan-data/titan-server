/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.serialization

import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import java.net.URI

class RemoteUtilProvider {

    data class ConnectionInfo(val username: String?, val password: String?,
                                      val host: String?, val port: Int?,
                                      val path: String?)

    fun getConnectionInfo(uri: URI) : ConnectionInfo {
        if (uri.scheme == null) {
            throw IllegalArgumentException("Malformed remote identifier")
        }
        var username = uri.userInfo
        var password : String? = null
        if (uri.userInfo != null && uri.userInfo.contains(":")) {
            username = uri.userInfo.substringBefore(":")
            password = uri.userInfo.substringAfter(":")
        }

        val host = when (uri.host) {
            "" -> null
            else -> uri.host
        }

        val port = when (uri.port) {
            -1 -> null
            else -> uri.port
        }

        val path = when (uri.path) {
            "" -> null
            else -> uri.path
        }

        return ConnectionInfo(username, password, host, port, path)
    }
}
