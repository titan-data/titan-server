package com.delphix.titan.serialization.remote

import com.delphix.titan.models.NopParameters
import com.delphix.titan.models.NopRemote
import com.delphix.titan.models.Remote
import com.delphix.titan.models.RemoteParameters
import com.delphix.titan.models.SshRemote
import com.delphix.titan.serialization.RemoteUtil
import com.delphix.titan.serialization.RemoteUtilProvider
import java.net.URI

/**
 * This is a really simple remote, that must always be "nop" with no additional URI trimmings.
 */
class NopRemoteUtil : RemoteUtilProvider() {

    override fun parseUri(uri: URI, name: String, properties: Map<String, String>): Remote {
        if (uri.scheme != null && (uri.authority != null || uri.path != null)) {
            throw IllegalArgumentException("Malformed remote identifier")
        }
        for (p in properties) {
            throw IllegalArgumentException("Invalid property '${p.key}'")
        }
        return NopRemote(name=name)
    }

    override fun toUri(remote: Remote): Pair<String, Map<String, String>> {
        return Pair("nop", mapOf())
    }

    override fun getParameters(remote: Remote): RemoteParameters {
        return NopParameters()
    }
}