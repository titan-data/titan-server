/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.nop

import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.serialization.RemoteUtilProvider
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
        return Remote("nop", name)
    }

    override fun toUri(remote: Remote): Pair<String, Map<String, String>> {
        return Pair("nop", mapOf())
    }

    override fun getParameters(remote: Remote): RemoteParameters {
        return RemoteParameters("nop")
    }
}