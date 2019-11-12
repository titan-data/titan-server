/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.nop

import io.titandata.remote.RemoteClient
import java.net.URI

/**
 * This is a really simple remote, that must always be "nop" with no additional URI trimmings.
 */
class NopRemoteUtil : RemoteClient {

    override fun getProvider(): String {
        return "nop"
    }

    override fun parseUri(uri: URI, additionalProperties: Map<String, String>): Map<String, Any> {
        if (uri.scheme != null && (uri.authority != null || uri.path != null)) {
            throw IllegalArgumentException("Malformed remote identifier")
        }
        for (p in additionalProperties) {
            throw IllegalArgumentException("Invalid property '${p.key}'")
        }

        return emptyMap()
    }

    override fun getParameters(remoteProperties: Map<String, Any>): Map<String, Any> {
        return emptyMap()
    }

    override fun toUri(properties: Map<String, Any>): Pair<String, Map<String, String>> {
        return Pair("nop", emptyMap())
    }
}