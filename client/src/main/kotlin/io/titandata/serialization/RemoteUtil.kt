/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.serialization

import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import java.net.URI
import java.net.URISyntaxException
import java.util.ServiceLoader
import io.titandata.remote.RemoteClient


class RemoteUtil {

    private val remoteUtil : Map<String, RemoteClient>

    private val loader = ServiceLoader.load(RemoteClient::class.java)

    init {
        val providers = mutableMapOf<String, RemoteClient>()
        loader.forEach {
            if (it != null) {
                providers[it.getProvider()] = it
            }
        }
        remoteUtil = providers.toMap()
    }

    fun parseUri(uriString: String, name: String, properties: Map<String, String>) : Remote {
        try {
            val uri = URI(uriString)

            val provider = uri.scheme ?: uriString

            if (uri.query != null || uri.fragment != null) {
                throw IllegalArgumentException("Malformed remote identifier, query and fragments are not allowed")
            }

            val remoteClient = remoteUtil[provider]
                    ?: throw IllegalArgumentException("Unknown remote provider or malformed remote identifier '$provider'")

            return Remote(
                    name = name,
                    provider = provider,
                    properties = remoteClient.parseUri(uri, properties)
            )

        } catch (e: URISyntaxException) {
            throw IllegalArgumentException("Invalid URI syntax", e)
        }
    }

    fun toUri(remote: Remote) : Pair<String, Map<String, String>> {
        val remoteClient = remoteUtil[remote.provider]
                ?: throw IllegalArgumentException("Unknown remote provider '${remote.provider}'")

        return remoteClient.toUri(remote.properties)
    }

    fun getParameters(remote: Remote) : RemoteParameters {
        val remoteClient = remoteUtil[remote.provider]
                ?: throw IllegalArgumentException("Unknown remote provider '${remote.provider}'")

        return RemoteParameters(remoteClient.getProvider(), remoteClient.getParameters(remote.properties))
    }
}
