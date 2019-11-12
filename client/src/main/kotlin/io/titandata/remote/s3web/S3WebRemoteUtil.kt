/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.s3web

import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.remote.RemoteClient
import io.titandata.serialization.RemoteUtilProvider
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import java.net.URI

/**
 * The URI syntax for S3 web remotes is to basically replace the "s3web" portion with "http".
 *
 *      s3://host[/path]
 *
 * Currently, we always use HTTP to access the resources, though a parameter could be provided to use HTTPS instead
 * if needed.
 */
class S3WebRemoteUtil : RemoteClient {
    private val util = RemoteUtilProvider()

    override fun getProvider(): String {
        return "s3web"
    }

    override fun parseUri(uri: URI, additionalProperties: Map<String, String>): Map<String, Any> {
        val (username, password, host, port, path) = util.getConnectionInfo(uri)

        if (username != null) {
            throw IllegalArgumentException("Username cannot be specified for S3 remote")
        }

        if (password != null) {
            throw IllegalArgumentException("Password cannot be specified for S3 remote")
        }

        if (host == null) {
            throw IllegalArgumentException("Missing bucket in S3 remote")
        }
        for (p in additionalProperties.keys) {
            throw IllegalArgumentException("Invalid remote property '$p'")
        }

        var url = "http://$host"
        if (port != null) {
            url += ":$port"
        }
        if (path != null) {
            url += "$path"
        }

        return mapOf("url" to url)
    }

    override fun getParameters(remoteProperties: Map<String, Any>): Map<String, Any> {
        return emptyMap()
    }

    override fun toUri(properties: Map<String, Any>): Pair<String, Map<String, String>> {
        val url = properties["url"] as String
        return Pair(url.replace("http", "s3web"), emptyMap())
    }
}