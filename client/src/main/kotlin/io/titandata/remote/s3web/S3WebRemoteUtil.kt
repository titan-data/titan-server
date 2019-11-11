/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.s3web

import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
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
class S3WebRemoteUtil : RemoteUtilProvider() {

    override fun parseUri(uri: URI, name: String, properties: Map<String, String>): Remote {
        val (username, password, host, port, path) = getConnectionInfo(uri)

        if (username != null) {
            throw IllegalArgumentException("Username cannot be specified for S3 remote")
        }

        if (password != null) {
            throw IllegalArgumentException("Password cannot be specified for S3 remote")
        }

        if (host == null) {
            throw IllegalArgumentException("Missing bucket in S3 remote")
        }
        for (p in properties.keys) {
            when {
                else -> throw IllegalArgumentException("Invalid remote property '$p'")
            }
        }

        var url = "http://$host"
        if (port != null) {
            url += ":$port"
        }
        if (path != null) {
            url += "$path"
        }

        return S3WebRemote(name = name, url = url)
    }

    override fun toUri(remote: Remote): Pair<String, Map<String, String>> {
        remote as S3WebRemote

        return Pair(remote.url.replace("http", "s3web"), mapOf())
    }

    override fun getParameters(remote: Remote): RemoteParameters {
        return RemoteParameters("s3web")
    }
}