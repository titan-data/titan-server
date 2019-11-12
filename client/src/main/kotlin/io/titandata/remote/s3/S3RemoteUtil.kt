/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.s3

import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.serialization.RemoteUtilProvider
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import java.net.URI

/**
 * The URI syntax for S3 remotes is:
 *
 *      s3://bucket[/object]
 *
 * The following properties are supported:
 *
 *      accessKey       AWS access key.
 *
 *      secretKey       AWS secret key.
 *
 *      region          AWS region.
 *
 * While all of these can be specified in the remote, best practices are to leave them blank, and have them pulled
 * from the user's environment at the time the operation request is made.
 */
class S3RemoteUtil : RemoteUtilProvider() {

    override fun parseUri(uri: URI, name: String, properties: Map<String, String>): Remote {
        val (username, password, bucket, port, path) = getConnectionInfo(uri)

        if (username != null) {
            throw IllegalArgumentException("Username cannot be specified for S3 remote")
        }

        if (password != null) {
            throw IllegalArgumentException("Password cannot be specified for S3 remote")
        }

        if (port != null) {
            throw IllegalArgumentException("Port cannot be specified for S3 remote")
        }

        if (bucket == null) {
            throw IllegalArgumentException("Missing bucket in S3 remote")
        }

        var accessKey : String? = null
        var secretKey : String? = null
        var region : String? = null
        for (p in properties.keys) {
            when {
                p == "accessKey" -> accessKey = properties[p]
                p == "secretKey" -> secretKey = properties[p]
                p == "region" -> region = properties[p]
                else -> throw IllegalArgumentException("Invalid SSH remote property '$p'")
            }
        }

        if ((accessKey == null && secretKey != null) || (accessKey != null && secretKey == null)) {
            throw IllegalArgumentException("Either both access key and secret key must be set, or neither in S3 remote")
        }

        val objectPath = when {
            path != null && path.startsWith("/") -> path.substring(1)
            else -> path
        }

        return Remote("s3", name, mapOf("bucket" to bucket, "path" to objectPath, "accessKey" to accessKey,
                "secretKey" to secretKey, "region" to region))
    }

    override fun toUri(remote: Remote): Pair<String, Map<String, String>> {
        var uri = "s3://${remote.properties["bucket"]}"
        if (remote.properties["path"] != null) {
            uri += "/${remote.properties["path"]}"
        }

        var properties = mutableMapOf<String, String>()
        if (remote.properties["accessKey"] != null) {
            properties["accessKey"] = remote.properties["accessKey"] as String
        }
        if (remote.properties["secretKey"] != null) {
            properties["secretKey"] = "*****"
        }
        if (remote.properties["region"] != null) {
            properties["region"] = remote.properties["region"] as String
        }

        return Pair(uri, properties)
    }

    override fun getParameters(remote: Remote): RemoteParameters {
        var accessKey = remote.properties["accessKey"] as String?
        var secretKey = remote.properties["secretKey"] as String?
        var sessionToken:String? = null
        if (accessKey == null || secretKey == null) {
            val creds = DefaultCredentialsProvider.create().resolveCredentials()
            if (creds == null) {
                throw IllegalArgumentException("Unable to determine AWS credentials")
            }
            accessKey = creds.accessKeyId()
            secretKey = creds.secretAccessKey()
            if (accessKey == null || secretKey == null) {
                throw IllegalArgumentException("Unable to determine AWS credentials")
            }

            if (creds is AwsSessionCredentials) {
                sessionToken = creds.sessionToken()
            }
        }

        var region = remote.properties["region"] as String?
        if (region == null) {
            region = DefaultAwsRegionProviderChain().region?.id()
        }

        return RemoteParameters("s3",
                mapOf("accessKey" to accessKey, "secretKey" to secretKey, "region" to region, "sessionToken" to sessionToken))
    }
}