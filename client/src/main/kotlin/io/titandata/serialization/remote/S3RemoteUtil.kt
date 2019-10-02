/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.serialization.remote

import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.S3Parameters
import io.titandata.models.S3Remote
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

        return S3Remote(name = name, bucket = bucket, path = objectPath, accessKey = accessKey,
                secretKey = secretKey, region = region)
    }

    override fun toUri(remote: Remote): Pair<String, Map<String, String>> {
        remote as S3Remote

        var uri = "s3://${remote.bucket}"
        if (remote.path != null) {
            uri += "/${remote.path}"
        }

        var properties = mutableMapOf<String, String>()
        if (remote.accessKey != null) {
            properties["accessKey"] = remote.accessKey as String
        }
        if (remote.secretKey != null) {
            properties["secretKey"] = "*****"
        }
        if (remote.region != null) {
            properties["region"] = remote.region as String
        }

        return Pair(uri, properties)
    }

    override fun getParameters(remote: Remote): RemoteParameters {
        remote as S3Remote

        var accessKey = remote.accessKey
        var secretKey = remote.secretKey
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

        var region = remote.region
        if (region == null) {
            region = DefaultAwsRegionProviderChain().region?.id()
        }

        return S3Parameters(accessKey = accessKey, secretKey = secretKey, region = region, sessionToken = sessionToken)
    }
}