/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.s3

import io.titandata.remote.RemoteClient
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
class S3RemoteUtil : RemoteClient {
     private val util = RemoteUtilProvider()

     override fun getProvider(): String {
         return "s3"
     }

     override fun parseUri(uri: URI, additionalProperties: Map<String, String>): Map<String, Any> {
         val (username, password, bucket, port, path) = util.getConnectionInfo(uri)

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
         for (p in additionalProperties.keys) {
             when {
                 p == "accessKey" -> accessKey = additionalProperties[p]
                 p == "secretKey" -> secretKey = additionalProperties[p]
                 p == "region" -> region = additionalProperties[p]
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

         val result = mutableMapOf("bucket" to bucket)
         if (objectPath != null) {
             result["path"] = objectPath
         }
         if (accessKey != null) {
             result["accessKey"] = accessKey
         }
         if (secretKey != null) {
             result["secretKey"] = secretKey
         }
         if (region != null) {
             result["region"] = region
         }
         return result
     }

     override fun toUri(properties: Map<String, Any>): Pair<String, Map<String, String>> {
         var uri = "s3://${properties["bucket"]}"
         if (properties["path"] != null) {
             uri += "/${properties["path"]}"
         }

         var params = mutableMapOf<String, String>()
         if (properties["accessKey"] != null) {
             params["accessKey"] = properties["accessKey"] as String
         }
         if (properties["secretKey"] != null) {
             params["secretKey"] = "*****"
         }
         if (properties["region"] != null) {
             params["region"] = properties["region"] as String
         }

         return Pair(uri, params)
     }

     override fun getParameters(remoteProperties: Map<String, Any>): Map<String, Any> {
         var accessKey = remoteProperties["accessKey"] as String?
         var secretKey = remoteProperties["secretKey"] as String?
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

         var region = remoteProperties["region"] as String?
         if (region == null) {
             region = DefaultAwsRegionProviderChain().region?.id()
         }

         val result = mutableMapOf<String, String>()
         result["accessKey"] = accessKey
         result["secretKey"] = secretKey
         if (region != null) {
             result["region"] = region
         }
         if (sessionToken != null) {
             result["sessionToken"] = sessionToken
         }
         return result
     }
}