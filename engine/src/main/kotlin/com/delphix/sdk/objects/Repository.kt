/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.sdk.objects

import org.json.JSONObject

data class Repository(
    val type: String,
    val reference: String,
    val name: String,
    val environment: String,
    val linkingEnabled: Boolean,
    val parameters: String = "",
    val provisioningEnabled: Boolean,
    val staging: Boolean,
    val toolkit: String = "",
    val version: String
){
    companion object {
        @JvmStatic
        fun fromJson(node: JSONObject): Repository {
            val repository = Repository(
                node.get("type").toString(),
                node.get("reference").toString(),
                node.get("name").toString(),
                node.get("environment").toString(),
                node.getBoolean("linkingEnabled"),
                node.optString("parameters"),
                node.getBoolean("provisioningEnabled"),
                node.getBoolean("staging"),
                node.optString("toolkit"),
                node.get("version").toString()
            )
            return repository
        }
    }
}

/*
{
"provisioningEnabled":true,
"discovered":true,
"segmentSize":16777216,
"bits":64,
"type":"PgSQLInstall",
"version":"9.6.11",
"reference":"PGSQL_INSTALL-2",
"environment":"UNIX_HOST_ENVIRONMENT-4",
"namespace":null,
"name":"/usr/pgsql-9.6",
"variant":"PostgreSQL",
"installationPath":"/usr/pgsql-9.6",
"linkingEnabled":true,
"staging":false
}
*/
