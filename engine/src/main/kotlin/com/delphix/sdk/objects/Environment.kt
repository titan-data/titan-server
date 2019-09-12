/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.sdk.objects

import org.json.JSONObject

data class Environment(
    val type: String,
    val name: String,
    val aseHostEnvironmentParameters: String,
    val description: String,
    val enabled: Boolean,
    val host: String,
    val logCollectionEnabled: Boolean,
    val primaryUser: String,
    val reference: String
){
    companion object {
        @JvmStatic
        fun fromJson(node: JSONObject): Environment {
            val environment = Environment(
                node.optString("type"),
                node.optString("name"),
                node.optString("aseHostEnvironmentParameters"),
                node.optString("description"),
                node.optBoolean("enabled"),
                node.optString("host"),
                node.optBoolean("logCollectionEnabled"),
                node.optString("primaryUser"),
                node.optString("reference")
            )
            return environment
        }
    }
}
