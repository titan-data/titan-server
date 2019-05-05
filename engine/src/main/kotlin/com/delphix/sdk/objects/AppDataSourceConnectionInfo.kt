/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Contains information that can be used to connect to the application source.
 */
open class AppDataSourceConnectionInfo (
    open val path: String? = null,//The path where the application data is located on the host.
    open val host: String? = null,//The hostname or IP address of the host where the source resides.
    override val version: String? = null,//The database version string.
    override val type: String = "AppDataSourceConnectionInfo"
) : SourceConnectionInfo {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "path" to path,
            "host" to host,
            "version" to version,
            "type" to type
        )
    }
}

