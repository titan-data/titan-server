/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Runtime (non-persistent) properties of an AppData source.
 */
open class AppDataSourceRuntime (
    override val accessible: Boolean? = null,//True if the source is JDBC accessible. If false then no properties can be retrieved.
    override val databaseSize: Int? = null,//Size of the database in bytes.
    override val enabled: String? = null,//Status indicating whether the source is enabled. A source has a 'PARTIAL' status if its sub-sources are not all enabled.
    override val notAccessibleReason: String? = null,//The reason why the source is not JDBC accessible.
    override val status: String? = null,//Status of the source. 'Unknown' if all attempts to connect to the source failed.
    override val type: String = "AppDataSourceRuntime"
) : SourceRuntime {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "accessible" to accessible,
            "databaseSize" to databaseSize,
            "enabled" to enabled,
            "notAccessibleReason" to notAccessibleReason,
            "status" to status,
            "type" to type
        )
    }
}

