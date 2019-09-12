/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The parameters to use as input to disable a MSSQL, PostgreSQL, AppData, ASE or MySQL source.
 */
open class SourceDisableParameters (
    open val attemptCleanup: Boolean? = null,//Whether to attempt a cleanup of the database from the environment before the disable.
    override val type: String = "SourceDisableParameters"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "attemptCleanup" to attemptCleanup,
            "type" to type
        )
    }
}

