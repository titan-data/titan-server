/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The parameters to use as input to delete requests for MSSQL, PostgreSQL, AppData, ASE or MySQL.
 */
open class DeleteParameters (
    open val force: Boolean? = null,//Flag indicating whether to continue the operation upon failures.
    override val type: String = "DeleteParameters"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "force" to force,
            "type" to type
        )
    }
}

