/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The parameters to use as input to sync an AppData source.
 */
open class AppDataSyncParameters (
    open val resync: Boolean? = null,//Whether or not to force a non-incremental load of data prior to taking a snapshot.
    override val type: String = "AppDataSyncParameters"
) : SyncParameters(
){
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "resync" to resync,
            "type" to type
        )
    }
}
