/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Runtime (non-persistent) properties of AppData TimeFlow snapshots.
 */
open class AppDataSnapshotRuntime (
    override val provisionable: Boolean? = null,//True if this snapshot can be used as the basis for provisioning a new TimeFlow.
    override val type: String = "AppDataSnapshotRuntime"
) : SnapshotRuntime(
    provisionable
){
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "provisionable" to provisionable,
            "type" to type
        )
    }
}
