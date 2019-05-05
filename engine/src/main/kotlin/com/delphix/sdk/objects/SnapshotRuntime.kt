/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Runtime properties of a TimeFlow snapshot.
 */
open class SnapshotRuntime (
    open val provisionable: Boolean? = null,//True if this snapshot can be used as the basis for provisioning a new TimeFlow.
    override val type: String = "SnapshotRuntime"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "provisionable" to provisionable,
            "type" to type
        )
    }
}

