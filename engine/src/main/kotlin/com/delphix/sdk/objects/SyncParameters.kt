/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The parameters to use as input to sync requests.
 */
open class SyncParameters (
    override val type: String = "SyncParameters"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "type" to type
        )
    }
}

