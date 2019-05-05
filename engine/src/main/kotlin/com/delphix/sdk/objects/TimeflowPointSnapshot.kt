/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * TimeFlow point based on a snapshot reference.
 */
open class TimeflowPointSnapshot (
    open val snapshot: String? = null,//Reference to the snapshot.
    override val type: String = "TimeflowPointSnapshot"
) : TimeflowPointParameters {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "snapshot" to snapshot,
            "type" to type
        )
    }
}

