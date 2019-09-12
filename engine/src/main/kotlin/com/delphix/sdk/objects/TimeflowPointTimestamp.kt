/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * TimeFlow point based on a timestamp.
 */
open class TimeflowPointTimestamp (
    open val timeflow: String? = null,//Reference to TimeFlow containing this point.
    open val timestamp: String? = null,//The logical time corresponding to the TimeFlow location.
    override val type: String = "TimeflowPointTimestamp"
) : TimeflowPointParameters {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "timeflow" to timeflow,
            "timestamp" to timestamp,
            "type" to type
        )
    }
}

