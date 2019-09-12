/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * TimeFlow point based on a database-specific identifier (SCN, LSN, etc).
 */
open class TimeflowPointLocation (
    open val location: String? = null,//The TimeFlow location.
    open val timeflow: String? = null,//Reference to TimeFlow containing this location.
    override val type: String = "TimeflowPointLocation"
) : TimeflowPointParameters {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "location" to location,
            "timeflow" to timeflow,
            "type" to type
        )
    }
}

