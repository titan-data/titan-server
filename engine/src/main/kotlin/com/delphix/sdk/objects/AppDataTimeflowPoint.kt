/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * A unique point within an AppData TimeFlow.
 */
open class AppDataTimeflowPoint (
    override val location: String? = null,//The TimeFlow location.
    override val timeflow: String? = null,//Reference to TimeFlow containing this point.
    override val timestamp: String? = null,//The logical time corresponding to the TimeFlow location.
    override val type: String = "AppDataTimeflowPoint"
) : TimeflowPoint {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "location" to location,
            "timeflow" to timeflow,
            "timestamp" to timestamp,
            "type" to type
        )
    }
}

