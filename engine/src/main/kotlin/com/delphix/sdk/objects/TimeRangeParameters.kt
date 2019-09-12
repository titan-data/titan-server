/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The parameters to use as input for methods requiring a time range.
 */
open class TimeRangeParameters (
    open val startTime: String? = null,//The date at the beginning of the period.
    open val endTime: String? = null,//The date at the end of the period.
    override val type: String = "TimeRangeParameters"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "startTime" to startTime,
            "endTime" to endTime,
            "type" to type
        )
    }
}

