/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * TimeFlow point based on a semantic reference.
 */
open class TimeflowPointSemantic (
    open val container: String? = null,//Reference to the container.
    open val location: String? = null,//A semantic description of a TimeFlow location.
    override val type: String = "TimeflowPointSemantic"
) : TimeflowPointParameters {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "container" to container,
            "location" to location,
            "type" to type
        )
    }
}

