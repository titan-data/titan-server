/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Describes an API version.
 */
open class APIVersion (
    open val major: Int? = null,//Major API version number.
    open val minor: Int? = null,//Minor API version number.
    open val micro: Int? = null,//Micro API version number.
    override val type: String = "APIVersion"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "major" to major,
            "minor" to minor,
            "micro" to micro,
            "type" to type
        )
    }
}

