/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * TimeFlow points represent a unique point within a TimeFlow.
 */
interface TimeflowPoint : TypedObject {
    val location: String?//The TimeFlow location.
    val timeflow: String?//Reference to TimeFlow containing this point.
    val timestamp: String?//The logical time corresponding to the TimeFlow location.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
