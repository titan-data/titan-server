/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Parameters indicating a TimeFlow point to use as input to database operations.
 */
interface TimeflowPointParameters : TypedObject {
    override val type: String
    override fun toMap(): Map<String, Any?>
}
