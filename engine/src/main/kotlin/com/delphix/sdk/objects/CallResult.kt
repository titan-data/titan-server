/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Result of an API call.
 */
interface CallResult : TypedObject {
    val status: String?//Indicates whether an error occurred during the call.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
