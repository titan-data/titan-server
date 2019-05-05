/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Result of a failed API call.
 */
open class ErrorResult (
    open val error: APIError? = null,//Specifics of the error that occurred during API call execution.
    override val status: String? = null,//Indicates whether an error occurred during the call.
    override val type: String = "ErrorResult"
) : CallResult {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "error" to error,
            "status" to status,
            "type" to type
        )
    }
}

