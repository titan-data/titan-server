/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Result of a successful API call.
 */
open class OKResult (
    open val result: Any? = null,//Result of the operation. This will be specific to the API being invoked.
    open val action: String? = null,//Reference to the action associated with the operation, if any.
    open val job: String? = null,//Reference to the job started by the operation, if any.
    override val status: String? = null,//Indicates whether an error occurred during the call.
    override val type: String = "OKResult"
) : CallResult {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "result" to result,
            "action" to action,
            "job" to job,
            "status" to status,
            "type" to type
        )
    }
}

