/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Details from a diagnosis check that was run due to a failed operation.
 */
open class DiagnosisResult (
    open val failure: Boolean? = null,//True if this was a check that did not pass.
    open val messageCode: String? = null,//Message code associated with the event.
    open val message: String? = null,//Localized message.
    override val type: String = "DiagnosisResult"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "failure" to failure,
            "messageCode" to messageCode,
            "message" to message,
            "type" to type
        )
    }
}

