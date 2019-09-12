/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Description of an error encountered during an API call.
 */
open class APIError (
    open val commandOutput: String? = null,//Extra output, often from a script or other external process, that may give more insight into the cause of this error.
    open val action: String? = null,//Action to be taken by the user, if any, to fix the underlying problem.
    open val details: Any? = null,//For validation errors, a map of fields to APIError objects. For all other errors, a string with further details of the error.
    open val id: String? = null,//A stable identifier for the class of error encountered.
    open val diagnoses: List<DiagnosisResult>,//Results of diagnostic checks run, if any, if the job failed.
    override val type: String = "APIError"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "commandOutput" to commandOutput,
            "action" to action,
            "details" to details,
            "id" to id,
            "diagnoses" to diagnoses,
            "type" to type
        )
    }
}

