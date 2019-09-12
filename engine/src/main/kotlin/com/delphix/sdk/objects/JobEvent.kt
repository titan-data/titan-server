/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Represents a job event object. This can either be a state change or a progress update.
 */
open class JobEvent (
    open val messageCommandOutput: String? = null,//Command output associated with the event, if applicable.
    open val messageDetails: String? = null,//Localized message details.
    open val messageAction: String? = null,//Localized message action.
    open val messageCode: String? = null,//Message ID associated with the event.
    open val eventType: String? = null,//Type of event.
    open val percentComplete: Int? = null,//Completion percentage.
    open val state: String? = null,//New state of the job.
    open val diagnoses: List<DiagnosisResult>,//Results of diagnostic checks run, if any, if the job failed.
    open val timestamp: String? = null,//Time the event occurred.
    override val type: String = "JobEvent"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "messageCommandOutput" to messageCommandOutput,
            "messageDetails" to messageDetails,
            "messageAction" to messageAction,
            "messageCode" to messageCode,
            "eventType" to eventType,
            "percentComplete" to percentComplete,
            "state" to state,
            "diagnoses" to diagnoses,
            "timestamp" to timestamp,
            "type" to type
        )
    }
}

