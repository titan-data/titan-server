/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Represents a job object.
 */
open class Job (
    open val targetName: String? = null,//A cached copy of the target object name.
    open val cancelable: Boolean? = null,//Whether this job can be canceled.
    open val jobState: String? = null,//State of the job.
    open val queued: Boolean? = null,//Whether this job is waiting for resources to be available for its execution.
    open val updateTime: String? = null,//Time the job was last updated.
    open val percentComplete: Int? = null,//Completion percentage. This value is a copy of the last event's percentComplete. It will be 0 if there are no job events or if the events field is not populated while fetching the job.
    open val title: String? = null,//Title of the job.
    open val parentActionState: String? = null,//State of this job's parent action. This value is populated only if the job is fetched via the plain get API call.
    open val target: String? = null,//Object reference of the target.
    open val actionType: String? = null,//Action type of the Job.
    open val suspendable: Boolean? = null,//Whether this job can be suspended.
    open val emailAddresses: List<String>,//Email addresses to be notified on job notification alerts.
    open val startTime: String? = null,//Time the job was created. Note that this is not the time when the job started executing.
    open val parentAction: String? = null,//This job's parent action.
    open val targetObjectType: String? = null,//Object type of the target.
    open val user: String? = null,//User that initiated the action.
    open val events: List<JobEvent>,//A list of time-sorted past JobEvent objects associated with this job.
    override val name: String? = null,//Object name.
    override val reference: String? = null,//The object reference.
    override val namespace: String? = null,//Alternate namespace for this object, for replicated and restored objects.
    override val type: String = "Job"
) : NamedUserObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "targetName" to targetName,
            "cancelable" to cancelable,
            "jobState" to jobState,
            "queued" to queued,
            "updateTime" to updateTime,
            "percentComplete" to percentComplete,
            "title" to title,
            "parentActionState" to parentActionState,
            "target" to target,
            "actionType" to actionType,
            "suspendable" to suspendable,
            "emailAddresses" to emailAddresses,
            "startTime" to startTime,
            "parentAction" to parentAction,
            "targetObjectType" to targetObjectType,
            "user" to user,
            "events" to events,
            "name" to name,
            "reference" to reference,
            "namespace" to namespace,
            "type" to type
        )
    }
}

