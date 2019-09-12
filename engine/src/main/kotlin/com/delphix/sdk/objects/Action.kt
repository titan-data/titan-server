/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Represents an action, a permanent record of activity on the server.
 */
open class Action (
    open val failureAction: String? = null,//Action to be taken to resolve the failure.
    open val workSource: String? = null,//Origin of the work that caused the action.
    open val userAgent: String? = null,//Name of client software used to initiate the action.
    open val title: String? = null,//Action title.
    open val failureMessageCode: String? = null,//Message ID associated with the event.
    open val actionType: String? = null,//Action type.
    open val report: String? = null,//Report of progress and warnings for some actions.
    open val details: String? = null,//Plain text description of the action.
    open val startTime: String? = null,//The time the action occurred. For long running processes, this represents the starting time.
    open val endTime: String? = null,//The time the action completed.
    open val state: String? = null,//State of the action.
    open val parentAction: String? = null,//The parent action of this action.
    open val user: String? = null,//User who initiated the action.
    open val workSourceName: String? = null,//Name of user or policy that initiated the action.
    open val failureDescription: String? = null,//Details of the action failure.
    override val reference: String? = null,//The object reference.
    override val namespace: String? = null,//Alternate namespace for this object, for replicated and restored objects.
    override val type: String = "Action"
) : PersistentObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "failureAction" to failureAction,
            "workSource" to workSource,
            "userAgent" to userAgent,
            "title" to title,
            "failureMessageCode" to failureMessageCode,
            "actionType" to actionType,
            "report" to report,
            "details" to details,
            "startTime" to startTime,
            "endTime" to endTime,
            "state" to state,
            "parentAction" to parentAction,
            "user" to user,
            "workSourceName" to workSourceName,
            "failureDescription" to failureDescription,
            "reference" to reference,
            "namespace" to namespace,
            "type" to type
        )
    }
}

