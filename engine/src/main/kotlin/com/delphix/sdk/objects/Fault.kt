/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * A representation of a fault, with associated user object.
 */
open class Fault (
    open val severity: String? = null,//The severity of the fault event. This can either be CRITICAL or WARNING.
    open val targetName: String? = null,//The name of the faulted object at the time the fault was diagnosed.
    open val bundleID: String? = null,//A unique dot delimited identifier associated with the fault.
    open val description: String? = null,//Full description of the fault.
    open val title: String? = null,//Summary of the fault.
    open val target: String? = null,//The user-visible Delphix object that is faulted.
    open val dateResolved: String? = null,//The date when the fault was resolved.
    open val resolutionComments: String? = null,//A comment that describes the fault resolution.
    open val response: String? = null,//The automated response taken by the system.
    open val action: String? = null,//A suggested user action.
    open val dateDiagnosed: String? = null,//The date when the fault was diagnosed.
    open val targetObjectType: String? = null,//The user-visible Delphix object that is faulted.
    open val status: String? = null,//The status of the fault. This can be ACTIVE, RESOLVED or IGNORED.
    override val reference: String? = null,//The object reference.
    override val namespace: String? = null,//Alternate namespace for this object, for replicated and restored objects.
    override val type: String = "Fault"
) : PersistentObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "severity" to severity,
            "targetName" to targetName,
            "bundleID" to bundleID,
            "description" to description,
            "title" to title,
            "target" to target,
            "dateResolved" to dateResolved,
            "resolutionComments" to resolutionComments,
            "response" to response,
            "action" to action,
            "dateDiagnosed" to dateDiagnosed,
            "targetObjectType" to targetObjectType,
            "status" to status,
            "reference" to reference,
            "namespace" to namespace,
            "type" to type
        )
    }
}

