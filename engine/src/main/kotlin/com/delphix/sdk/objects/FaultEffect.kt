/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * An error affecting a user object whose root cause is a fault. A fault effect can only be resolved by resolving the fault which is its root cause.
 */
open class FaultEffect (
    open val severity: String? = null,//The severity of the fault effect. This can either be CRITICAL or WARNING.
    open val targetName: String? = null,//The name of the user-visible Delphix object that has a fault effect.
    open val response: String? = null,//The automated response taken by the system.
    open val bundleID: String? = null,//A unique dot delimited identifier associated with the fault effect.
    open val rootCause: String? = null,//The root cause of this fault effect. Resolving the fault effect can only occur by resolving its root cause.
    open val action: String? = null,//A suggested user action.
    open val causedBy: String? = null,//The cause of the fault effect, in case there is a chain of fault effects originating from the root cause which resulted in this effect.
    open val description: String? = null,//Full description of the fault effect.
    open val dateDiagnosed: String? = null,//The date when the root cause fault was diagnosed.
    open val title: String? = null,//Summary of the fault effect.
    open val target: String? = null,//The user-visible Delphix object that has a fault effect.
    override val reference: String? = null,//The object reference.
    override val namespace: String? = null,//Alternate namespace for this object, for replicated and restored objects.
    override val type: String = "FaultEffect"
) : PersistentObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "severity" to severity,
            "targetName" to targetName,
            "response" to response,
            "bundleID" to bundleID,
            "rootCause" to rootCause,
            "action" to action,
            "causedBy" to causedBy,
            "description" to description,
            "dateDiagnosed" to dateDiagnosed,
            "title" to title,
            "target" to target,
            "reference" to reference,
            "namespace" to namespace,
            "type" to type
        )
    }
}

