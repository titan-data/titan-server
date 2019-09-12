/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * TimeFlow representing historical data for a particular timeline within a data container.
 */
open class AppDataTimeflow (
    override val parentPoint: TimeflowPoint? = null,//The origin point on the parent TimeFlow from which this TimeFlow was provisioned. This will not be present for TimeFlows derived from linked sources.
    override val container: String? = null,//Reference to the data container (database) for this TimeFlow.
    override val creationType: String? = null,//The source action that created the TimeFlow.
    override val parentSnapshot: String? = null,//Reference to the parent snapshot that serves as the provisioning base for this object. This may be different from the snapshot within the parent point, and is only present for virtual TimeFlows.
    override val name: String? = null,//Object name.
    override val reference: String? = null,//The object reference.
    override val namespace: String? = null,//Alternate namespace for this object, for replicated and restored objects.
    override val type: String = "AppDataTimeflow"
) : AppDataBaseTimeflow {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "parentPoint" to parentPoint,
            "container" to container,
            "creationType" to creationType,
            "parentSnapshot" to parentSnapshot,
            "name" to name,
            "reference" to reference,
            "namespace" to namespace,
            "type" to type
        )
    }
}

