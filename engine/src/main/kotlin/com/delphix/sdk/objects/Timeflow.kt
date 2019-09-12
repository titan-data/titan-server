/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Data for a particular historical timeline within a database.
 */
interface Timeflow : NamedUserObject {
    val container: String?//Reference to the data container (database) for this TimeFlow.
    val creationType: String?//The source action that created the TimeFlow.
    val parentSnapshot: String?//Reference to the parent snapshot that serves as the provisioning base for this object. This may be different from the snapshot within the parent point, and is only present for virtual TimeFlows.
    val parentPoint: TimeflowPoint?//The origin point on the parent TimeFlow from which this TimeFlow was provisioned. This will not be present for TimeFlows derived from linked sources.
    override val name: String?//Object name.
    override val reference: String?//The object reference.
    override val namespace: String?//Alternate namespace for this object, for replicated and restored objects.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
