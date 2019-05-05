/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Snapshot of an AppData TimeFlow.
 */
open class AppDataSnapshot (
    override val firstChangePoint: TimeflowPoint? = null,//The location within the parent TimeFlow at which this snapshot was initiated. No recovery earlier than this point needs to be applied in order to provision a database from this snapshot. If "firstChangePoint" equals "latestChangePoint", then no recovery needs to be applied in order to provision a database.
    open val metadata: Json? = null,//The JSON payload conforming to the DraftV4 schema based on the type of application data being manipulated.
    override val latestChangePoint: TimeflowPoint? = null,//The location of the snapshot within the parent TimeFlow represented by this snapshot.
    override val runtime: SnapshotRuntime? = null,//Runtime properties of the snapshot.
    override val container: String? = null,//Reference to the database of which this TimeFlow is a part.
    override val temporary: Boolean? = null,//Boolean value indicating that this snapshot is in a transient state and should not be user visible.
    override val missingNonLoggedData: Boolean? = null,//Boolean value indicating if a virtual database provisioned from this snapshot will be missing nologging changes.
    override val creationTime: String? = null,//Point in time at which this snapshot was created. This may be different from the time corresponding to the TimeFlow.
    override val timezone: String? = null,//Time zone of the source database at the time the snapshot was taken.
    override val consistency: String? = null,//A value in the set {CONSISTENT, INCONSISTENT, CRASH_CONSISTENT} indicating what type of recovery strategies must be invoked when provisioning from this snapshot.
    override val timeflow: String? = null,//TimeFlow of which this snapshot is a part.
    override val version: String? = null,//Version of database source repository at the time the snapshot was taken.
    override val retention: Int? = null,//Retention policy, in days. A value of -1 indicates the snapshot should be kept forever.
    override val name: String? = null,//Object name.
    override val reference: String? = null,//The object reference.
    override val namespace: String? = null,//Alternate namespace for this object, for replicated and restored objects.
    override val type: String = "AppDataSnapshot"
) : TimeflowSnapshot {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "firstChangePoint" to firstChangePoint,
            "metadata" to metadata,
            "latestChangePoint" to latestChangePoint,
            "runtime" to runtime,
            "container" to container,
            "temporary" to temporary,
            "missingNonLoggedData" to missingNonLoggedData,
            "creationTime" to creationTime,
            "timezone" to timezone,
            "consistency" to consistency,
            "timeflow" to timeflow,
            "version" to version,
            "retention" to retention,
            "name" to name,
            "reference" to reference,
            "namespace" to namespace,
            "type" to type
        )
    }
}

