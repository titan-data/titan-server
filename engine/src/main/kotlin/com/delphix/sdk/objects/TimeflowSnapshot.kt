/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Snapshot of a point within a TimeFlow that is used as the basis for provisioning.
 */
interface TimeflowSnapshot : ReadonlyNamedUserObject {
    val container: String?//Reference to the database of which this TimeFlow is a part.
    val temporary: Boolean?//Boolean value indicating that this snapshot is in a transient state and should not be user visible.
    val firstChangePoint: TimeflowPoint?//The location within the parent TimeFlow at which this snapshot was initiated. No recovery earlier than this point needs to be applied in order to provision a database from this snapshot. If "firstChangePoint" equals "latestChangePoint", then no recovery needs to be applied in order to provision a database.
    val missingNonLoggedData: Boolean?//Boolean value indicating if a virtual database provisioned from this snapshot will be missing nologging changes.
    val creationTime: String?//Point in time at which this snapshot was created. This may be different from the time corresponding to the TimeFlow.
    val latestChangePoint: TimeflowPoint?//The location of the snapshot within the parent TimeFlow represented by this snapshot.
    val timezone: String?//Time zone of the source database at the time the snapshot was taken.
    val runtime: SnapshotRuntime?//Runtime properties of the snapshot.
    val consistency: String?//A value in the set {CONSISTENT, INCONSISTENT, CRASH_CONSISTENT} indicating what type of recovery strategies must be invoked when provisioning from this snapshot.
    val timeflow: String?//TimeFlow of which this snapshot is a part.
    val version: String?//Version of database source repository at the time the snapshot was taken.
    val retention: Int?//Retention policy, in days. A value of -1 indicates the snapshot should be kept forever.
    override val name: String?//Object name.
    override val reference: String?//The object reference.
    override val namespace: String?//Alternate namespace for this object, for replicated and restored objects.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
