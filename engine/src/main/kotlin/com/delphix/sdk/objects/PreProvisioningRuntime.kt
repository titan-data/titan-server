/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Runtime properties for pre-provisioning of a MSSQL database container.
 */
open class PreProvisioningRuntime (
    open val lastUpdateTimestamp: String? = null,//Timestamp of the last update to the status.
    open val response: String? = null,//Response taken based on the status of the pre-provisioning run.
    open val pendingAction: String? = null,//User action required to resolve any error that the pre-provisioning run encountered.
    open val preProvisioningState: String? = null,//Indicates the current state of pre-provisioning for the database.
    open val status: String? = null,//The status of the pre-provisioning run.
    override val type: String = "PreProvisioningRuntime"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "lastUpdateTimestamp" to lastUpdateTimestamp,
            "response" to response,
            "pendingAction" to pendingAction,
            "preProvisioningState" to preProvisioningState,
            "status" to status,
            "type" to type
        )
    }
}

