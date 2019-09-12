/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Runtime properties of an AppData container.
 */
open class AppDataContainerRuntime (
    override val preProvisioningStatus: PreProvisioningRuntime? = null,//The pre-provisioning runtime for the container.
    override val logSyncActive: Boolean? = null,//True if the LogSync is enabled and running for this container.
    override val type: String = "AppDataContainerRuntime"
) : DBContainerRuntime {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "preProvisioningStatus" to preProvisioningStatus,
            "logSyncActive" to logSyncActive,
            "type" to type
        )
    }
}

