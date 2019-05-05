/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Runtime properties of a database container.
 */
interface DBContainerRuntime : TypedObject {
    val preProvisioningStatus: PreProvisioningRuntime?//The pre-provisioning runtime for the container.
    val logSyncActive: Boolean?//True if the LogSync is enabled and running for this container.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
