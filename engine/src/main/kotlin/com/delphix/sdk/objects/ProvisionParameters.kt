/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The parameters to use as input when creating a new virtual dataset by provisioning.
 */
interface ProvisionParameters : VirtualDatasetCreationParameters {
    val timeflowPointParameters: TimeflowPointParameters?//The TimeFlow point, bookmark, or semantic location to base provisioning on.
    val maskingJob: String?//The Masking Job to be run when this dataset is provisioned or refreshed.
    override val container: Container?//The new container for the created dataset.
    override val sourceConfig: SourceConfig?//The source config including dynamically discovered attributes of the source.
    override val source: Source?//The source that describes an external dataset instance.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
