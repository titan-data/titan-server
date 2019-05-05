/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The parameters to use as input to provision AppData.
 */
open class AppDataProvisionParameters (
    override val container: Container? = null,//The new container for the created dataset.
    override val sourceConfig: SourceConfig? = null,//The source config including dynamically discovered attributes of the source.
    override val source: Source? = null,//The source that describes an external dataset instance.
    override val timeflowPointParameters: TimeflowPointParameters? = null,//The TimeFlow point, bookmark, or semantic location to base provisioning on.
    override val maskingJob: String? = null,//The Masking Job to be run when this dataset is provisioned or refreshed.
    override val type: String = "AppDataProvisionParameters"
) : ProvisionParameters {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "container" to container,
            "sourceConfig" to sourceConfig,
            "source" to source,
            "timeflowPointParameters" to timeflowPointParameters,
            "maskingJob" to maskingJob,
            "type" to type
        )
    }
}

