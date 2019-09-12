/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Data container for AppData.
 */
open class AppDataContainer (
    override val group: String? = null,//A reference to the group containing this container.
    override val name: String? = null,//Object name.
    open val toolkit: String? = null,//The toolkit managing the data in the container.
    override val runtime: DBContainerRuntime? = null,//Runtime properties of this container.
    override val restoration: Boolean? = null,//True if this container is part of a restoration dataset.
    override val os: String? = null,//Native operating system of the original database source system.
    override val performanceMode: String? = null,//Whether to enable high performance mode.
    override val processor: String? = null,//Native processor type of the original database source system.
    override val sourcingPolicy: SourcingPolicy? = null,//Policies for managing LogSync and SnapSync across sources.
    override val currentTimeflow: String? = null,//A reference to the currently active TimeFlow for this container.
    override val previousTimeflow: String? = null,//A reference to the previous TimeFlow for this container.
    override val creationTime: String? = null,//The date this container was created.
    override val masked: Boolean? = null,//True if this container is a masked container.
    override val description: String? = null,//Optional user-provided description for the container.
    override val provisionContainer: String? = null,//A reference to the container this container was provisioned from.
    override val transformation: Boolean? = null,//True if this container is a transformation container.
    override val reference: String? = null,//The object reference.
    override val namespace: String? = null,//Alternate namespace for this object, for replicated and restored objects.
    override val type: String = "AppDataContainer"
) : DatabaseContainer {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "toolkit" to toolkit,
            "runtime" to runtime,
            "restoration" to restoration,
            "os" to os,
            "performanceMode" to performanceMode,
            "processor" to processor,
            "sourcingPolicy" to sourcingPolicy,
            "currentTimeflow" to currentTimeflow,
            "previousTimeflow" to previousTimeflow,
            "creationTime" to creationTime,
            "masked" to masked,
            "description" to description,
            "provisionContainer" to provisionContainer,
            "transformation" to transformation,
            "group" to group,
            "name" to name,
            "reference" to reference,
            "namespace" to namespace,
            "type" to type
        )
    }
}

