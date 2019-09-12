/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * A container holding database data.
 */
interface DatabaseContainer : Container {
    val os: String?//Native operating system of the original database source system.
    val performanceMode: String?//Whether to enable high performance mode.
    val processor: String?//Native processor type of the original database source system.
    val sourcingPolicy: SourcingPolicy?//Policies for managing LogSync and SnapSync across sources.
    override val currentTimeflow: String?//A reference to the currently active TimeFlow for this container.
    override val previousTimeflow: String?//A reference to the previous TimeFlow for this container.
    override val creationTime: String?//The date this container was created.
    override val masked: Boolean?//True if this container is a masked container.
    override val description: String?//Optional user-provided description for the container.
    override val provisionContainer: String?//A reference to the container this container was provisioned from.
    override val runtime: DBContainerRuntime?//Runtime properties of this container.
    override val transformation: Boolean?//True if this container is a transformation container.
    override val restoration: Boolean?//True if this container is part of a restoration dataset.
    override val group: String?//A reference to the group containing this container.
    override val name: String?//Object name.
    override val reference: String?//The object reference.
    override val namespace: String?//Alternate namespace for this object, for replicated and restored objects.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
