/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * A container holding data.
 */
interface Container : NamedUserObject {
    val currentTimeflow: String?//A reference to the currently active TimeFlow for this container.
    val previousTimeflow: String?//A reference to the previous TimeFlow for this container.
    val creationTime: String?//The date this container was created.
    val masked: Boolean?//True if this container is a masked container.
    val description: String?//Optional user-provided description for the container.
    val provisionContainer: String?//A reference to the container this container was provisioned from.
    val runtime: DBContainerRuntime?//Runtime properties of this container.
    val transformation: Boolean?//True if this container is a transformation container.
    val restoration: Boolean?//True if this container is part of a restoration dataset.
    val group: String?//A reference to the group containing this container.
    override val name: String?//Object name.
    override val reference: String?//The object reference.
    override val namespace: String?//Alternate namespace for this object, for replicated and restored objects.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
