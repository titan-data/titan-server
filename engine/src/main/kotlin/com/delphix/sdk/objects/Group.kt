/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Database group.
 */
open class Group (
    open val description: String? = null,//Optional description for the group.
    open val dataNode: String? = null,//The data node where databases of this group will be located.
    override val name: String? = null,//Object name.
    override val reference: String? = null,//The object reference.
    override val namespace: String? = null,//Alternate namespace for this object, for replicated and restored objects.
    override val type: String = "Group"
) : NamedUserObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "description" to description,
            "dataNode" to dataNode,
            "name" to name,
            "reference" to reference,
            "namespace" to namespace,
            "type" to type
        )
    }
}

