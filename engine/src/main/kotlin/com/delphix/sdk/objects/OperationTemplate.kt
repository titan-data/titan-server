/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Template for commonly used operations.
 */
open class OperationTemplate (
    open val lastUpdated: String? = null,//Most recently modified time.
    override val name: String? = null,//Object name.
    open val description: String? = null,//User provided description for this template.
    open val operation: SourceOperation? = null,//Template contents.
    override val reference: String? = null,//The object reference.
    override val namespace: String? = null,//Alternate namespace for this object, for replicated and restored objects.
    override val type: String = "OperationTemplate"
) : NamedUserObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "lastUpdated" to lastUpdated,
            "name" to name,
            "description" to description,
            "operation" to operation,
            "reference" to reference,
            "namespace" to namespace,
            "type" to type
        )
    }
}

