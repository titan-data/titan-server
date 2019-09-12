/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The representation of a repository template object.
 */
open class SourceRepositoryTemplate (
    open val container: String? = null,//The reference to the database container.
    open val template: String? = null,//The reference to the associated template.
    open val repository: String? = null,//The reference to the target repository.
    override val name: String? = null,//Object name.
    override val reference: String? = null,//The object reference.
    override val namespace: String? = null,//Alternate namespace for this object, for replicated and restored objects.
    override val type: String = "SourceRepositoryTemplate"
) : NamedUserObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "container" to container,
            "template" to template,
            "repository" to repository,
            "name" to name,
            "reference" to reference,
            "namespace" to namespace,
            "type" to type
        )
    }
}

