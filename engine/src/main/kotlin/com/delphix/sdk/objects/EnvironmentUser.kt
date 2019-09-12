/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The representation of an environment user object.
 */
open class EnvironmentUser (
    open val environment: String? = null,//A reference to the associated environment.
    open val credential: Credential? = null,//The credential for the environment user.
    open val groupId: Int? = null,//Group ID of the user.
    open val userId: Int? = null,//User ID of the user.
    override val name: String? = null,//Object name.
    override val reference: String? = null,//The object reference.
    override val namespace: String? = null,//Alternate namespace for this object, for replicated and restored objects.
    override val type: String = "EnvironmentUser"
) : NamedUserObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "environment" to environment,
            "credential" to credential,
            "groupId" to groupId,
            "userId" to userId,
            "name" to name,
            "reference" to reference,
            "namespace" to namespace,
            "type" to type
        )
    }
}

