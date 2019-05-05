/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * An AppData repository.
 */
open class AppDataRepository (
    open val toolkit: String? = null,//The toolkit associated with this repository.
    open val parameters: Json? = null,//The list of parameters specified by the repository schema in the toolkit. If no schema is specified, this list is empty.
    override val environment: String? = null,//Reference to the environment containing this repository.
    override val provisioningEnabled: Boolean? = null,//Flag indicating whether the repository should be used for provisioning.
    override val linkingEnabled: Boolean? = null,//Flag indicating whether the repository should be used for linking.
    override val staging: Boolean? = null,//Flag indicating whether this repository can be used by the Delphix Engine for internal processing.
    override val version: String? = null,//Version of the repository.
    override val name: String? = null,//Object name.
    override val reference: String? = null,//The object reference.
    override val namespace: String? = null,//Alternate namespace for this object, for replicated and restored objects.
    override val type: String = "AppDataRepository"
) : SourceRepository {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "toolkit" to toolkit,
            "parameters" to parameters,
            "environment" to environment,
            "provisioningEnabled" to provisioningEnabled,
            "linkingEnabled" to linkingEnabled,
            "staging" to staging,
            "version" to version,
            "name" to name,
            "reference" to reference,
            "namespace" to namespace,
            "type" to type
        )
    }
}

