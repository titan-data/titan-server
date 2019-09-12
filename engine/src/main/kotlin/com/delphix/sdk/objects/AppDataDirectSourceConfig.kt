/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Source config for directly linked AppData sources.
 */
open class AppDataDirectSourceConfig (
    open val path: String? = null,//The path to the data to be synced.
    open val restoration: Boolean? = null,//True if this source config is part of a restoration dataset.
    override val name: String? = null,//Object name.
    override val repository: String? = null,//The object reference of the source repository.
    override val parameters: Map<String, Any>? = null,//The list of parameters specified by the source config schema in the toolkit. If no schema is specified, this list is empty.
    override val discovered: Boolean? = null,//Whether this source was discovered.
    override val environmentUser: String? = null,//The user used to create and manage the configuration.
    override val linkingEnabled: Boolean? = null,//Whether this source should be used for linking.
    override val reference: String? = null,//The object reference.
    override val namespace: String? = null,//Alternate namespace for this object, for replicated and restored objects.
    override val type: String = "AppDataDirectSourceConfig"
) : AppDataSourceConfig {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "path" to path,
            "restoration" to restoration,
            "name" to name,
            "repository" to repository,
            "parameters" to parameters,
            "discovered" to discovered,
            "environmentUser" to environmentUser,
            "linkingEnabled" to linkingEnabled,
            "reference" to reference,
            "namespace" to namespace,
            "type" to type
        )
    }
}

