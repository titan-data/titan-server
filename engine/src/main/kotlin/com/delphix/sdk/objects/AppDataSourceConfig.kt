/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Base Source config for AppDataToolkits.
 */
interface AppDataSourceConfig : SourceConfig {
    override val name: String?//Object name.
    override val repository: String?//The object reference of the source repository.
    val parameters: Map<String, Any>?//The list of parameters specified by the source config schema in the toolkit. If no schema is specified, this list is empty.
    override val discovered: Boolean?//Whether this source was discovered.
    override val environmentUser: String?//The user used to create and manage the configuration.
    override val linkingEnabled: Boolean?//Whether this source should be used for linking.
    override val reference: String?//The object reference.
    override val namespace: String?//Alternate namespace for this object, for replicated and restored objects.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
