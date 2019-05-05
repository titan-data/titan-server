/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The source config represents the dynamically discovered attributes of a source.
 */
interface SourceConfig : ReadonlyNamedUserObject {
    val discovered: Boolean?//Whether this source was discovered.
    val environmentUser: String?//The user used to create and manage the configuration.
    val linkingEnabled: Boolean?//Whether this source should be used for linking.
    val repository: String?//The object reference of the source repository.
    override val name: String?//Object name.
    override val reference: String?//The object reference.
    override val namespace: String?//Alternate namespace for this object, for replicated and restored objects.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
