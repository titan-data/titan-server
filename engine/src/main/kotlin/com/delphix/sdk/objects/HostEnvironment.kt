/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The representation of an host environment object.
 */
interface HostEnvironment : SourceEnvironment {
    val host: String?//The reference to the associated host.
    override val primaryUser: String?//A reference to the primary user for this environment.
    override val description: String?//The environment description.
    override val enabled: Boolean?//Indicates whether the source environment is enabled.
    override val name: String?//Object name.
    override val reference: String?//The object reference.
    override val namespace: String?//Alternate namespace for this object, for replicated and restored objects.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
