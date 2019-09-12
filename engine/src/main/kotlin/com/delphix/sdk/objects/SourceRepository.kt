/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * A source repository represents a container for the source config.
 */
interface SourceRepository : ReadonlyNamedUserObject {
    val environment: String?//Reference to the environment containing this repository.
    val provisioningEnabled: Boolean?//Flag indicating whether the repository should be used for provisioning.
    val linkingEnabled: Boolean?//Flag indicating whether the repository should be used for linking.
    val staging: Boolean?//Flag indicating whether this repository can be used by the Delphix Engine for internal processing.
    val version: String?//Version of the repository.
    override val name: String?//Object name.
    override val reference: String?//The object reference.
    override val namespace: String?//Alternate namespace for this object, for replicated and restored objects.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
