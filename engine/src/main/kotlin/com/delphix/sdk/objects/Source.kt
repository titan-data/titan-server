/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * A source represents an external database instance outside the Delphix system.
 */
interface Source : UserObject {
    val container: String?//Reference to the container being fed by this source, if any.
    val virtual: Boolean?//Flag indicating whether the source is a virtual source in the Delphix system.
    val description: String?//A user-provided description of the source.
    val runtime: SourceRuntime?//Runtime properties of this source.
    val config: String?//Reference to the configuration for the source.
    val staging: Boolean?//Flag indicating whether the source is used as a staging source for pre-provisioning. Staging sources are managed by the Delphix system.
    val restoration: Boolean?//Flag indicating whether the source is a restoration source in the Delphix system.
    val linked: Boolean?//Flag indicating whether the source is a linked source in the Delphix system.
    val status: String?//Status of this source.
    override val name: String?//Object name.
    override val reference: String?//The object reference.
    override val namespace: String?//Alternate namespace for this object, for replicated and restored objects.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
