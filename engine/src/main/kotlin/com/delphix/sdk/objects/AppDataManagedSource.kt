/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * An AppData source managed by Delphix.
 */
interface AppDataManagedSource : AppDataSource {
    override val runtime: SourceRuntime?//Runtime properties of this source.
    override val container: String?//Reference to the container being fed by this source, if any.
    override val virtual: Boolean?//Flag indicating whether the source is a virtual source in the Delphix system.
    override val description: String?//A user-provided description of the source.
    override val config: String?//Reference to the configuration for the source.
    override val staging: Boolean?//Flag indicating whether the source is used as a staging source for pre-provisioning. Staging sources are managed by the Delphix system.
    override val restoration: Boolean?//Flag indicating whether the source is a restoration source in the Delphix system.
    override val linked: Boolean?//Flag indicating whether the source is a linked source in the Delphix system.
    override val status: String?//Status of this source.
    override val name: String?//Object name.
    override val reference: String?//The object reference.
    override val namespace: String?//Alternate namespace for this object, for replicated and restored objects.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
