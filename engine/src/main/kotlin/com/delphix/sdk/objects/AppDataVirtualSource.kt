/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * A virtual AppData source.
 */
open class AppDataVirtualSource (
    open val operations: VirtualSourceOperations? = null,//User-specified operation hooks for this source.
    open val additionalMountPoints: List<AppDataAdditionalMountPoint>? = null,//Locations to mount subdirectories of the AppData in addition to the normal target mount point. These paths will be mounted and unmounted as part of enabling and disabling this source.
    open val parameters: Map<String, Any>? = null,//The JSON payload conforming to the DraftV4 schema based on the type of application data being manipulated.
    override val runtime: SourceRuntime? = null,//Runtime properties of this source.
    override val container: String? = null,//Reference to the container being fed by this source, if any.
    override val virtual: Boolean? = null,//Flag indicating whether the source is a virtual source in the Delphix system.
    override val description: String? = null,//A user-provided description of the source.
    override val config: String? = null,//Reference to the configuration for the source.
    override val staging: Boolean? = null,//Flag indicating whether the source is used as a staging source for pre-provisioning. Staging sources are managed by the Delphix system.
    override val restoration: Boolean? = null,//Flag indicating whether the source is a restoration source in the Delphix system.
    override val linked: Boolean? = null,//Flag indicating whether the source is a linked source in the Delphix system.
    override val status: String? = null,//Status of this source.
    override val name: String? = null,//Object name.
    override val reference: String? = null,//The object reference.
    override val namespace: String? = null,//Alternate namespace for this object, for replicated and restored objects.
    override val type: String = "AppDataVirtualSource"
) : AppDataManagedSource {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "operations" to operations,
            "additionalMountPoints" to additionalMountPoints,
            "parameters" to parameters,
            "runtime" to runtime,
            "container" to container,
            "virtual" to virtual,
            "description" to description,
            "config" to config,
            "staging" to staging,
            "restoration" to restoration,
            "linked" to linked,
            "status" to status,
            "name" to name,
            "reference" to reference,
            "namespace" to namespace,
            "type" to type
        )
    }
}

