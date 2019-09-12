/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * A virtual source definition for toolkits.
 */
open class ToolkitVirtualSource (
    open val preSnapshot: String? = null,//A workflow script to run before taking a snapshot of a virtual copy of the application.
    open val postSnapshot: String? = null,//A workflow script to run after taking a snapshot of a virtual copy of the application.
    open val stop: String? = null,//A workflow script to run when stopping a virtual copy of the application.
    open val start: String? = null,//A workflow script to run when starting a virtual copy of the application.
    open val mountSpec: String? = null,//A workflow script that specifies where the virtual copy of the application should be mounted.
    open val configure: String? = null,//A workflow script run when configuring a virtual copy of the application in a new environment.
    open val initialize: String? = null,//A workflow script to run when creating an empty application.
    open val unconfigure: String? = null,//A workflow script run when removing a virtual copy of the application from an environment (e.g. on delete, disable, or refresh).
    open val parameters: SchemaDraftV4? = null,//A user defined schema for the provisioning parameters.
    open val reconfigure: String? = null,//A workflow script run when returning a virtual copy of the appliction to an environment that it was previously removed from.
    open val status: String? = null,//The workflow script to run to determine if a virtual copy of the application is running. The script should output 'ACTIVE' if the application is running, 'INACTIVE' if the application is not running, and 'UNKNOWN' if the script encounters an unexpected problem.
    override val type: String = "ToolkitVirtualSource"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "preSnapshot" to preSnapshot,
            "postSnapshot" to postSnapshot,
            "stop" to stop,
            "start" to start,
            "mountSpec" to mountSpec,
            "configure" to configure,
            "initialize" to initialize,
            "unconfigure" to unconfigure,
            "parameters" to parameters,
            "reconfigure" to reconfigure,
            "status" to status,
            "type" to type
        )
    }
}

