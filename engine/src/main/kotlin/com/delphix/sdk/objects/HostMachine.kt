/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The representation of the host machine.
 */
open class HostMachine (
    open val memorySize: Int? = null,//The amount of RAM on the host machine.
    open val platform: String? = null,//The platform for the host machine.
    override val type: String = "HostMachine"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "memorySize" to memorySize,
            "platform" to platform,
            "type" to type
        )
    }
}

