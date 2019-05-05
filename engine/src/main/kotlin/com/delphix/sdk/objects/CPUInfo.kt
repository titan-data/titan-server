/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Describes a processor available to the system.
 */
open class CPUInfo (
    open val cores: Int? = null,//Number of cores in the processor.
    open val speed: Int? = null,//Speed of the processor, in hertz.
    override val type: String = "CPUInfo"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "cores" to cores,
            "speed" to speed,
            "type" to type
        )
    }
}

