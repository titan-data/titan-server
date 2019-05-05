/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Trace route info from target host to Delphix Engine.
 */
open class TracerouteInfo (
    open val networkHops: String? = null,//Latency of network hops from host to Delphix Engine.
    override val type: String = "TracerouteInfo"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "networkHops" to networkHops,
            "type" to type
        )
    }
}

