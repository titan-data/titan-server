/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Runtime, non-persistent properties for a host machine.
 */
open class HostRuntime (
    open val available: Boolean? = null,//True if the host is up and a connection can be established.
    open val traceRouteInfo: TracerouteInfo? = null,//Traceroute network hops from host to Delphix Engine.
    override val type: String = "HostRuntime"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "available" to available,
            "traceRouteInfo" to traceRouteInfo,
            "type" to type
        )
    }
}

