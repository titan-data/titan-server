/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The representation of the host configuration properties.
 */
open class HostConfiguration (
    open val lastRefreshed: String? = null,//The timestamp when the host was last refreshed.
    open val lastUpdated: String? = null,//The timestamp when the host was last updated.
    open val discovered: Boolean? = null,//Indicates whether the host configuration properties were discovered.
    open val machine: HostMachine? = null,//The host machine information.
    open val operatingSystem: HostOS? = null,//The host operating system information.
    override val type: String = "HostConfiguration"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "lastRefreshed" to lastRefreshed,
            "lastUpdated" to lastUpdated,
            "discovered" to discovered,
            "machine" to machine,
            "operatingSystem" to operatingSystem,
            "type" to type
        )
    }
}

