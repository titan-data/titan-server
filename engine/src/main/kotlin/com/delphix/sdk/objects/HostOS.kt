/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The operating system information for the host.
 */
open class HostOS (
    open val kernel: String? = null,//The OS kernel.
    open val release: String? = null,//The OS release.
    open val timezone: String? = null,//The OS timezone.
    open val name: String? = null,//The OS name.
    open val distribution: String? = null,//The OS distribution.
    open val version: String? = null,//The OS version.
    override val type: String = "HostOS"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "kernel" to kernel,
            "release" to release,
            "timezone" to timezone,
            "name" to name,
            "distribution" to distribution,
            "version" to version,
            "type" to type
        )
    }
}

