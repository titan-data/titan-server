/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Contains information that can be used to connect to the source.
 */
interface SourceConnectionInfo : TypedObject {
    val version: String?//The database version string.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
