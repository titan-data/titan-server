/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The filesystem configuration of a database.
 */
interface FilesystemLayout : TypedObject {
    override val type: String
    override fun toMap(): Map<String, Any?>
}
