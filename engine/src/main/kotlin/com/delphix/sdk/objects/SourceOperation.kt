/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * A user-specifiable operation that can be performed on sources.
 */
interface SourceOperation : Operation {
    override val type: String
    override fun toMap(): Map<String, Any?>
}
