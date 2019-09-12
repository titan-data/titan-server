/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The security credential.
 */
interface Credential : TypedObject {
    override val type: String
    override fun toMap(): Map<String, Any?>
}
