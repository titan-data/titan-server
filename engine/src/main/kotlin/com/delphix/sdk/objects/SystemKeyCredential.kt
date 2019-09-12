/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The system public key based security credential.
 */
open class SystemKeyCredential (
    override val type: String = "SystemKeyCredential"
) : PublicKeyCredential {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "type" to type
        )
    }
}

