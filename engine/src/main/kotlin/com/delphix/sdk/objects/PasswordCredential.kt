/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The password based security credential.
 */
open class PasswordCredential (
    open val password: String? = null,//The password.
    override val type: String = "PasswordCredential"
) : Credential {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "password" to password,
            "type" to type
        )
    }
}

