/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Represents a Delphix user authentication request.
 */
open class LoginRequest (
    open val password: String? = null,//The password of the user to authenticate.
    open val target: String? = null,//The authentication domain.
    open val username: String? = null,//The username of the user to authenticate.
    override val type: String = "LoginRequest"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "password" to password,
            "target" to target,
            "username" to username,
            "type" to type
        )
    }
}

