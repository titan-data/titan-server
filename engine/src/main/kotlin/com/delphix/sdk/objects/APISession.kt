/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Describes a Delphix web service session and is the result of an initial handshake.
 */
open class APISession (
    open val client: String? = null,//Client software identification token.
    open val locale: String? = null,//Locale as an IETF BCP 47 language tag, defaults to 'en-US'.
    open val version: APIVersion? = null,//Version of the API to use.
    override val type: String = "APISession"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "client" to client,
            "locale" to locale,
            "version" to version,
            "type" to type
        )
    }
}

