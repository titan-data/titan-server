/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The public key based security credential.
 */
interface PublicKeyCredential : Credential {
    override val type: String
    override fun toMap(): Map<String, Any?>
}
