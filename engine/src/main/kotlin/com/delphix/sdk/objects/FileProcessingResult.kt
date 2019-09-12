/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Result of a file processing request (upload or download).
 */
interface FileProcessingResult : TypedObject {
    val url: String?//URL to download from or upload to.
    val token: String?//Token to pass as parameter to identify the file.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
