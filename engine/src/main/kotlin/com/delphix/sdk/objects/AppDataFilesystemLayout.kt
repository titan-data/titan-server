/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * A filesystem layout that matches the filesystem of a Delphix TimeFlow.
 */
open class AppDataFilesystemLayout (
    open val targetDirectory: String? = null,//The base directory to use for the exported database.
    override val type: String = "AppDataFilesystemLayout"
) : FilesystemLayout {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "targetDirectory" to targetDirectory,
            "type" to type
        )
    }
}

