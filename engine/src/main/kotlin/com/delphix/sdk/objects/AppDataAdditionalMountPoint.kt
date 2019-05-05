/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Specifies an additional location on which to mount a subdirectory of an AppData container.
 */
open class AppDataAdditionalMountPoint (
    open val environment: String? = null,//Reference to the environment on which the file system will be mounted.
    open val mountPath: String? = null,//Absolute path on the target environment were the filesystem should be mounted.
    open val sharedPath: String? = null,//Relative path within the container of the directory that should be mounted.
    override val type: String = "AppDataAdditionalMountPoint"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "environment" to environment,
            "mountPath" to mountPath,
            "sharedPath" to sharedPath,
            "type" to type
        )
    }
}

