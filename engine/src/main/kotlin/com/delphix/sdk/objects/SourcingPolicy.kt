/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Database policies for managing SnapSync and LogSync across sources for a MSSQL container.
 */
open class SourcingPolicy (
    open val loadFromBackup: Boolean? = null,//True if the initial load and subsequent syncs for this container restore from already existing database backups. In such cases Delphix does not take any full database backups of the source database. When false, Delphix will take a full backup of the source.
    open val logsyncEnabled: Boolean? = null,//True if LogSync should run for this database.
    override val type: String = "SourcingPolicy"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "loadFromBackup" to loadFromBackup,
            "logsyncEnabled" to logsyncEnabled,
            "type" to type
        )
    }
}

