/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The parameters to use as input to create TimeFlow bookmarks.
 */
open class TimeflowBookmarkCreateParameters (
    open val retentionProof: Boolean? = null,//Indicates whether retention should be allowed to clean up the TimeFlow bookmark and associated data.
    open val name: String? = null,//The bookmark name.
    open val tag: String? = null,//A tag for the bookmark that can be used to group bookmarks together or qualify the type of the bookmark.
    open val timeflowPoint: TimeflowPoint? = null,//The TimeFlow point which is referenced by this bookmark.
    override val type: String = "TimeflowBookmarkCreateParameters"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "retentionProof" to retentionProof,
            "name" to name,
            "tag" to tag,
            "timeflowPoint" to timeflowPoint,
            "type" to type
        )
    }
}

