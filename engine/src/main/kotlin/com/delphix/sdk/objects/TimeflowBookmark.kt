/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * A TimeFlow bookmark is a user defined name for a TimeFlow point (location or timestamp within a TimeFlow).
 */
open class TimeflowBookmark (
    open val retentionProof: Boolean? = null,//Indicates whether retention should be allowed to clean up the TimeFlow bookmark and associated data.
    open val location: String? = null,//The TimeFlow location.
    open val tag: String? = null,//A tag for the bookmark that can be used to group TimeFlow bookmarks together or qualify the type of the bookmark.
    open val timeflow: String? = null,//Reference to the TimeFlow for this bookmark.
    open val timestamp: String? = null,//The logical time corresponding to the TimeFlow location.
    override val name: String? = null,//Object name.
    override val reference: String? = null,//The object reference.
    override val namespace: String? = null,//Alternate namespace for this object, for replicated and restored objects.
    override val type: String = "TimeflowBookmark"
) : NamedUserObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "retentionProof" to retentionProof,
            "location" to location,
            "tag" to tag,
            "timeflow" to timeflow,
            "timestamp" to timestamp,
            "name" to name,
            "reference" to reference,
            "namespace" to namespace,
            "type" to type
        )
    }
}

