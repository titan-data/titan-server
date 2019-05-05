/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Describes operations which are performed on virtual sources at various times.
 */
open class VirtualSourceOperations (
    open val preSnapshot: List<SourceOperation>,//Operations to perform before snapshotting a virtual source. These operations can quiesce any data prior to snapshotting.
    open val postSnapshot: List<SourceOperation>,//Operations to perform after snapshotting a virtual source.
    open val preRefresh: List<SourceOperation>,//Operations to perform before refreshing a virtual source. These operations can backup any data or configuration from the running source before doing the refresh.
    open val postRollback: List<SourceOperation>,//Operations to perform after rewinding a virtual source. These operations can be used to automate processes once the rewind is complete.
    open val preRollback: List<SourceOperation>,//Operations to perform before rewinding a virtual source. These operations can backup any data or configuration from the running source prior to rewinding.
    open val configureClone: List<SourceOperation>,//Operations to perform when initially creating the virtual source and every time it is refreshed.
    open val postRefresh: List<SourceOperation>,//Operations to perform after refreshing a virtual source. These operations can be used to restore any data or configuration backed up in the preRefresh operations.
    override val type: String = "VirtualSourceOperations"
) : TypedObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "preSnapshot" to preSnapshot,
            "postSnapshot" to postSnapshot,
            "preRefresh" to preRefresh,
            "postRollback" to postRollback,
            "preRollback" to preRollback,
            "configureClone" to configureClone,
            "postRefresh" to postRefresh,
            "type" to type
        )
    }
}

