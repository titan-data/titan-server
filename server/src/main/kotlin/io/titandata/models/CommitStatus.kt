/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

data class CommitStatus(
    var logicalSize: Long,
    var actualSize: Long,
    var uniqueSize: Long,
    var ready: Boolean,
    var error: String?
)
