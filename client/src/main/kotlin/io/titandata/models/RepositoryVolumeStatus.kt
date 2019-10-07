/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

data class RepositoryVolumeStatus(
    var logicalSize: Long,
    var actualSize: Long,
    var name: String,
    var properties: Map<String, Any>
)
