/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

data class RepositoryVolumeStatus(
    var name: String,
    var logicalSize: Long,
    var actualSize: Long,
    var properties: Map<String, Any>
)
