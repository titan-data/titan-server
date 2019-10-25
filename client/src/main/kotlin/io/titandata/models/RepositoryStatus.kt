/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

data class RepositoryStatus(
    var logicalSize: Long,
    var actualSize: Long,
    var lastCommit: String?,
    var sourceCommit: String?,
    var volumeStatus: List<RepositoryVolumeStatus>
)
