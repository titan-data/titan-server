/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

data class RepositoryStatus(
    var logicalSize: Long,
    var actualSize: Long,
    var lastCommit: String? = null,
    var sourceCommit: String? = null,
    var volumeStatus: List<RepositoryVolumeStatus>
)
