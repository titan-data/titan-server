/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

data class RepositoryStatus(
    var logicalSize: Long,
    var actualSize: Long,
    var checkedOutFrom: String?,
    var lastCommit: String?,
    var volumeStatus: List<RepositoryVolumeStatus>
)
