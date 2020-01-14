/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

data class RepositoryStatus(
    var lastCommit: String? = null,
    var sourceCommit: String? = null
)
