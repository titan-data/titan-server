/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

data class Commit(
    var id: String,
    var properties: Map<String, Any> = emptyMap()
)
