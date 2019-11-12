/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

data class Repository(
    var name: String,
    var properties: Map<String, Any> = emptyMap()
)
