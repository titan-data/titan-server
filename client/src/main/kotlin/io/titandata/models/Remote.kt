/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

data class Remote (
    var provider: String,
    var name: String,
    var properties: Map<String, Any?> = emptyMap()
)
