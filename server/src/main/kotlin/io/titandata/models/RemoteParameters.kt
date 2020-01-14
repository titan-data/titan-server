/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

data class RemoteParameters(
    var provider: String,
    var properties: Map<String, Any> = emptyMap()
)
