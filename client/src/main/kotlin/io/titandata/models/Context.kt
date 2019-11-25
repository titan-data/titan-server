/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

data class Context(
    var provider: String,
    var properties: Map<String, String>
)
