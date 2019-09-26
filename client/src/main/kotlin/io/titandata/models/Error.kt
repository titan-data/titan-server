/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.models

data class Error(
    var code: String? = null,
    var message: String,
    var details: String? = null
)
