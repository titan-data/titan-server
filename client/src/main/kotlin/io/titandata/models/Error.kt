/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package io.titandata.models

data class Error(
    var code: String? = null,
    var message: String,
    var details: String? = null
)
