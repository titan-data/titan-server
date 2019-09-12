/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan.models

data class Error(
    var code: String? = null,
    var message: String,
    var details: String? = null
)
