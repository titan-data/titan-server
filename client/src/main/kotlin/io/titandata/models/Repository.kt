/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package io.titandata.models

data class Repository(
    var name: String,
    var properties: Map<String, Any>
)
