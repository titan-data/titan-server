/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan.models

data class Commit(
    var id: String,
    var properties: Map<String, Any>
)
