/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan.storage

import com.delphix.titan.models.Operation
import com.delphix.titan.models.RemoteParameters

/**
 * this is a very simple data class that lets us store the (operation, request) tuple on disk.
 */
data class OperationData(
    val operation: Operation,
    val params: RemoteParameters
)
