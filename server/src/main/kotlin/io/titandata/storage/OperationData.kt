/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package io.titandata.storage

import io.titandata.models.Operation
import io.titandata.models.RemoteParameters

/**
 * this is a very simple data class that lets us store the (operation, request) tuple on disk.
 */
data class OperationData(
        val operation: Operation,
        val params: RemoteParameters
)
