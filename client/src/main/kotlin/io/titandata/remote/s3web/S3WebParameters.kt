/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.s3web

import io.titandata.models.RemoteParameters

data class S3WebParameters(
    override var provider: String = "s3web"
) : RemoteParameters()
