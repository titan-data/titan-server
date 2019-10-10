/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.s3

import io.titandata.models.RemoteParameters

data class S3Parameters(
    override var provider: String = "s3",
    var accessKey: String? = null,
    var secretKey: String? = null,
    var sessionToken: String? = null,
    var region: String? = null
) : RemoteParameters()
