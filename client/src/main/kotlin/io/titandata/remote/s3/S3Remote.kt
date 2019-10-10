/*
 * Copyright The Titan Project Contributors.
 */

package io.titandata.remote.s3

import io.titandata.models.Remote

data class S3Remote(
    override var provider: String = "s3",
    override var name: String,
    var bucket: String,
    var path: String? = null,
    var accessKey: String? = null,
    var secretKey: String? = null,
    var region: String? = null
) : Remote()
