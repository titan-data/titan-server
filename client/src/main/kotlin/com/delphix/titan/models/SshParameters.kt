/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package com.delphix.titan.models

data class SshParameters(
    override var provider: String = "ssh",
    var password: String? = null,
    var key: String? = null
) : RemoteParameters()
