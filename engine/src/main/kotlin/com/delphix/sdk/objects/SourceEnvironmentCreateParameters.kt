/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The parameters used for source environment create parameters.
 */
interface SourceEnvironmentCreateParameters : TypedObject {
    val primaryUser: EnvironmentUser?//The primary user associated with the environment.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
