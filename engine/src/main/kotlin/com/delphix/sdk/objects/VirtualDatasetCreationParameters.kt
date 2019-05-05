/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The parameters to use as input when creating a new virtual dataset.
 */
interface VirtualDatasetCreationParameters : TypedObject {
    val container: Container?//The new container for the created dataset.
    val sourceConfig: SourceConfig?//The source config including dynamically discovered attributes of the source.
    val source: Source?//The source that describes an external dataset instance.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
