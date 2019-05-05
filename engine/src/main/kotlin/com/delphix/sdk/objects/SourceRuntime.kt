/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Runtime properties of a linked or virtual source database.
 */
interface SourceRuntime : TypedObject {
    val accessible: Boolean?//True if the source is JDBC accessible. If false then no properties can be retrieved.
    val databaseSize: Int?//Size of the database in bytes.
    val enabled: String?//Status indicating whether the source is enabled. A source has a 'PARTIAL' status if its sub-sources are not all enabled.
    val notAccessibleReason: String?//The reason why the source is not JDBC accessible.
    val status: String?//Status of the source. 'Unknown' if all attempts to connect to the source failed.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
