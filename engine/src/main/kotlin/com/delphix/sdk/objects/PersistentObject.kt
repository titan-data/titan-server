/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Super schema for all typed schemas with a reference property.
 */
interface PersistentObject : TypedObject {
    val reference: String?//The object reference.
    val namespace: String?//Alternate namespace for this object, for replicated and restored objects.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
