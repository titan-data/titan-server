/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * The representation of a host object.
 */
interface Host : ReadonlyNamedUserObject {
    val privilegeElevationProfile: String?//Profile for escalating user privileges.
    val sshPort: Int?//The port number used to connect to the host via SSH.
    val hostRuntime: HostRuntime?//Runtime properties for this host.
    val address: String?//The address associated with the host.
    val hostConfiguration: HostConfiguration?//The host configuration object associated with the host.
    val dateAdded: String?//The date the host was added.
    override val name: String?//Object name.
    override val reference: String?//The object reference.
    override val namespace: String?//Alternate namespace for this object, for replicated and restored objects.
    override val type: String
    override fun toMap(): Map<String, Any?>
}
