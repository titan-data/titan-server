/**
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */
package com.delphix.sdk.objects

/**
 * Delphix users.
 */
open class User (
    open val lastName: String? = null,//Last name of user.
    open val mobilePhoneNumber: String? = null,//Mobile phone number of user.
    open val homePhoneNumber: String? = null,//Home phone number of user.
    open val publicKey: Any? = null,//Public key used for authentication.
    open val locale: String? = null,//Preferred locale as an IETF BCP 47 language tag, defaults to 'en-US'.
    open val passwordUpdateRequested: Boolean? = null,//True if the user's password should be updated.
    open val enabled: Boolean? = null,//True if the user is currently enabled and can log into the system.
    open val principal: String? = null,//Principal name used for authentication.
    open val firstName: String? = null,//First name of user.
    open val emailAddress: String? = null,//Email address for the user.
    open val isDefault: Boolean? = null,//True if this is the default user and cannot be deleted.
    open val credential: PasswordCredential? = null,//Credential used for authentication.
    open val workPhoneNumber: String? = null,//Work phone number of user.
    override val name: String? = null,//Object name.
    open val sessionTimeout: Int? = null,//Session timeout in minutes.
    open val authenticationType: String? = null,//User authentication type.
    open val userType: String? = null,//Type of user.
    override val reference: String? = null,//The object reference.
    override val namespace: String? = null,//Alternate namespace for this object, for replicated and restored objects.
    override val type: String = "User"
) : NamedUserObject {
    override fun toMap(): Map<String, Any?> {
        return mapOf(
            "lastName" to lastName,
            "mobilePhoneNumber" to mobilePhoneNumber,
            "homePhoneNumber" to homePhoneNumber,
            "publicKey" to publicKey,
            "locale" to locale,
            "passwordUpdateRequested" to passwordUpdateRequested,
            "enabled" to enabled,
            "principal" to principal,
            "firstName" to firstName,
            "emailAddress" to emailAddress,
            "isDefault" to isDefault,
            "credential" to credential,
            "workPhoneNumber" to workPhoneNumber,
            "name" to name,
            "sessionTimeout" to sessionTimeout,
            "authenticationType" to authenticationType,
            "userType" to userType,
            "reference" to reference,
            "namespace" to namespace,
            "type" to type
        )
    }
}

