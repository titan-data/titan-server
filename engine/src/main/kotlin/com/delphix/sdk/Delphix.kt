/**
 * Copyright (c) 2019 by Delphix. All rights reserved. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.delphix.sdk

import com.delphix.sdk.repos.Action
import com.delphix.sdk.repos.Container
import com.delphix.sdk.repos.EnvironmentUser
import com.delphix.sdk.repos.Group
import com.delphix.sdk.repos.Host
import com.delphix.sdk.repos.Job
import com.delphix.sdk.repos.Repository
import com.delphix.sdk.repos.Source
import com.delphix.sdk.repos.SourceConfig
import com.delphix.sdk.repos.SourceEnvironment
import com.delphix.sdk.repos.TimeflowSnapshot

open class Delphix (
    var http: Http
){
    val loginResource: String = "/resources/json/delphix/login"

    fun requestLogin(username: String, password: String): Map<String, String> {
        return mapOf("type" to "LoginRequest", "username" to username, "password" to password)
    }

    open fun login(username: String, password: String) {
        http.setSession()
        http.handlePost(loginResource, requestLogin(username, password))
    }

    fun action(): Action {
        return Action(http)
    }

    fun job(): Job {
        return Job(http)
    }

    fun container(): Container {
        return Container(http)
    }

    fun group(): Group {
      return Group(http)
    }

    fun repository(): Repository {
      return Repository(http)
    }

    fun source(): Source {
        return Source(http)
    }

    fun sourceConfig(): SourceConfig {
        return SourceConfig(http)
    }

    fun sourceEnvironment(): SourceEnvironment {
        return SourceEnvironment(http)
    }

    fun snapshot(): TimeflowSnapshot {
        return TimeflowSnapshot(http)
    }

    fun host(): Host {
        return Host(http)
    }

    fun environmentUser(): EnvironmentUser {
        return EnvironmentUser(http)
    }
}
