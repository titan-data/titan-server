/*
 * Copyright (c) 2019 by Delphix. All rights reserved.
 */

package io.titandata.serialization

import io.titandata.models.EngineRemote
import io.titandata.models.NopRemote
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import com.google.gson.GsonBuilder
import com.google.gson.JsonParseException
import com.google.gson.reflect.TypeToken
import io.kotlintest.matchers.types.shouldBeInstanceOf
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec

class SerializationTest : StringSpec() {

    val gson = ModelTypeAdapters.configure(GsonBuilder()).create()

    init {
        "deserializing a list of remotes succeeds" {
            val listType = object : TypeToken<List<Remote>>() { }.type
            val result = gson.fromJson<List<Remote>>("[{\"provider\":\"nop\",\"name\":\"foo\"}," +
                    "{\"provider\":\"engine\",\"name\":\"bar\",\"address\":\"a\"," +
                    "\"username\":\"u\",\"password\":\"p\"}]", listType)
            result.size shouldBe 2
            result[0].shouldBeInstanceOf<NopRemote>()
            result[0].provider shouldBe "nop"
            result[0].name shouldBe "foo"
            result[1].shouldBeInstanceOf<EngineRemote>()
            result[1].provider shouldBe "engine"
            result[1].name shouldBe "bar"
            (result[1] as EngineRemote).username shouldBe "u"
            (result[1] as EngineRemote).address shouldBe "a"
            (result[1] as EngineRemote).password shouldBe "p"
        }

        "deserializing an unknown request provider fails" {
            shouldThrow<JsonParseException> {
                gson.fromJson("{\"provider\":\"blah\"}",
                        RemoteParameters::class.java)
            }
        }
    }
}
