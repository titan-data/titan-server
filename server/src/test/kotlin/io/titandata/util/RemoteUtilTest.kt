package io.titandata.util

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.titandata.serialization.RemoteUtil

class RemoteUtilTest : StringSpec() {

    private val util = RemoteUtil()

    /*
     * The remotes themselves have extensive tests. This is just to test that they are wired up correctly into the
     * client.
     */
    init {

        "parse nop URI succeeds" {
            val remote = util.parseUri("nop", "origin", emptyMap())
            remote.provider shouldBe "nop"
            remote.name shouldBe "origin"
            remote.properties.size shouldBe 0
        }

        "parse ssh URI succeeds" {
            val remote = util.parseUri("ssh://user@host/path", "origin", mapOf("keyFile" to "/keyfile"))
            remote.provider shouldBe "ssh"
            remote.properties["username"] shouldBe "user"
            remote.properties["address"] shouldBe "host"
            remote.properties["path"] shouldBe "/path"
            remote.properties["keyFile"] shouldBe "/keyfile"
        }

        "parse s3 URI succeeds" {
            val remote = util.parseUri("s3://bucket/path", "origin", mapOf("accessKey" to "ACCESS", "secretKey" to "SECRET"))
            remote.provider shouldBe "s3"
            remote.properties["bucket"] shouldBe "bucket"
            remote.properties["path"] shouldBe "path"
            remote.properties["accessKey"] shouldBe "ACCESS"
            remote.properties["secretKey"] shouldBe "SECRET"
        }

        "parse s3web URI succeeds" {
            val remote = util.parseUri("s3web://host/path", "origin", emptyMap())
            remote.provider shouldBe "s3web"
            remote.properties["url"] shouldBe "http://host/path"
        }
    }
}
