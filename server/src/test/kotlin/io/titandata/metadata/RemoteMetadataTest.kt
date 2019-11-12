package io.titandata.metadata

import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.matchers.types.shouldBeInstanceOf
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Remote
import io.titandata.models.Repository
import org.jetbrains.exposed.sql.transactions.transaction

class RemoteMetadataTest : StringSpec() {

    val md = MetadataProvider()

    override fun beforeSpec(spec: Spec) {
        md.init()
    }

    override fun beforeTest(testCase: TestCase) {
        md.clear()
    }

    init {
        "add remote succeeds" {
            transaction {
                md.createRepository(Repository(name = "foo"))
                md.addRemote("foo", Remote("nop", "origin"))
            }
        }

        "add duplicate remote fails" {
            shouldThrow<ObjectExistsException> {
                transaction {
                    md.createRepository(Repository(name = "foo"))
                    md.addRemote("foo", Remote("nop", "origin"))
                    md.addRemote("foo", Remote("nop", "origin"))
                }
            }
        }

        "get remote succeeds" {
            val remote = transaction {
                md.createRepository(Repository(name = "foo"))
                md.addRemote("foo", Remote("nop", "origin"))
                md.getRemote("foo", "origin")
            }
            remote.name shouldBe "origin"
            remote.provider shouldBe "nop"
        }

        "get non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                transaction {
                    md.createRepository(Repository(name = "foo"))
                    md.getRemote("foo", "origin")
                }
            }
        }

        "list remotes succeeds" {
            val remotes = transaction {
                md.createRepository(Repository(name = "foo"))
                md.addRemote("foo", Remote("nop", "foo"))
                md.addRemote("foo", Remote("s3", "bar", mapOf("bucket" to "bucket")))
                md.listRemotes("foo")
            }

            remotes.size shouldBe 2
            remotes[0].name shouldBe "foo"
            remotes[1].name shouldBe "bar"
            remotes[1].provider shouldBe "s3"
            remotes[1].properties["bucket"] shouldBe "bucket"
        }

        "remove remote succeeds" {
            val remotes = transaction {
                md.createRepository(Repository(name = "foo"))
                md.addRemote("foo", Remote("nop", "foo"))
                md.removeRemote("foo", "foo")
                md.listRemotes("foo")
            }
            remotes.size shouldBe 0
        }

        "remove non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                transaction {
                    md.createRepository(Repository(name = "foo"))
                    md.removeRemote("foo", "foo")
                }
            }
        }

        "update remote succeeds" {
            val remote = transaction {
                md.createRepository(Repository(name = "foo"))
                md.addRemote("foo", Remote("nop", "origin"))
                md.updateRemote("foo", "origin", Remote("s3", "origin", mapOf("bucket" to "bucket")))
                md.getRemote("foo", "origin")
            }
            remote.provider shouldBe "s3"
            remote.name shouldBe "origin"
            remote.properties["bucket"] shouldBe "bucket"
        }

        "update remote to conflicting name fails" {
            shouldThrow<ObjectExistsException> {
                transaction {
                    md.createRepository(Repository(name = "foo"))
                    md.addRemote("foo", Remote("nop", "one"))
                    md.addRemote("foo", Remote("nop", "two"))
                    md.updateRemote("foo", "one", Remote("nop", "two"))
                }
            }
        }

        "rename remote succeeds" {
            val remote = transaction {
                md.createRepository(Repository(name = "foo"))
                md.addRemote("foo", Remote("nop", "origin"))
                md.updateRemote("foo", "origin", Remote("nop", "upstream"))
                md.getRemote("foo", "upstream")
            }
            remote.name shouldBe "upstream"
        }

        "update of non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                transaction {
                    md.createRepository(Repository(name = "foo"))
                    md.updateRemote("foo", "origin", Remote("nop", "upstream"))
                }
            }
        }
    }
}
