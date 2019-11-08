package io.titandata.metadata

import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.matchers.types.shouldBeInstanceOf
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Repository
import io.titandata.remote.nop.NopRemote
import io.titandata.remote.s3.S3Remote
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
                md.createRepository(Repository(name = "foo", properties = mapOf()))
                md.addRemote("foo", NopRemote(name = "origin"))
            }
        }

        "add duplicate remote fails" {
            shouldThrow<ObjectExistsException> {
                transaction {
                    md.createRepository(Repository(name = "foo", properties = mapOf()))
                    md.addRemote("foo", NopRemote(name = "origin"))
                    md.addRemote("foo", NopRemote(name = "origin"))
                }
            }
        }

        "get remote succeeds" {
            val remote = transaction {
                md.createRepository(Repository(name = "foo", properties = mapOf()))
                md.addRemote("foo", NopRemote(name = "origin"))
                md.getRemote("foo", "origin")
            }
            remote.name shouldBe "origin"
            remote.shouldBeInstanceOf<NopRemote>()
        }

        "get non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                transaction {
                    md.createRepository(Repository(name = "foo", properties = mapOf()))
                    md.getRemote("foo", "origin")
                }
            }
        }

        "list remotes succeeds" {
            val remotes = transaction {
                md.createRepository(Repository(name = "foo", properties = mapOf()))
                md.addRemote("foo", NopRemote(name = "foo"))
                md.addRemote("foo", S3Remote(name = "bar", bucket = "bucket"))
                md.listRemotes("foo")
            }

            remotes.size shouldBe 2
            remotes[0].name shouldBe "foo"
            remotes[1].name shouldBe "bar"
            remotes[1].shouldBeInstanceOf<S3Remote>()
            val s3 = remotes[1] as S3Remote
            s3.bucket shouldBe "bucket"
        }

        "remove remote succeeds" {
            val remotes = transaction {
                md.createRepository(Repository(name = "foo", properties = mapOf()))
                md.addRemote("foo", NopRemote(name = "foo"))
                md.removeRemote("foo", "foo")
                md.listRemotes("foo")
            }
            remotes.size shouldBe 0
        }

        "remove non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                transaction {
                    md.createRepository(Repository(name = "foo", properties = mapOf()))
                    md.removeRemote("foo", "foo")
                }
            }
        }

        "update remote succeeds" {
            val remote = transaction {
                md.createRepository(Repository(name = "foo", properties = mapOf()))
                md.addRemote("foo", NopRemote(name = "origin"))
                md.updateRemote("foo", "origin", S3Remote(name = "origin", bucket = "bucket"))
                md.getRemote("foo", "origin")
            }
            remote.shouldBeInstanceOf<S3Remote>()
            remote.name shouldBe "origin"
            val s3 = remote as S3Remote
            s3.bucket shouldBe "bucket"
        }

        "update remote to conflicting name fails" {
            shouldThrow<ObjectExistsException> {
                transaction {
                    md.createRepository(Repository(name = "foo", properties = mapOf()))
                    md.addRemote("foo", NopRemote(name = "one"))
                    md.addRemote("foo", NopRemote(name = "two"))
                    md.updateRemote("foo", "one", NopRemote(name = "two"))
                }
            }
        }

        "rename remote succeeds" {
            val remote = transaction {
                md.createRepository(Repository(name = "foo", properties = mapOf()))
                md.addRemote("foo", NopRemote(name = "origin"))
                md.updateRemote("foo", "origin", NopRemote(name = "upstream"))
                md.getRemote("foo", "upstream")
            }
            remote.name shouldBe "upstream"
        }

        "update of non-existent remote fails" {
            shouldThrow<NoSuchObjectException> {
                transaction {
                    md.createRepository(Repository(name = "foo", properties = mapOf()))
                    md.updateRemote("foo", "origin", NopRemote(name = "upstream"))
                }
            }
        }
    }
}
