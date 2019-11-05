package io.titandata.metadata

import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.matchers.types.shouldBeInstanceOf
import io.kotlintest.shouldBe
import io.kotlintest.shouldFail
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Repository
import io.titandata.remote.nop.NopRemote
import io.titandata.remote.s3.S3Remote
import org.jetbrains.exposed.sql.transactions.transaction
import java.util.*

class VolumeSetMetadataTest : StringSpec() {

    val md = MetadataProvider()

    override fun beforeSpec(spec: Spec) {
        md.init()
    }

    override fun beforeTest(testCase: TestCase) {
        md.clear()
    }

    init {
        "create volume set succeeds" {
            transaction {
                md.createRepository(Repository(name="foo", properties=mapOf()))
                val vs = md.createVolumeSet("foo")
                // Make sure it can be parsed
                UUID.fromString(vs)
            }
        }

        "create volume set for non-existent repo fails" {
            shouldThrow<NoSuchObjectException> {
                transaction {
                    md.createVolumeSet("foo")
                }
            }
        }

        "get active volumeset returns vs if activate" {
            transaction {
                md.createRepository(Repository(name="foo", properties=mapOf()))
                val vs = md.createVolumeSet("foo", true)
                md.getActiveVolumeSet("foo") shouldBe vs
            }
        }

        "activate volumeset marks other volumeset inactive" {
            transaction {
                md.createRepository(Repository(name="foo", properties=mapOf()))
                md.createVolumeSet("foo", true)
                val vs = md.createVolumeSet("foo", false)
                md.activateVolumeSet("foo", vs)
                md.getActiveVolumeSet("foo") shouldBe vs
            }
        }

        "mark volumeset deleting succeeds" {
            transaction {
                md.createRepository(Repository(name="foo", properties=mapOf()))
                val vs = md.createVolumeSet("foo")
                md.markVolumeSetDeleting(vs)
            }
        }

        "mark active volumeset deleting makes it no longer active" {
            transaction {
                md.createRepository(Repository(name="foo", properties=mapOf()))
                val vs = md.createVolumeSet("foo", true)
                md.markVolumeSetDeleting(vs)
                // Don't really have a way to verify this other than checking this fails
                shouldThrow<NullPointerException> {
                    md.getActiveVolumeSet("foo")
                }
            }
        }

        "deleting volume set shows up in list" {
            transaction {
                md.createRepository(Repository(name="foo", properties=mapOf()))
                val vs = md.createVolumeSet("foo", true)
                md.markVolumeSetDeleting(vs)
                val deleting = md.listDeletingVolumeSets()
                deleting.size shouldBe 1
                deleting[0] shouldBe vs
            }
        }

        "volume set deletion succeeds" {
            transaction {
                md.createRepository(Repository(name="foo", properties=mapOf()))
                val vs = md.createVolumeSet("foo", true)
                md.deleteVolumeSet(vs)
                // Don't really have a way to verify this other than checking this fails
                shouldThrow<NullPointerException> {
                    md.getActiveVolumeSet("foo")
                }
            }
        }
    }
}
