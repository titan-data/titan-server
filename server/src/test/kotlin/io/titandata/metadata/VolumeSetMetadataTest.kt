package io.titandata.metadata

import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.titandata.exception.NoSuchObjectException
import io.titandata.models.Commit
import io.titandata.models.Repository
import io.titandata.models.Volume
import java.util.UUID
import org.jetbrains.exposed.sql.transactions.transaction

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
                md.createRepository(Repository(name = "foo"))
                val vs = md.createVolumeSet("foo")
                // Make sure it can be parsed
                UUID.fromString(vs)
            }
        }

        "create volume set with commit source succeeds" {
            transaction {
                md.createRepository(Repository(name = "foo"))
                val src = md.createVolumeSet("foo")
                md.createCommit("foo", src, Commit(id = "id"))
                val dst = md.createVolumeSet("foo", "id")

                md.getCommitSource(dst) shouldBe "id"
            }
        }

        "get active volumeset returns vs if activate" {
            transaction {
                md.createRepository(Repository(name = "foo"))
                val vs = md.createVolumeSet("foo", null, true)
                md.getActiveVolumeSet("foo") shouldBe vs
            }
        }

        "activate volumeset marks other volumeset inactive" {
            transaction {
                md.createRepository(Repository(name = "foo"))
                md.createVolumeSet("foo", null, true)
                val vs = md.createVolumeSet("foo", null, false)
                md.activateVolumeSet("foo", vs)
                md.getActiveVolumeSet("foo") shouldBe vs
            }
        }

        "activate unknown volume set fails" {
            shouldThrow<IllegalArgumentException> {
                transaction {
                    md.createRepository(Repository(name = "foo"))
                    md.activateVolumeSet("foo", UUID.randomUUID().toString())
                }
            }
        }

        "mark volumeset deleting succeeds" {
            transaction {
                md.createRepository(Repository(name = "foo"))
                val vs = md.createVolumeSet("foo")
                md.markVolumeSetDeleting(vs)
            }
        }

        "mark volumeset deleting marks all volumes and commits" {
            transaction {
                md.createRepository(Repository(name = "foo"))
                val vs = md.createVolumeSet("foo")
                md.createVolume(vs, Volume(name = "vol"))
                val commit = Commit(id = "id")
                md.createCommit("foo", vs, commit)
                md.markVolumeSetDeleting(vs)
                shouldThrow<NoSuchObjectException> {
                    md.getVolume(vs, "vol")
                }
                shouldThrow<NoSuchObjectException> {
                    md.getCommit("foo", "vol")
                }
            }
        }

        "mark all volumesets deleting succeeds" {
            transaction {
                md.createRepository(Repository(name = "foo"))
                val vs = md.createVolumeSet("foo")
                md.markAllVolumeSetsDeleting("foo")
                val volumeSets = md.listDeletingVolumeSets()
                volumeSets.size shouldBe 1
                volumeSets[0] shouldBe vs
            }
        }

        "mark active volumeset deleting makes it no longer active" {
            transaction {
                md.createRepository(Repository(name = "foo"))
                val vs = md.createVolumeSet("foo", null, true)
                md.markVolumeSetDeleting(vs)
                // Don't really have a way to verify this other than checking this fails
                shouldThrow<NullPointerException> {
                    md.getActiveVolumeSet("foo")
                }
            }
        }

        "deleting volume set shows up in list" {
            transaction {
                md.createRepository(Repository(name = "foo"))
                val vs = md.createVolumeSet("foo", null, true)
                md.markVolumeSetDeleting(vs)
                val deleting = md.listDeletingVolumeSets()
                deleting.size shouldBe 1
                deleting[0] shouldBe vs
            }
        }

        "volume set deletion succeeds" {
            transaction {
                md.createRepository(Repository(name = "foo"))
                val vs = md.createVolumeSet("foo", null, true)
                md.deleteVolumeSet(vs)
                // Don't really have a way to verify this other than checking this fails
                shouldThrow<NullPointerException> {
                    md.getActiveVolumeSet("foo")
                }
            }
        }
    }
}
