package io.titandata.metadata

import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Repository
import io.titandata.models.Volume
import org.jetbrains.exposed.sql.transactions.transaction

class VolumeMetadataTest : StringSpec() {

    val md = MetadataProvider()

    override fun beforeSpec(spec: Spec) {
        md.init()
    }

    override fun beforeTest(testCase: TestCase) {
        md.clear()
    }

    fun createVolumeSet(): String {
        return transaction {
            md.createRepository(Repository(name = "foo"))
            md.createVolumeSet("foo", null, true)
        }
    }

    init {
        "create volume succeeds" {
            val vs = createVolumeSet()
            transaction {
                md.createVolume(vs, Volume("vol"))
            }
        }

        "create duplicate volume fails" {
            val vs = createVolumeSet()
            shouldThrow<ObjectExistsException> {
                transaction {
                    md.createVolume(vs, Volume("vol"))
                    md.createVolume(vs, Volume("vol"))
                }
            }
        }

        "get volume succeeds" {
            val vs = createVolumeSet()
            transaction {
                md.createVolume(vs, Volume(name = "vol", properties = mapOf("a" to "b"), config = mapOf("c" to "d")))
                val vol = md.getVolume(vs, "vol")
                vol.name shouldBe "vol"
                vol.properties["a"] shouldBe "b"
                vol.config["c"] shouldBe "d"
            }
        }

        "get non-existent volume fails" {
            val vs = createVolumeSet()
            shouldThrow<NoSuchObjectException> {
                transaction {
                    md.getVolume(vs, "vol")
                }
            }
        }

        "mark non-existent volume deleting fails" {
            val vs = createVolumeSet()
            shouldThrow<NoSuchObjectException> {
                transaction {
                    md.markVolumeDeleting(vs, "vol")
                }
            }
        }

        "list volumes succeeds" {
            val vs = createVolumeSet()
            transaction {
                md.createVolume(vs, Volume(name = "vol1", properties = mapOf("a" to "b")))
                md.createVolume(vs, Volume(name = "vol2", properties = mapOf("c" to "d")))
                val vols = md.listVolumes(vs).sortedBy { it.name }
                vols.size shouldBe 2
                vols[0].name shouldBe "vol1"
                vols[0].properties["a"] shouldBe "b"
                vols[1].name shouldBe "vol2"
                vols[1].properties["c"] shouldBe "d"
            }
        }

        "list all volumes succeeds" {
            val vs = createVolumeSet()
            transaction {
                md.createVolume(vs, Volume(name = "vol1", properties = mapOf("a" to "b")))
                md.createVolume(vs, Volume(name = "vol2", properties = mapOf("c" to "d")))
                val vols = md.listAllVolumes().sortedBy { it.name }
                vols.size shouldBe 2
                vols[0].name shouldBe "vol1"
                vols[0].properties["a"] shouldBe "b"
                vols[1].name shouldBe "vol2"
                vols[1].properties["c"] shouldBe "d"
            }
        }

        "update volume config succeeds" {
            val vs = createVolumeSet()
            transaction {
                md.createVolume(vs, Volume("vol"))
                md.updateVolumeConfig(vs, "vol", mapOf("a" to "b"))
                val vol = md.getVolume(vs, "vol")
                vol.config["a"] shouldBe "b"
            }
        }

        "mark volume deleting succeeds" {
            val vs = createVolumeSet()
            transaction {
                md.createVolume(vs, Volume("vol"))
                md.markVolumeDeleting(vs, "vol")
                shouldThrow<NoSuchObjectException> {
                    md.getVolume(vs, "vol")
                }
            }
        }

        "list deleting volumes succeeds" {
            val vs = createVolumeSet()
            val volumes = transaction {
                md.createVolume(vs, Volume("vol"))
                md.markVolumeDeleting(vs, "vol")
                md.listDeletingVolumes()
            }
            volumes.size shouldBe 1
            volumes[0].first shouldBe vs
            volumes[0].second.name shouldBe "vol"
        }

        "delete volume succeeds" {
            val vs = createVolumeSet()
            transaction {
                md.createVolume(vs, Volume("vol"))
                md.deleteVolume(vs, "vol")
                shouldThrow<NoSuchObjectException> {
                    md.getVolume(vs, "vol")
                }
            }
        }

        "delete non-existent volume succeeds" {
            val vs = createVolumeSet()
            shouldThrow<NoSuchObjectException> {
                transaction {
                    md.deleteVolume(vs, "vol")
                }
            }
        }
    }
}
