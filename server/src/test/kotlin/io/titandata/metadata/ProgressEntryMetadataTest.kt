package io.titandata.metadata

import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Repository
import io.titandata.remote.nop.NopParameters
import io.titandata.storage.OperationData
import org.jetbrains.exposed.sql.transactions.transaction

class ProgressEntryMetadataTest : StringSpec() {

    val md = MetadataProvider()
    lateinit var vs: String

    override fun beforeSpec(spec: Spec) {
        md.init()
    }

    override fun beforeTest(testCase: TestCase) {
        md.clear()
        transaction {
            md.createRepository(Repository(name = "foo", properties = emptyMap()))
            vs = md.createVolumeSet("foo", null, true)
            md.createOperation("foo", vs, OperationData(
                    metadataOnly = false,
                    params = NopParameters(),
                    operation = Operation(
                            id = vs,
                            type = Operation.Type.PULL,
                            state = Operation.State.RUNNING,
                            remote = "origin",
                            commitId = "id"))
            )
        }
    }

    init {
        "add progress entry succeeds" {
            transaction {
                md.addProgressEntry(vs, ProgressEntry(type = ProgressEntry.Type.MESSAGE, message = "one"))
            }
        }

        "list progress entries returns entries in order" {
            transaction {
                md.addProgressEntry(vs, ProgressEntry(type = ProgressEntry.Type.MESSAGE, message = "one"))
                md.addProgressEntry(vs, ProgressEntry(type = ProgressEntry.Type.MESSAGE, message = "two"))
                md.addProgressEntry(vs, ProgressEntry(type = ProgressEntry.Type.MESSAGE, message = "three"))
                val entries = md.listProgressEntries(vs)
                entries.size shouldBe 3
                entries[0].type shouldBe ProgressEntry.Type.MESSAGE
                entries[0].message shouldBe "one"
                entries[0].percent shouldBe null
                entries[1].message shouldBe "two"
                entries[2].message shouldBe "three"
            }
        }

        "progress entry with percentage is persisted" {
            transaction {
                md.addProgressEntry(vs, ProgressEntry(type = ProgressEntry.Type.PROGRESS, percent = 5))
                val entries = md.listProgressEntries(vs)
                entries.size shouldBe 1
                entries[0].type shouldBe ProgressEntry.Type.PROGRESS
                entries[0].percent shouldBe 5
                entries[0].message shouldBe null
            }
        }

        "list progress entries succeeds after offset" {
            transaction {
                md.addProgressEntry(vs, ProgressEntry(type = ProgressEntry.Type.MESSAGE, message = "one"))
                val offset = md.listProgressEntries(vs)[0].id
                md.addProgressEntry(vs, ProgressEntry(type = ProgressEntry.Type.MESSAGE, message = "two"))
                md.addProgressEntry(vs, ProgressEntry(type = ProgressEntry.Type.MESSAGE, message = "three"))
                val entries = md.listProgressEntries(vs, offset)
                entries.size shouldBe 2
                entries[0].message shouldBe "two"
                entries[1].message shouldBe "three"
            }
        }

        "operation with progress entries can be deleted" {
            transaction {
                md.addProgressEntry(vs, ProgressEntry(type = ProgressEntry.Type.MESSAGE, message = "one"))
                md.deleteOperation(vs)
            }
        }
    }
}
