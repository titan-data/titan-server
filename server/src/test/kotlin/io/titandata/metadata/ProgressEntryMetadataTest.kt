package io.titandata.metadata

import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.RemoteParameters
import io.titandata.models.Repository
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
                    params = RemoteParameters("nop"),
                    repo = "foo",
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

        "add failed progress entry updates operation state" {
            val op = transaction {
                md.addProgressEntry(vs, ProgressEntry(type = ProgressEntry.Type.FAILED, message = "message"))
                md.getOperation(vs)
            }
            op.operation.state shouldBe Operation.State.FAILED
        }

        "add aborted progress entry updates operation state" {
            val op = transaction {
                md.addProgressEntry(vs, ProgressEntry(type = ProgressEntry.Type.ABORT, message = "message"))
                md.getOperation(vs)
            }
            op.operation.state shouldBe Operation.State.ABORTED
        }

        "add completed progress entry updates operation state" {
            val op = transaction {
                md.addProgressEntry(vs, ProgressEntry(type = ProgressEntry.Type.COMPLETE, message = "message"))
                md.getOperation(vs)
            }
            op.operation.state shouldBe Operation.State.COMPLETE
        }
    }
}
