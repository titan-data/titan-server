package io.titandata.metadata

import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.matchers.types.shouldBeInstanceOf
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.titandata.exception.NoSuchObjectException
import io.titandata.models.Operation
import io.titandata.models.Repository
import io.titandata.remote.nop.NopParameters
import io.titandata.storage.OperationData
import org.jetbrains.exposed.sql.transactions.transaction

class OperationMetadataTest : StringSpec() {

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
        }
    }

    fun buildOperationData(id: String): OperationData {
        return OperationData(
                metadataOnly = false,
                params = NopParameters(),
                operation = Operation(
                        id = id,
                        type = Operation.Type.PULL,
                        state = Operation.State.RUNNING,
                        remote = "origin",
                        commitId = "id")
        )
    }

    init {
        "create operation succeeds" {
            transaction {
                md.createOperation("foo", vs, buildOperationData(vs))
                val op = md.getOperation(vs)
                op.metadataOnly shouldBe false
                op.params.shouldBeInstanceOf<NopParameters>()
                op.operation.id shouldBe vs
                op.operation.type shouldBe Operation.Type.PULL
                op.operation.state shouldBe Operation.State.RUNNING
                op.operation.remote shouldBe "origin"
                op.operation.commitId shouldBe "id"
            }
        }

        "list operations succeeds" {
            transaction {
                md.createOperation("foo", vs, buildOperationData(vs))
                md.createRepository(Repository(name = "bar", properties = emptyMap()))
                val vs2 = md.createVolumeSet("bar")
                md.createOperation("bar", vs2, buildOperationData(vs2))
                val result = md.listOperations("foo")
                result.size shouldBe 1
                result[0].operation.id shouldBe vs
            }
        }

        "update operation state succeeds" {
            transaction {
                md.createOperation("foo", vs, buildOperationData(vs))
                md.updateOperationState(vs, Operation.State.COMPLETE)
                val op = md.getOperation(vs)
                op.operation.state shouldBe Operation.State.COMPLETE
            }
        }

        "delete operation succeeds" {
            transaction {
                md.createOperation("foo", vs, buildOperationData(vs))
                md.deleteOperation(vs)
            }

            shouldThrow<NoSuchObjectException> {
                transaction {
                    md.getOperation(vs)
                }
            }
        }

        "delete non-existent operation fails" {
            shouldThrow<NoSuchObjectException> {
                transaction {
                    md.deleteOperation(vs)
                }
            }
        }

        "operation in progress returns false when none in progress" {
            transaction {
                md.operationInProgress("foo", Operation.Type.PULL, "id", null) shouldBe null
            }
        }

        "operation in progress returns false when criteria doesn't match" {
            transaction {
                md.createOperation("foo", vs, buildOperationData(vs))

                md.operationInProgress("foo", Operation.Type.PULL, "id", "badremote") shouldBe null
                md.operationInProgress("foo", Operation.Type.PULL, "badid", "origin") shouldBe null
                md.operationInProgress("foo", Operation.Type.PUSH, "id", "origin") shouldBe null
                md.operationInProgress("bar", Operation.Type.PULL, "id", "origin") shouldBe null
            }
        }

        "operation in progress succeeds if operation is in progress" {
            transaction {
                md.createOperation("foo", vs, buildOperationData(vs))

                md.operationInProgress("foo", Operation.Type.PULL, "id", "origin") shouldBe vs
                md.operationInProgress("foo", Operation.Type.PULL, "id", null) shouldBe vs
            }
        }
    }
}
