package io.titandata.metadata

import io.kotlintest.TestCase
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Repository
import org.jetbrains.exposed.sql.transactions.transaction

class MetadataProviderTest : StringSpec() {

    companion object {
        var dbIdentifier = 0
    }

    lateinit var md : MetadataProvider

    override fun beforeTest(testCase: TestCase) {
        dbIdentifier++
        md = MetadataProvider(true, "db${dbIdentifier}")
        md.init()
    }

    init {
        "list repositories returns empty list" {
            transaction {
                md.listRepositories().size shouldBe 0
            }
        }

        "create repository succeeds" {
            transaction {
                val repo = Repository(name="foo", properties=mapOf("a" to "b"))
                md.createRepository(repo)
            }
        }

        "get repository succeeds" {
            transaction {
                md.createRepository(Repository(name = "foo", properties = mapOf("a" to "b")))
                val result = md.getRepository("foo")
                result.name shouldBe "foo"
                result.properties["a"] shouldBe "b"
            }
        }

        "get non-existent repository fails" {
            shouldThrow<NoSuchObjectException> {
                transaction {
                    md.getRepository("foo")
                }
            }
        }

        "list repository succeeds" {
            transaction {
                md.createRepository(Repository(name = "foo", properties = mapOf("a" to "b")))
                md.createRepository(Repository(name = "bar", properties = mapOf("b" to "b")))
                val result = md.listRepositories()
                result.size shouldBe 2
                result.find { it.name == "foo" } shouldNotBe null
                result.find { it.name == "bar" } shouldNotBe null
            }
        }

        "create duplicate repository fails" {
            shouldThrow<ObjectExistsException> {
                transaction {
                    md.createRepository(Repository(name = "foo", properties = mapOf("a" to "b")))
                    md.createRepository(Repository(name = "foo", properties = mapOf("a" to "b")))
                }
            }
        }

        "delete repository succeeds" {
            shouldThrow<NoSuchObjectException> {
                transaction {
                    md.createRepository(Repository(name = "foo", properties = mapOf("a" to "b")))
                    md.deleteRepository("foo")
                    md.getRepository("foo")
                }
            }
        }

        "delete non-existent repository fails" {
            shouldThrow<NoSuchObjectException> {
                transaction {
                    md.deleteRepository("foo")
                }
            }
        }

        "update repository succeeds" {
            transaction {
                md.createRepository(Repository(name = "foo", properties = mapOf("a" to "b")))
                md.updateRepository("foo", Repository(name = "foo", properties = mapOf("a" to "c")))
                val result = md.getRepository("foo")
                result.properties["a"] shouldBe "c"
            }
        }

        "rename repository succeeds" {
            transaction {
                md.createRepository(Repository(name = "foo", properties = mapOf("a" to "b")))
                md.updateRepository("foo", Repository(name = "bar", properties = mapOf("a" to "c")))
                val result = md.getRepository("bar")
                result.properties["a"] shouldBe "c"
            }
        }

        "rename repository to conflicting name fails" {
            shouldThrow<ObjectExistsException> {
                transaction {
                    md.createRepository(Repository(name = "foo", properties = mapOf("a" to "b")))
                    md.createRepository(Repository(name = "bar", properties = mapOf("a" to "b")))
                    md.updateRepository("foo", Repository(name = "bar", properties = mapOf("a" to "c")))
                }
            }
        }

        "update of non-existent repository fails" {
            shouldThrow<NoSuchObjectException> {
                transaction {
                    md.updateRepository("foo", Repository(name = "bar", properties = mapOf("a" to "c")))
                }
            }
        }
    }

}