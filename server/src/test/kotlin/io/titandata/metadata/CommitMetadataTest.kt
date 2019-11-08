package io.titandata.metadata

import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.titandata.exception.NoSuchObjectException
import io.titandata.models.Commit
import io.titandata.models.Repository
import org.jetbrains.exposed.sql.transactions.transaction

class CommitMetadataTest : StringSpec() {

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

    init {
        "create commit without timestamp succeeds" {
            transaction {
                md.createCommit("foo", vs, Commit(id = "id", properties = emptyMap()))
            }
        }

        "create commit with timestamp succeeds" {
            transaction {
                md.createCommit("foo", vs, Commit(id = "id", properties = mapOf("timestmap" to "2019-09-20T13:45:38Z")))
            }
        }

        "get non-existent commit fails" {
            shouldThrow<NoSuchObjectException> {
                transaction {
                    md.getCommit("foo", "id")
                }
            }
        }

        "get commit succeeds" {
            transaction {
                md.createCommit("foo", vs, Commit(id = "id", properties = mapOf("timestamp" to "2019-09-20T13:45:38Z",
                        "a" to "b")))
                val (volumeSet, commit) = md.getCommit("foo", "id")
                volumeSet shouldBe vs
                commit.id shouldBe "id"
                commit.properties["a"] shouldBe "b"
            }
        }

        "list commits returns empty list" {
            val commits = transaction {
                md.listCommits("foo")
            }
            commits.size shouldBe 0
        }

        "list commits succeeds" {
            val commits = transaction {
                md.createCommit("foo", vs, Commit(id = "id", properties = emptyMap()))
                md.listCommits("foo")
            }
            commits.size shouldBe 1
            commits[0].id shouldBe "id"
        }

        "list commits sorts commits in reverse timestamp order" {
            val commits = transaction {
                md.createCommit("foo", vs, Commit(id = "two", properties = mapOf("timestamp" to "2019-09-20T13:46:38Z")))
                md.createCommit("foo", vs, Commit(id = "one", properties = mapOf("timestamp" to "2019-09-20T13:45:38Z")))
                md.createCommit("foo", vs, Commit(id = "four", properties = mapOf("timestamp" to "2019-10-20T14:46:38Z")))
                md.createCommit("foo", vs, Commit(id = "three", properties = mapOf("timestamp" to "2019-10-20T13:46:38Z")))
                md.listCommits("foo")
            }
            commits.size shouldBe 4
            commits[0].id shouldBe "four"
            commits[1].id shouldBe "three"
            commits[2].id shouldBe "two"
            commits[3].id shouldBe "one"
        }

        "get last commit returns latest commit" {
            val commit = transaction {
                val vs2 = md.createVolumeSet("foo")
                md.createCommit("foo", vs2, Commit(id = "two", properties = mapOf("timestamp" to "2019-09-20T13:46:38Z")))
                md.createCommit("foo", vs, Commit(id = "one", properties = mapOf("timestamp" to "2019-09-20T13:45:38Z")))
                md.createCommit("foo", vs2, Commit(id = "four", properties = mapOf("timestamp" to "2019-10-20T14:46:38Z")))
                md.createCommit("foo", vs, Commit(id = "three", properties = mapOf("timestamp" to "2019-10-20T13:46:38Z")))
                md.getLastCommit("foo")
            }
            commit shouldBe "four"
        }

        "get commit source returns previous commit in volumeset" {
            val commit = transaction {
                md.createCommit("foo", vs, Commit(id = "one", properties = mapOf("timestamp" to "2019-09-20T13:45:38Z")))
                md.createCommit("foo", vs, Commit(id = "two", properties = mapOf("timestamp" to "2019-09-20T13:46:38Z")))
                md.getCommitSource(vs)
            }
            commit shouldBe "two"
        }

        "get commit source returns volumeset source" {
            val commit = transaction {
                md.createCommit("foo", vs, Commit(id = "id", properties = emptyMap()))
                val vs2 = md.createVolumeSet("foo", "id")

                md.getCommitSource(vs2)
            }
            commit shouldBe "id"
        }

        "get commit source returns null if no commits exists" {
            val commit = transaction {
                md.getCommitSource(vs)
            }
            commit shouldBe null
        }

        "update commit succeeds" {
            val commit = transaction {
                md.createCommit("foo", vs, Commit(id = "id", properties = mapOf("timestamp" to "2019-09-20T13:45:38Z",
                        "a" to "b")))
                md.updateCommit("foo", Commit(id = "id", properties = mapOf("timestamp" to "2019-09-20T13:45:39Z",
                        "a" to "c")))
                md.getCommit("foo", "id").second
            }
            commit.properties["a"] shouldBe "c"
            commit.properties["timestamp"] shouldBe "2019-09-20T13:45:39Z"
        }

        "tags filtering works after updating commit" {
            transaction {
                md.createCommit("foo", vs, Commit(id = "id", properties = mapOf("timestamp" to "2019-09-20T13:45:38Z",
                        "tags" to mapOf("a" to "b"))))
                md.updateCommit("foo", Commit(id = "id", properties = mapOf("timestamp" to "2019-09-20T13:45:39Z",
                        "tags" to mapOf("a" to "c"))))
                md.listCommits("foo", listOf("a=b")).size shouldBe 0
                md.listCommits("foo", listOf("a=c")).size shouldBe 1
            }
        }

        "update non-existent commit fails" {
            shouldThrow<NoSuchObjectException> {
                transaction {
                    md.updateCommit("foo", Commit(id = "id", properties = emptyMap()))
                }
            }
        }

        "create commit with tags succeeds" {
            transaction {
                md.createCommit("foo", vs, Commit(id = "id", properties = mapOf("tags" to mapOf(
                        "a" to "b",
                        "c" to ""
                ))))
            }
        }

        "filter by tag existence succeeds" {
            val commits = transaction {
                md.createCommit("foo", vs, Commit(id = "one", properties = mapOf("timestamp" to "2019-09-20T13:45:38Z", "tags" to mapOf(
                        "a" to "b",
                        "c" to "d"
                ))))
                md.createCommit("foo", vs, Commit(id = "two", properties = mapOf("timestamp" to "2019-09-20T13:45:37Z", "tags" to mapOf(
                        "a" to "e"
                ))))
                md.createCommit("foo", vs, Commit(id = "three", properties = mapOf("timestamp" to "2019-09-20T13:45:36Z", "tags" to mapOf(
                        "c" to "d"
                ))))
                md.listCommits("foo", listOf("a"))
            }
            commits.size shouldBe 2
            commits[0].id shouldBe "one"
            commits[1].id shouldBe "two"
        }

        "filter by exact tag succeeds" {
            val commits = transaction {
                md.createCommit("foo", vs, Commit(id = "one", properties = mapOf("timestamp" to "2019-09-20T13:45:38Z", "tags" to mapOf(
                        "a" to "b",
                        "c" to "d"
                ))))
                md.createCommit("foo", vs, Commit(id = "two", properties = mapOf("timestamp" to "2019-09-20T13:45:37Z", "tags" to mapOf(
                        "a" to "e"
                ))))
                md.createCommit("foo", vs, Commit(id = "three", properties = mapOf("timestamp" to "2019-09-20T13:45:36Z", "tags" to mapOf(
                        "c" to "d"
                ))))
                md.listCommits("foo", listOf("a=b"))
            }
            commits.size shouldBe 1
            commits[0].id shouldBe "one"
        }

        "filter by multiple tag succeeds" {
            val commits = transaction {
                md.createCommit("foo", vs, Commit(id = "one", properties = mapOf("timestamp" to "2019-09-20T13:45:38Z", "tags" to mapOf(
                        "a" to "b",
                        "c" to "d"
                ))))
                md.createCommit("foo", vs, Commit(id = "two", properties = mapOf("timestamp" to "2019-09-20T13:45:37Z", "tags" to mapOf(
                        "a" to "e",
                        "c" to "d"
                ))))
                md.createCommit("foo", vs, Commit(id = "three", properties = mapOf("timestamp" to "2019-09-20T13:45:36Z", "tags" to mapOf(
                        "c" to "e"
                ))))
                md.createCommit("foo", vs, Commit(id = "four", properties = mapOf("timestamp" to "2019-09-20T13:45:35Z", "tags" to mapOf(
                        "a" to "b"
                ))))
                md.listCommits("foo", listOf("c", "a=b"))
            }
            commits.size shouldBe 1
            commits[0].id shouldBe "one"
        }

        "list deleting commits succeeds" {
            transaction {
                md.createCommit("foo", vs, Commit(id = "one", properties = emptyMap()))
                val vs2 = md.createVolumeSet("foo")
                md.createCommit("foo", vs2, Commit(id = "two", properties = emptyMap()))
                md.createCommit("foo", vs2, Commit(id = "three", properties = emptyMap()))

                md.markCommitDeleting("foo", "one")
                md.markCommitDeleting("foo", "two")

                val commits = md.listDeletingCommits().sortedBy { it.id }

                commits.size shouldBe 2
                commits[0].guid shouldBe "one"
                commits[0].volumeSet shouldBe vs
                commits[1].guid shouldBe "two"
                commits[1].volumeSet shouldBe vs2
            }
        }

        "has clone returns false with no clone" {
            transaction {
                md.createCommit("foo", vs, Commit(id = "id", properties = emptyMap()))
                md.markCommitDeleting("foo", "id")
                val commits = md.listDeletingCommits()

                commits.size shouldBe 1

                md.hasClones(commits[0]) shouldBe false
            }
        }

        "has clone returns true with volumeset clone" {
            transaction {
                md.createCommit("foo", vs, Commit(id = "id", properties = emptyMap()))
                md.createVolumeSet("foo", "id")
                md.markCommitDeleting("foo", "id")
                val commits = md.listDeletingCommits()

                commits.size shouldBe 1

                md.hasClones(commits[0]) shouldBe true
            }
        }

        "mark non-existent commit deleting fails" {
            shouldThrow<NoSuchObjectException> {
                transaction {
                    md.markCommitDeleting("foo", "nosuchcommit")
                }
            }
        }

        "delete commit succeeds" {
            transaction {
                md.createCommit("foo", vs, Commit(id = "id", properties = emptyMap()))
                md.markCommitDeleting("foo", "id")
                val commits = md.listDeletingCommits()
                commits.size shouldBe 1
                md.deleteCommit(commits[0])
                md.listDeletingCommits().size shouldBe 0
            }
        }
    }
}
