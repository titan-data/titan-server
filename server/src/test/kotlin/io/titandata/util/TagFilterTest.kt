package io.titandata.util

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.titandata.models.Commit

class TagFilterTest : StringSpec() {

    fun makeCommit(tags: Map<String, String>? = null): Commit {
        if (tags != null) {
            return Commit(id = "id", properties = mapOf("tags" to tags as Any))
        } else {
            return Commit(id = "id", properties = mapOf())
        }
    }

    init {
        "null tags allows any commit" {
            TagFilter(null).match(makeCommit()) shouldBe true
            TagFilter(null).match(makeCommit(mapOf("a" to "b"))) shouldBe true
            TagFilter(null).match(makeCommit(mapOf("c" to "d"))) shouldBe true
        }

        "empty tags allows any commit" {
            TagFilter(listOf()).match(makeCommit()) shouldBe true
            TagFilter(listOf()).match(makeCommit(mapOf("a" to "b"))) shouldBe true
            TagFilter(listOf()).match(makeCommit(mapOf("c" to "d"))) shouldBe true
        }

        "exact match works correctly" {
            TagFilter(listOf("a=b")).match(makeCommit()) shouldBe false
            TagFilter(listOf("a=b")).match(makeCommit(mapOf("a" to "b"))) shouldBe true
            TagFilter(listOf("a=b")).match(makeCommit(mapOf("c" to "d"))) shouldBe false
        }

        "existence match works correctly" {
            TagFilter(listOf("a")).match(makeCommit()) shouldBe false
            TagFilter(listOf("a")).match(makeCommit(mapOf("a" to "b"))) shouldBe true
            TagFilter(listOf("a")).match(makeCommit(mapOf("c" to "d"))) shouldBe false
        }

        "multiple checks works correctly" {
            val filter = TagFilter(listOf("a", "c=d"))
            filter.match(makeCommit(mapOf("a" to "b"))) shouldBe false
            filter.match(makeCommit(mapOf("c" to "d"))) shouldBe false
            filter.match(makeCommit(mapOf("a" to "b", "c" to "d"))) shouldBe true
        }

        "filter prunes list" {
            val filter = TagFilter(listOf("a=b"))
            val commits = filter.filter(listOf(makeCommit(), makeCommit(mapOf("a" to "b")),
                    makeCommit(mapOf("c" to "d"))))
            commits.size shouldBe 1
            @Suppress("UNCHECKED_CAST")
            val tags = commits[0].properties["tags"] as Map<String, String>
            tags["a"] shouldBe "b"
        }
    }
}
