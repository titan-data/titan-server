/*
 * Copyright The Titan Project Contributors.
 */
package io.titandata.util

import io.titandata.models.Commit

/**
 * This is a utility class that helps with matching tag searches to commits. While we expect to provide more enhanced
 * filtering capabilities (e.g. doing it server-side for remote logs), this will always serve as a foundation where
 * we need basic filtering capabilities.
 */
class TagFilter(val tags: List<String> ?) {

    private val TAGS_METADATA = "tags"
    private val matches: List<Pair<String, String?>>

    init {
        matches = tags?.map {
            if (it.contains("=")) {
                Pair(it.substringBefore("="), it.substringAfter("="))
            } else {
                Pair(it, null)
            }
        } ?: listOf()
    }

    fun match(commit: Commit): Boolean {
        if (matches.size == 0) {
            return true
        }

        val metadata = commit.properties.get(TAGS_METADATA)
        if (metadata == null || metadata !is Map<*, *>) {
            return false
        }
        @Suppress("UNCHECKED_CAST")
        metadata as Map<String, String>

        for (match in matches) {
            val key = match.first
            if (!metadata.containsKey(key)) {
                return false
            }

            if (match.second != null && metadata.get(key) != match.second) {
                return false
            }
        }

        return true
    }

    fun filter(commits: List<Commit>): List<Commit> {
        return commits.filter { match(it) }
    }
}
