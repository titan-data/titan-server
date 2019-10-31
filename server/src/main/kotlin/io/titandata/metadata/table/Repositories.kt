package io.titandata.metadata.table

import org.jetbrains.exposed.sql.Table

/*
 * Repositories are the top level abstraction within the titan metadata. Everything is connected in some shape or form
 * to a repository, and repository names must be unique. There is no on-disk state associated with repositories
 * (only volumes and/or volumesets have such state), so these exist entirely within titan metadata.
 */
object Repositories : Table() {
    val name = varchar("name", 64).primaryKey()
    val metadata = varchar("metadata", 8192)
}
