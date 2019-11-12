package io.titandata.metadata.table

import io.titandata.metadata.table.Remotes.references
import io.titandata.models.ProgressEntry
import org.jetbrains.exposed.dao.IntIdTable
import org.jetbrains.exposed.sql.ReferenceOption

/**
 * Progress entries record information from ongoing operations. Each entry has a monotonically increasing ID number,
 * such that consumers can fetch any progress entries that have been added since the last time they checked.
 */
object ProgressEntries : IntIdTable("progress_entries") {
    val operation = uuid("volume_set").references(Operations.volumeSet, onDelete = ReferenceOption.CASCADE).primaryKey()
    val percent = integer("percent").nullable()
    val message = varchar("message", 4096).nullable()
    val type = enumerationByName("type", 16, ProgressEntry.Type::class)
}
