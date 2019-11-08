package io.titandata.metadata.table

import io.titandata.metadata.MetadataProvider
import io.titandata.metadata.table.Remotes.references
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import org.jetbrains.exposed.dao.IntIdTable
import org.jetbrains.exposed.sql.ReferenceOption

/*
 * TODO comment
 */
object ProgressEntries : IntIdTable("progress_entries") {
    val operation = uuid("volume_set").references(Operations.volumeSet, onDelete = ReferenceOption.CASCADE).primaryKey()
    val percent = integer("percent").nullable()
    val message = varchar("message", 4096).nullable()
    val type = enumerationByName("type", 16, ProgressEntry.Type::class)
}
