package io.titandata.metadata.table

import org.jetbrains.exposed.sql.ReferenceOption
import org.jetbrains.exposed.sql.Table

object Remotes : Table() {
    val name = varchar("name", 64).primaryKey()
    val repo = varchar("repositories", 64).references(Repositories.name, onDelete = ReferenceOption.CASCADE).primaryKey()
    val metadata = varchar("metadata", 8192)
}
