package com.cherenkov.lab_3.repository

import com.cherenkov.generated.jooq.tables.references.GROUPS
import com.cherenkov.generated.jooq.tables.references.DEPARTMENT
import org.jooq.DSLContext
import org.springframework.stereotype.Repository
import java.util.*

@Repository
class GroupRepository(private val dsl: DSLContext) {
    
    fun findById(id: Long): Optional<com.cherenkov.generated.jooq.tables.records.GroupsRecord> {
        val result = dsl.select()
            .from(GROUPS)
            .join(DEPARTMENT)
            .on(GROUPS.ID_DEPARTMENT.eq(DEPARTMENT.ID))
            .where(GROUPS.ID.eq(id.toInt()))
            .fetchOneInto(com.cherenkov.generated.jooq.tables.records.GroupsRecord::class.java)
            
        return Optional.ofNullable(result)
    }
} 