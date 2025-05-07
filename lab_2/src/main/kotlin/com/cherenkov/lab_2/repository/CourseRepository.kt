package com.cherenkov.lab_2.repository

import com.cherenkov.generated.jooq.tables.Course.Companion.COURSE
import com.cherenkov.lab_2.entity.Course
import org.jooq.DSLContext
import org.springframework.stereotype.Repository

@Repository
class CourseRepository(private val dsl: DSLContext) {

    /**
     * Получение курса по идентификатору
     */
    fun findById(id: Long): Course? {
        return dsl.select()
            .from(COURSE)
            .where(COURSE.ID.eq(id.toInt()))
            .fetchOne { record ->
                record[COURSE.NAME]?.let {
                    record[COURSE.ID]?.let { it1 ->
                        record[COURSE.ID_DEPARTMENT]?.let { it2 ->
                            Course(
                                id = it1.toLong(),
                                name = it,
                                departmentId = it2.toLong()
                            )
                        }
                    }
                }
            }
    }
} 