package com.cherenkov.lab_2.repository

import com.cherenkov.generated.jooq.tables.Lecture.Companion.LECTURE
import com.cherenkov.lab_2.entity.Lecture
import org.jooq.DSLContext
import org.springframework.stereotype.Repository

@Repository
class LectureRepository(private val dsl: DSLContext) {

    /**
     * Получение списка лекций по идентификатору курса
     */
    fun findByCourseId(courseId: Long): List<Lecture> {
        return dsl.select()
            .from(LECTURE)
            .where(LECTURE.ID_COURSE.eq(courseId.toInt()))
            .fetch { record ->
                record[LECTURE.ID]?.let {
                    record[LECTURE.NAME]?.let { it1 ->
                        record[LECTURE.DURATION_HOURS]?.let { it2 ->
                            record[LECTURE.TECH_EQUIPMENT]?.let { it3 ->
                                record[LECTURE.ID_COURSE]?.let { it4 ->
                                    Lecture(
                                        id = it.toLong(),
                                        name = it1,
                                        durationHours = it2,
                                        techEquipment = it3,
                                        courseId = it4.toLong(),
                                        elasticsearchId = record[LECTURE.ELASTICSEARCH_ID]
                                    )
                                }
                            }
                        }
                    }
                }
            }
    }
} 