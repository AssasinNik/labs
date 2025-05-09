package com.cherenkov.lab_3.repository

import org.jooq.DSLContext
import org.springframework.stereotype.Repository
import com.cherenkov.generated.jooq.tables.Schedule.Companion.SCHEDULE
import com.cherenkov.generated.jooq.tables.Lecture.Companion.LECTURE
import com.cherenkov.generated.jooq.tables.Course.Companion.COURSE

@Repository
class ScheduleRepository(private val dsl: DSLContext) {

    /**
     * Найти ID лекций специальных дисциплин для группы
     * Специальными считаются лекции курсов, которые не связаны с кафедрой группы
     */
    fun findSpecialLectureIds(groupId: Long, deptId: Long): MutableList<Int?> {
        return dsl.selectDistinct(LECTURE.ID)
            .from(SCHEDULE)
            .join(LECTURE).on(SCHEDULE.ID_LECTURE.eq(LECTURE.ID))
            .join(COURSE).on(LECTURE.ID_COURSE.eq(COURSE.ID))
            .where(SCHEDULE.ID_GROUP.eq(groupId.toInt()))
            .and(COURSE.ID_DEPARTMENT.ne(deptId.toInt()))
            .fetch(LECTURE.ID)
    }
}