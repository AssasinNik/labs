package com.cherenkov.lab_3.repository

import com.cherenkov.generated.jooq.tables.Attendance.Companion.ATTENDANCE
import com.cherenkov.generated.jooq.tables.Lecture.Companion.LECTURE
import com.cherenkov.generated.jooq.tables.Schedule.Companion.SCHEDULE
import com.cherenkov.generated.jooq.tables.Student.Companion.STUDENT
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.springframework.stereotype.Repository
import java.util.*

@Repository
class AttendanceRepository(private val dsl: DSLContext) {

    /**
     * Найти количество посещенных часов (для каждого студента) для лекций из списка
     * Каждая посещенная лекция = 2 академических часа
     */
    fun findAttendedHoursByGroupAndLectures(groupId: Long, lectureIds: MutableList<Int?>): List<Pair<String, Int>> {
        if (lectureIds.isEmpty()) return emptyList()

        return dsl.select(
            STUDENT.STUDENT_NUMBER,
            DSL.sum(
                DSL.`when`(ATTENDANCE.STATUS.eq(true), 2).otherwise(0)
            ).`as`("total_hours")
        )
            .from(ATTENDANCE)
            .join(SCHEDULE).on(ATTENDANCE.ID_SCHEDULE.eq(SCHEDULE.ID))
            .join(STUDENT).on(ATTENDANCE.ID_STUDENT.eq(STUDENT.STUDENT_NUMBER))
            .join(LECTURE).on(SCHEDULE.ID_LECTURE.eq(LECTURE.ID))
            .where(SCHEDULE.ID_GROUP.eq(groupId.toInt()))
            .and(LECTURE.ID.`in`(lectureIds))
            .groupBy(STUDENT.STUDENT_NUMBER)
            .fetch { record ->
                Pair(
                    record.get(STUDENT.STUDENT_NUMBER)!!,
                    record.get("total_hours", Int::class.java) ?: 0
                )
            }
    }
    
    /**
     * Преобразование номера студента в Long
     * Обрабатывает особые случаи, когда номер начинается с буквы S
     */
    private fun parseStudentNumber(studentNumber: String): Long {
        return if (studentNumber.startsWith("S")) {
            // Генерируем хеш-код для строковых идентификаторов
            studentNumber.hashCode().toLong() and Long.MAX_VALUE
        } else {
            studentNumber.toLong()
        }
    }
}