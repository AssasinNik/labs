package com.cherenkov.lab_1.service

import com.cherenkov.generated.jooq.tables.Attendance.Companion.ATTENDANCE
import com.cherenkov.generated.jooq.tables.Department.Companion.DEPARTMENT
import com.cherenkov.generated.jooq.tables.Groups.Companion.GROUPS
import com.cherenkov.generated.jooq.tables.Institute.Companion.INSTITUTE
import com.cherenkov.generated.jooq.tables.Schedule.Companion.SCHEDULE
import com.cherenkov.generated.jooq.tables.Student.Companion.STUDENT
import com.cherenkov.generated.jooq.tables.University.Companion.UNIVERSITY
import com.cherenkov.lab_1.dto.LectureMaterial
import com.cherenkov.lab_1.dto.ReportRequest
import com.cherenkov.lab_1.dto.StudentReport
import org.springframework.data.elasticsearch.core.query.Query
import org.springframework.data.elasticsearch.core.query.Criteria
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.stereotype.Service
import org.jooq.DSLContext
import org.jooq.Log
import org.jooq.impl.DSL
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate
import org.springframework.data.elasticsearch.core.SearchHits
import org.springframework.data.elasticsearch.core.query.CriteriaQuery
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId

@Service
class ReportService(
    private val elasticsearchTemplate: ElasticsearchTemplate,
    private val dsl: DSLContext,
    private val redisOperations: RedisTemplate<String, Any>,
) {

    fun generateReport(request: ReportRequest): List<StudentReport> {
        val lectures = searchLecturesByTerm(request.term)
        val attendances = if (lectures.isNotEmpty()) findLowAttendanceStudents(lectures.map { it.lectureId }, request.startDate, request.endDate) else emptyList()
        val attendancesAndInfos = if (attendances.isNotEmpty()) getStudentsInfo(attendances) else emptyList()

        return attendancesAndInfos.map { (studentInfo, attendance) ->
            fetchRedisData(studentInfo.redisKey)?.let { redisData ->
                StudentReport(
                    studentNumber = studentInfo.studentNumber,
                    fullName = studentInfo.fullName,
                    email = studentInfo.email,
                    groupName = studentInfo.groupName,
                    departmentName = studentInfo.departmentName,
                    instituteName = studentInfo.instituteName,
                    universityName = studentInfo.universityName,
                    attendancePercentage = attendance.percentage,
                    reportPeriod = "${request.startDate} - ${request.endDate}",
                    searchTerm = request.term,
                    redisKey = studentInfo.redisKey,
                    groupNameFromRedis = redisData.groupName
                )
            }
        }.filterNotNull()
    }

    private fun searchLecturesByTerm(term: String): List<LectureMaterial> {
        val criteria = Criteria("description").matches(term)
        val query = CriteriaQuery(criteria)
        val answer = elasticsearchTemplate.search(query, LectureMaterial::class.java)
        val answer1 = answer.toList123()
        return answer1
    }

    private fun findLowAttendanceStudents(
        lectureIds: List<Long>,
        start: Instant,
        end: Instant
    ): List<StudentAttendance> {
        val a = ATTENDANCE.`as`("a")
        val s = SCHEDULE.`as`("s")

        val startDateTime = LocalDateTime.ofInstant(start, ZoneId.systemDefault())
        val endDateTime = LocalDateTime.ofInstant(end, ZoneId.systemDefault())

        val query = dsl.select(
            a.ID_STUDENT,
            DSL.count().`as`("total"),
            DSL.sum(DSL.`when`(a.STATUS, 1).otherwise(0)).`as`("attended"),
            DSL.sum(DSL.`when`(a.STATUS, 1).otherwise(0))
                .times(100.0)
                .divide(DSL.count())
                .`as`("percentage")
        )
            .from(a)
            .join(s).on(a.ID_SCHEDULE.eq(s.ID))
            .where(s.ID_LECTURE.`in`(lectureIds))
            .and(s.TIMESTAMP.between(startDateTime, endDateTime))
            .groupBy(a.ID_STUDENT)
            .orderBy(DSL.field("percentage").asc())
            .limit(10)

        val result = query.fetch()

        return result.map { record ->
            StudentAttendance(
                record[a.ID_STUDENT]!!,
                record["total", Long::class.java]!!,
                record["attended", Long::class.java]!!,
                record["percentage", Double::class.java]!!
            )
        }
    }

    private fun getStudentsInfo(attendances: List<StudentAttendance>): List<Pair<StudentInfo, StudentAttendance>> {
        val studentNumbers = attendances.map { it.studentNumber }

        val query = dsl.select(
            STUDENT.STUDENT_NUMBER,
            STUDENT.FULLNAME,
            STUDENT.EMAIL,
            GROUPS.NAME.`as`("group_name"),
            DEPARTMENT.NAME.`as`("department_name"),
            INSTITUTE.NAME.`as`("institute_name"),
            UNIVERSITY.NAME.`as`("university_name"),
            STUDENT.REDIS_KEY
        )
            .from(STUDENT)
            .join(GROUPS).on(STUDENT.ID_GROUP.eq(GROUPS.ID))
            .join(DEPARTMENT).on(GROUPS.ID_DEPARTMENT.eq(DEPARTMENT.ID))
            .join(INSTITUTE).on(DEPARTMENT.ID_INSTITUTE.eq(INSTITUTE.ID))
            .join(UNIVERSITY).on(INSTITUTE.ID_UNIVERSITY.eq(UNIVERSITY.ID))
            .where(STUDENT.STUDENT_NUMBER.`in`(studentNumbers))

        val result = query.fetch()

        return result.map { record ->
            StudentInfo(
                record[STUDENT.STUDENT_NUMBER]!!,
                record[STUDENT.FULLNAME]!!,
                record[STUDENT.EMAIL],
                record["group_name", String::class.java]!!,
                record["department_name", String::class.java]!!,
                record["institute_name", String::class.java]!!,
                record["university_name", String::class.java]!!,
                record[STUDENT.REDIS_KEY]
            ) to attendances.first { it.studentNumber == record[STUDENT.STUDENT_NUMBER]!! }
        }
    }

    private fun fetchRedisData(redisKey: String?): RedisData? {
        return redisKey?.let { key ->
            val dataMap = redisOperations.opsForHash<String, String>().entries(key)
            return if (dataMap.isNotEmpty()) {
                RedisData(dataMap["group_name"])
            } else {
                null
            }
        }
    }
}

data class StudentAttendance(
    val studentNumber: String,
    val total: Long,
    val attended: Long,
    val percentage: Double
)

data class StudentInfo(
    val studentNumber: String,
    val fullName: String,
    val email: String?,
    val groupName: String,
    val departmentName: String,
    val instituteName: String,
    val universityName: String,
    val redisKey: String?
)

data class RedisData(val groupName: String?)

fun SearchHits<LectureMaterial>.toList123(): List<LectureMaterial> {
    return this.searchHits.map { it.content }
}