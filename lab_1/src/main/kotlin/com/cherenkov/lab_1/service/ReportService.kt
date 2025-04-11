package com.cherenkov.lab_1.service

import com.cherenkov.generated.jooq.tables.Attendance.Companion.ATTENDANCE
import com.cherenkov.generated.jooq.tables.Department.Companion.DEPARTMENT
import com.cherenkov.generated.jooq.tables.Groups.Companion.GROUPS
import com.cherenkov.generated.jooq.tables.Institute.Companion.INSTITUTE
import com.cherenkov.generated.jooq.tables.Lecture.Companion.LECTURE
import com.cherenkov.generated.jooq.tables.LectureMaterials.Companion.LECTURE_MATERIALS
import com.cherenkov.generated.jooq.tables.Course.Companion.COURSE
import com.cherenkov.generated.jooq.tables.Schedule.Companion.SCHEDULE
import com.cherenkov.generated.jooq.tables.Student.Companion.STUDENT
import com.cherenkov.generated.jooq.tables.University.Companion.UNIVERSITY
import com.cherenkov.lab_1.dto.LectureMaterial
import com.cherenkov.lab_1.dto.ReportRequest
import com.cherenkov.lab_1.dto.StudentReport
import org.bson.Document
import org.springframework.cache.annotation.Cacheable
import org.springframework.data.elasticsearch.core.query.Criteria
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.stereotype.Service
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.slf4j.LoggerFactory
import org.springframework.cache.annotation.EnableCaching
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate
import org.springframework.data.elasticsearch.core.SearchHits
import org.springframework.data.elasticsearch.core.query.CriteriaQuery
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Query
import org.springframework.security.core.context.SecurityContextHolder
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId

@Service
@EnableCaching
class ReportService(
    private val elasticsearchTemplate: ElasticsearchTemplate,
    private val dsl: DSLContext,
    private val redisOperations: RedisTemplate<String, Any>,
    private val mongoTemplate: MongoTemplate
) {
    private val logger = LoggerFactory.getLogger(ReportService::class.java)

    /**
     * Генерирует отчет о студентах с минимальным процентом посещения лекций,
     * содержащих заданный термин, за определенный период обучения
     */
    @Cacheable(value = ["attendance_reports"], key = "#request.term + '_' + #request.startDate + '_' + #request.endDate")
    fun generateReport(request: ReportRequest): List<StudentReport> {
        logger.info("Генерация отчета о посещаемости: термин='${request.term}', период=${request.startDate} - ${request.endDate}")
        
        // Получаем текущего пользователя для логирования
        val userId = SecurityContextHolder.getContext().authentication?.principal?.toString() ?: "anonymous"
        logger.info("Пользователь $userId запросил отчет о посещаемости")

        // Поиск лекций, содержащих заданный термин
        val lectures = searchLecturesByTerm(request.term)
        
        if (lectures.isEmpty()) {
            logger.warn("Лекции с термином '${request.term}' не найдены")
            return emptyList()
        }
        logger.info("Найдено ${lectures.size} лекций с термином '${request.term}'")

        // Поиск студентов с низкой посещаемостью этих лекций
        val lectureIds = lectures.map { it.lectureId }
        val studentsWithAttendance = findLowAttendanceStudents(lectureIds, request.startDate, request.endDate)
        
        if (studentsWithAttendance.isEmpty()) {
            logger.warn("Студенты с низкой посещаемостью для выбранных лекций не найдены")
            return emptyList()
        }
        logger.info("Найдено ${studentsWithAttendance.size} студентов с низкой посещаемостью")

        // Получение полной информации о студентах
        val studentsInfo = getStudentsInfo(studentsWithAttendance)
        logger.info("Получена информация для ${studentsInfo.size} студентов")
        
        // Формирование отчета для каждого студента
        return studentsInfo.map { (studentInfo, attendance) ->
            val redisData = fetchRedisData(studentInfo.redisKey)
            val mongoData = fetchMongoDataForStudent(studentInfo) 
            
            StudentReport(
                studentNumber = studentInfo.studentNumber,
                fullName = studentInfo.fullName,
                email = studentInfo.email,
                groupName = studentInfo.groupName,
                departmentName = studentInfo.departmentName,
                instituteName = studentInfo.instituteName,
                universityName = studentInfo.universityName,
                attendancePercentage = attendance.percentage,
                attendedLectures = attendance.attended,
                totalLectures = attendance.total,
                reportPeriod = "${request.startDate} - ${request.endDate}",
                searchTerm = request.term,
                redisKey = studentInfo.redisKey,
                groupNameFromRedis = redisData?.groupName,
                mongoData = mongoData
            )
        }
    }

    /**
     * Поиск лекций, содержащих указанный термин в описании материалов
     */
    private fun searchLecturesByTerm(term: String): List<LectureMaterial> {
        logger.debug("Поиск лекций с термином: $term")
        
        // Сначала пробуем искать в Elasticsearch
        val criteria = Criteria("description").contains(term)
        val query = CriteriaQuery(criteria)
        val searchResults = elasticsearchTemplate.search(query, LectureMaterial::class.java)
        val lectures = searchResults.searchHits.map { it.content }
        
        // Если в ES ничего не нашли, можем попробовать поискать в PostgreSQL
        if (lectures.isEmpty()) {
            logger.debug("В Elasticsearch не найдены лекции с термином '$term', ищем в PostgreSQL")
            
            // Поиск в PostgreSQL через материалы лекций
            val lm = LECTURE_MATERIALS.`as`("lm")
            val l = LECTURE.`as`("l")
            
            val pgResults = dsl.select(
                l.ID,
                l.NAME,
                lm.DESCRIPTION
            )
                .from(lm)
                .join(l).on(lm.ID_LECTURE.eq(l.ID))
                .where(lm.DESCRIPTION.containsIgnoreCase(term))
                .fetch()
                
            return pgResults.map { record ->
                LectureMaterial(
                    id = record[l.ID].toString(),
                    lectureId = record[l.ID],
                    name = record[l.NAME],
                    description = record[lm.DESCRIPTION] ?: ""
                )
            }
        }
        
        logger.debug("Найдено ${lectures.size} лекций с термином '$term'")
        return lectures
    }

    /**
     * Поиск студентов с минимальным процентом посещения указанных лекций за период
     */
    private fun findLowAttendanceStudents(
        lectureIds: List<Long>,
        start: Instant,
        end: Instant
    ): List<StudentAttendance> {
        logger.debug("Поиск студентов с низкой посещаемостью лекций: ${lectureIds.joinToString()}")
        
        val a = ATTENDANCE.`as`("a")
        val s = SCHEDULE.`as`("s")
        val l = LECTURE.`as`("l")
        val c = COURSE.`as`("c")

        val startDateTime = LocalDateTime.ofInstant(start, ZoneId.systemDefault())
        val endDateTime = LocalDateTime.ofInstant(end, ZoneId.systemDefault())

        // Запрос для получения студентов с минимальным процентом посещения лекций
        val query = dsl.select(
            a.ID_STUDENT,
            DSL.count().`as`("total_lectures"),
            DSL.sum(DSL.`when`(a.STATUS, 1).otherwise(0)).`as`("attended_lectures"),
            DSL.sum(DSL.`when`(a.STATUS, 1).otherwise(0))
                .times(100.0)
                .divide(DSL.greatest(DSL.count(), 1))
                .`as`("attendance_percentage")
        )
            .from(a)
            .join(s).on(a.ID_SCHEDULE.eq(s.ID))
            .join(l).on(s.ID_LECTURE.eq(l.ID))
            .join(c).on(l.ID_COURSE.eq(c.ID))
            .where(l.ID.`in`(lectureIds))
            .and(s.TIMESTAMP.between(startDateTime, endDateTime))
            .groupBy(a.ID_STUDENT)
            .orderBy(DSL.field("attendance_percentage").asc())
            .limit(10)

        val result = query.fetch()
        logger.debug("Найдено ${result.size} студентов с низкой посещаемостью")

        return result.map { record ->
            StudentAttendance(
                studentNumber = record[a.ID_STUDENT]!!,
                total = record["total_lectures", Long::class.java]!!,
                attended = record["attended_lectures", Long::class.java]!!,
                percentage = record["attendance_percentage", Double::class.java]!!
            )
        }
    }

    /**
     * Получение полной информации о студентах
     */
    private fun getStudentsInfo(attendances: List<StudentAttendance>): List<Pair<StudentInfo, StudentAttendance>> {
        logger.debug("Получение информации о студентах: ${attendances.size} записей")
        
        val studentNumbers = attendances.map { it.studentNumber }

        // Запрос для получения полной информации о студентах
        val query = dsl.select(
            STUDENT.STUDENT_NUMBER,
            STUDENT.FULLNAME,
            STUDENT.EMAIL,
            GROUPS.NAME.`as`("group_name"),
            GROUPS.MONGO_ID,
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
        logger.debug("Получена информация для ${result.size} студентов")

        return result.map { record ->
            StudentInfo(
                studentNumber = record[STUDENT.STUDENT_NUMBER]!!,
                fullName = record[STUDENT.FULLNAME]!!,
                email = record[STUDENT.EMAIL],
                groupName = record["group_name", String::class.java]!!,
                departmentName = record["department_name", String::class.java]!!,
                instituteName = record["institute_name", String::class.java]!!,
                universityName = record["university_name", String::class.java]!!,
                redisKey = record[STUDENT.REDIS_KEY],
                mongoId = record[GROUPS.MONGO_ID]
            ) to attendances.first { it.studentNumber == record[STUDENT.STUDENT_NUMBER]!! }
        }
    }

    /**
     * Получение данных студента из Redis
     */
    private fun fetchRedisData(redisKey: String?): RedisData? {
        if (redisKey == null) return null
        
        logger.debug("Получение данных из Redis для ключа: $redisKey")
        return try {
            val dataMap = redisOperations.opsForHash<String, String>().entries(redisKey)
            if (dataMap.isNotEmpty()) {
                RedisData(dataMap["group_name"])
            } else {
                logger.warn("Данные для ключа $redisKey не найдены в Redis")
                null
            }
        } catch (e: Exception) {
            logger.error("Ошибка при получении данных из Redis: ${e.message}")
            null
        }
    }
    
    /**
     * Получение данных о студенте из MongoDB
     */
    private fun fetchMongoDataForStudent(studentInfo: StudentInfo): Map<String, Any>? {
        if (studentInfo.mongoId.isNullOrEmpty()) {
            logger.debug("MongoDB ID для группы ${studentInfo.groupName} не найден")
            return null
        }
        
        logger.debug("Получение данных из MongoDB для группы ${studentInfo.groupName}")
        return try {
            // Ищем группу в MongoDB используя ID группы
            val collection = mongoTemplate.getCollection("universities")
            
            // Агрегированный запрос для поиска группы во вложенных документах
            val pipeline = listOf(
                Document("\$match", 
                    Document("institutes.departments.id", studentInfo.departmentName)),
                Document("\$project", 
                    Document("name", 1)
                        .append("institutes", 
                            Document("\$filter", 
                                Document("input", "\$institutes")
                                    .append("as", "institute")
                                    .append("cond", 
                                        Document("\$in", listOf(studentInfo.departmentName, "\$\$institute.departments.id"))
                                    )
                            )
                        )
                )
            )
            
            val result = collection.aggregate(pipeline).first()
            result?.toMap()
        } catch (e: Exception) {
            logger.error("Ошибка при получении данных из MongoDB: ${e.message}")
            null
        }
    }
}

/**
 * Класс, представляющий информацию о посещаемости студента
 */
data class StudentAttendance(
    val studentNumber: String,
    val total: Long,
    val attended: Long,
    val percentage: Double
)

/**
 * Класс, представляющий основную информацию о студенте
 */
data class StudentInfo(
    val studentNumber: String,
    val fullName: String,
    val email: String?,
    val groupName: String,
    val departmentName: String,
    val instituteName: String,
    val universityName: String,
    val redisKey: String?,
    val mongoId: String? = null
)

/**
 * Класс, представляющий данные из Redis
 */
data class RedisData(val groupName: String?)

/**
 * Расширение для конвертации результатов поиска в список
 */
fun SearchHits<LectureMaterial>.toList123(): List<LectureMaterial> {
    return this.searchHits.map { it.content }
}