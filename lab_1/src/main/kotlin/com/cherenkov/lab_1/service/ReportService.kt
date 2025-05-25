package com.cherenkov.lab_1.service
import com.cherenkov.generated.jooq.tables.Attendance.Companion.ATTENDANCE
import com.cherenkov.generated.jooq.tables.Schedule.Companion.SCHEDULE
import com.cherenkov.lab_1.dto.*
import com.cherenkov.lab_1.exceptions.*
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.impl.DSL
import org.neo4j.driver.Driver
import org.neo4j.driver.Session
import org.neo4j.driver.Values
import org.slf4j.LoggerFactory
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate
import org.springframework.data.elasticsearch.core.query.Criteria
import org.springframework.data.elasticsearch.core.query.CriteriaQuery
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.stream.Collectors
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue

@Service
@Transactional(readOnly = true, transactionManager = "transactionManager")
class ReportService(
    private val elasticsearchTemplate: ElasticsearchTemplate,
    private val dsl: DSLContext,
    private val redisTemplate: RedisTemplate<String, String>,
    private val neo4jDriver: Driver
) {
    private val logger = LoggerFactory.getLogger(ReportService::class.java)

    @Transactional(transactionManager = "transactionManager")
    fun generateReport(request: ReportRequest): ReportResult {
        logger.info("Запущен старый метод generateReport с параметрами: term={}, startDate={}, endDate={}", 
                  request.term, request.startDate, request.endDate)
        
        try {
            val attendanceData = generateAttendanceReport(request.term, request.startDate, request.endDate)
            
            // Преобразование из нового формата в старый
            val reportData = attendanceData.map { dto -> 
                StudentReport(
                    studentNumber = dto.studentNumber,
                    fullName = dto.fullName,
                    email = dto.email,
                    groupName = dto.groupName,
                    attendancePercentage = dto.attendancePercent,
                    reportPeriod = "${dto.periodStart} - ${dto.periodEnd}",
                    searchTerm = dto.searchTerm,
                )
            }
            
            logger.info("Данные успешно преобразованы в старый формат, количество записей: {}", reportData.size)
            
            return ReportResult(
                status = if (reportData.isEmpty()) "WARNING" else "SUCCESS",
                message = if (reportData.isEmpty()) "Не найдены данные для указанных параметров" else "Отчет успешно сформирован",
                data = reportData
            )
        } catch (e: Exception) {
            logger.error("Ошибка при выполнении запроса: {}", e.message, e)
            return ReportResult(
                status = "ERROR",
                message = "Ошибка при формировании отчета: ${e.message}",
                data = emptyList()
            )
        }
    }

    fun generateAttendanceReport(term: String, from: Instant, to: Instant): List<FullStudentAttendanceDTO> {
        logger.info("Начало генерации отчета: term={}, from={}, to={}", term, from, to)
        
        // Преобразуем Instant в LocalDateTime для работы с Neo4j
        val fromDateTime = LocalDateTime.ofInstant(from, ZoneId.systemDefault())
        val toDateTime = LocalDateTime.ofInstant(to, ZoneId.systemDefault()) // Исправлено: было from, должно быть to
        
        // Поиск лекций по термину
        val lectureIds = findLectureIdsByTerm(term)
        if (lectureIds.isEmpty()) {
            logger.warn("Лекции не найдены для термина: {}", term)
            return emptyList()
        }
        
        // Получение данных об ожидаемой посещаемости из Neo4j
        val expectedAttendance = getExpectedAttendanceFromNeo4j(lectureIds, fromDateTime, toDateTime)
        if (expectedAttendance.isEmpty()) {
            logger.warn("Не найдены данные об ожидаемой посещаемости")
            return emptyList()
        }
        
        // Получение данных о фактической посещаемости из PostgreSQL
        val actualAttendance = getActualAttendanceFromPostgres(
            lectureIds,
            fromDateTime,
            toDateTime,
            ArrayList(expectedAttendance.keys)
        )
        
        // Обработка данных и формирование отчета
        return processAttendanceData(expectedAttendance, actualAttendance, fromDateTime, toDateTime, term)
    }
    private fun findLectureIdsByTerm(term: String): List<Long> {
        logger.debug("Поиск ID лекций по термину: {}", term)
        try {
            // Используем ElasticsearchTemplate для поиска лекций по описанию
            val criteria = Criteria("description").matches(term)
            val query = CriteriaQuery(criteria)
            
            val searchHits = elasticsearchTemplate.search(query, LectureMaterial::class.java)
            
            val lectureIds = searchHits.searchHits.map { it.content.lectureId }
            logger.info("Найдено {} лекций по термину '{}'", lectureIds.size, term)
            
            return lectureIds
        } catch (e: Exception) {
            logger.error("Ошибка при поиске лекций: {}", e.message, e)
            throw ElasticsearchAccessException("Ошибка при поиске лекций: ${e.message}", e)
        }
    }
    private fun getExpectedAttendanceFromNeo4j(
        lectureIds: List<Long>, 
        from: LocalDateTime, 
        to: LocalDateTime
    ): Map<String, Int> {
        logger.debug("Получение данных об ожидаемой посещаемости из Neo4j")
        
        val cypherQuery = """
            MATCH (l:Lecture)<-[hs:HAS_SCHEDULE]-(g:Group)<-[:BELONGS_TO]-(s:Student)
            WHERE l.id IN ${'$'}lectureIds
              AND hs.date >= datetime(${'$'}from)
              AND hs.date <= datetime(${'$'}to)
            RETURN s.student_number AS studentNumber, COUNT(hs) AS expectedCount
        """
        
        val fromStr = from.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        val toStr = to.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        
        try {
            neo4jDriver.session().use { session ->
                return session.readTransaction { tx ->
                    val result = tx.run(
                        cypherQuery, 
                        Values.parameters(
                            "lectureIds", lectureIds,
                            "from", fromStr,
                            "to", toStr
                        )
                    )
                    val attendanceMap = HashMap<String, Int>()
                    
                    result.list().forEach { record ->
                        attendanceMap[record.get("studentNumber").asString()] = record.get("expectedCount").asInt()
                    }
                    
                    logger.info("Получены данные об ожидаемой посещаемости для {} студентов", attendanceMap.size)
                    attendanceMap
                }
            }
        } catch (e: Exception) {
            logger.error("Ошибка при получении данных из Neo4j: {}", e.message, e)
            throw Exception("Ошибка при получении данных из Neo4j: ${e.message}")
        }
    }

    private fun getActualAttendanceFromPostgres(
        lectureIds: List<Long>,
        from: LocalDateTime,
        to: LocalDateTime,
        studentNumbers: List<String>
    ): Map<String, Int> {
        logger.debug("Получение данных о фактической посещаемости из PostgreSQL")
        
        try {
            val a = ATTENDANCE.`as`("a")
            val s = SCHEDULE.`as`("s")
            
            val result = dsl.select(
                a.ID_STUDENT,
                DSL.count().filterWhere(a.STATUS.eq(true)).`as`("actual_count")
            )
                .from(a)
                .join(s).on(a.ID_SCHEDULE.eq(s.ID))
                .where(s.ID_LECTURE.`in`(lectureIds))
                .and(s.TIMESTAMP.between(from, to))
                .and(a.ID_STUDENT.`in`(studentNumbers))
                .groupBy(a.ID_STUDENT)
                .fetch()
            
            val attendanceMap = HashMap<String, Int>()
            
            for (record in result) {
                val studentId = record.get(a.ID_STUDENT)
                val count = record.get("actual_count", Int::class.java)
                if (studentId != null && count != null) {
                    attendanceMap[studentId] = count
                }
            }
            
            logger.info("Получены данные о фактической посещаемости для {} студентов", attendanceMap.size)
            return attendanceMap
        } catch (e: Exception) {
            logger.error("Ошибка при получении данных из PostgreSQL: {}", e.message, e)
            throw DatabaseAccessException("Ошибка при получении данных о фактической посещаемости: ${e.message}", e)
        }
    }
    private fun getStudentInfoFromRedis(studentNumber: String): RedisStudentInfo {
        logger.debug("Получение информации о студенте из Redis: {}", studentNumber)
        
        val key = "student:$studentNumber"
        try {
            val jsonValue = redisTemplate.opsForValue().get(key)
            
            if (jsonValue.isNullOrEmpty()) {
                logger.warn("Информация о студенте не найдена в Redis: {}", key)
                throw RuntimeException("Информация о студенте не найдена в Redis: $key")
            }
            
            // Десериализация JSON в объект RedisStudentInfo
            val objectMapper = ObjectMapper()
            return objectMapper.readValue(jsonValue)
            
        } catch (e: Exception) {
            logger.error("Ошибка при получении данных из Redis: {}", e.message, e)
            throw RedisAccessException("Ошибка при получении информации о студенте из Redis: ${e.message}", e)
        }
    }
    private fun processAttendanceData(
        expected: Map<String, Int>,
        actual: Map<String, Int>,
        from: LocalDateTime,
        to: LocalDateTime,
        term: String
    ): List<FullStudentAttendanceDTO> {
        logger.debug("Обработка данных о посещаемости для {} студентов", expected.size)
        
        val results = expected.entries
            .map { entry -> calculateAttendancePercent(entry, actual) }
            .sortedBy { it.percent }
            .take(10)
            .mapNotNull { dto -> buildFullDTO(dto, from, to, term) }
        
        logger.info("Сформирован отчет с {} записями", results.size)
        return results
    }
    private fun calculateAttendancePercent(
        entry: Map.Entry<String, Int>,
        actual: Map<String, Int>
    ): StudentAttendancePercentDTO {
        val studentNumber = entry.key
        val expectedCount = entry.value
        val actualCount = actual.getOrDefault(studentNumber, 0)
        
        val percent = if (expectedCount > 0) {
            (actualCount * 100.0) / expectedCount
        } else {
            0.0
        }
        
        return StudentAttendancePercentDTO(studentNumber, percent)
    }

    private fun buildFullDTO(
        dto: StudentAttendancePercentDTO,
        from: LocalDateTime,
        to: LocalDateTime,
        term: String
    ): FullStudentAttendanceDTO? {
        try {
            val info = getStudentInfoFromRedis(dto.studentNumber)
            
            return FullStudentAttendanceDTO(
                studentNumber = dto.studentNumber,
                fullName = info.fullname,
                email = info.email,
                groupName = info.groupName,
                attendancePercent = dto.percent,
                periodStart = from,
                periodEnd = to,
                searchTerm = term
            )
        } catch (e: Exception) {
            logger.warn("Не удалось получить информацию о студенте {}: {}", dto.studentNumber, e.message)
            return null
        }
    }
}




