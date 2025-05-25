package com.cherenkov.lab_2.service

import com.cherenkov.lab_2.dto.LectureReportDTO
import com.cherenkov.lab_2.entity.Course
import com.cherenkov.lab_2.entity.Lecture
import com.cherenkov.lab_2.entity.UniversityDocument
import com.cherenkov.lab_2.exceptions.MongoDbException
import com.cherenkov.lab_2.exceptions.Neo4jException
import com.cherenkov.lab_2.exceptions.PostgresException
import com.cherenkov.lab_2.exceptions.ResourceNotFoundException
import com.cherenkov.lab_2.repository.CourseRepository
import com.cherenkov.lab_2.repository.LectureRepository
import com.mongodb.MongoException
import org.neo4j.driver.Driver
import org.neo4j.driver.exceptions.Neo4jException as Neo4jDriverException
import org.neo4j.driver.Values
import org.slf4j.LoggerFactory
import org.springframework.dao.DataAccessException
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.stereotype.Service
import java.util.AbstractMap
import java.util.HashSet
import java.util.UUID

@Service
class ReportService(
    private val courseRepository: CourseRepository,
    private val lectureRepository: LectureRepository,
    private val neo4jDriver: Driver,
    private val mongoTemplate: MongoTemplate
) {
    private val logger = LoggerFactory.getLogger(ReportService::class.java)

    /**
     * Генерация отчета о необходимом объеме аудитории для проведения занятий
     * по курсу заданного семестра и года обучения
     */
    fun generateReport(courseId: Long, semester: Int, studyYear: Int): List<LectureReportDTO> {
        val requestId = generateRequestId()
        logger.info("[{}] Запрос на генерацию отчета: courseId={}, semester={}, studyYear={}", 
            requestId, courseId, semester, studyYear)
        
        validateParameters(courseId, semester, studyYear)
        
        // Получаем курс по ID
        val course = getCourseById(courseId, requestId)
            ?: throw ResourceNotFoundException(
                resourceType = "Course", 
                resourceId = courseId,
                details = mapOf(
                    "requestId" to requestId,
                    "courseId" to courseId
                )
            )
        
        logger.debug("[{}] Курс загружен: id={}, name='{}', departmentId={}", 
            requestId, course.id, course.name, course.departmentId)

        // Получаем все лекции по ID курса
        val allLectures = getLecturesByCourseId(courseId, requestId)
        logger.debug("[{}] Загружено {} лекций для курса '{}'", 
            requestId, allLectures.size, course.name)
        
        if (allLectures.isEmpty()) {
            logger.warn("[{}] Не найдены лекции для курса с ID={}", requestId, courseId)
            return emptyList()
        }

        // Получаем ID лекций, запланированных в указанном семестре и году
        val scheduledIds = getScheduledLectureIds(semester, studyYear, requestId)
        logger.debug("[{}] Найдено {} лекций, запланированных в семестре {} года {}", 
            requestId, scheduledIds.size, semester, studyYear)
        
        if (scheduledIds.isEmpty()) {
            logger.warn("[{}] Не найдены запланированные лекции для семестра {} года {}", 
                requestId, semester, studyYear)
            return emptyList()
        }

        // Получаем информацию об университете по ID кафедры
        val uniDoc = getUniversityByDepartmentId(course.departmentId, requestId)
        if (uniDoc == null) {
            logger.warn("[{}] Документ университета не найден для departmentId={}", 
                requestId, course.departmentId)
        } else {
            logger.debug("[{}] Получен документ университета: id={}, name='{}'", 
                requestId, uniDoc.id, uniDoc.name)
        }

        // Находим информацию об институте и кафедре
        val departmentInfo = uniDoc?.institutes?.flatMap { institute ->
            institute.departments
                .filter { it.id == course.departmentId.toInt() }
                .map { AbstractMap.SimpleEntry(institute.name, it.name) }
        }?.firstOrNull()

        val instName = departmentInfo?.key ?: "(не найдено)"
        val deptName = departmentInfo?.value ?: "(не найдено)"
        logger.debug("[{}] Определен институт='{}', кафедра='{}'", 
            requestId, instName, deptName)

        // Формируем отчет
        val filteredLectures = allLectures.filter { scheduledIds.contains(it.id) }
        logger.debug("[{}] Отфильтровано {} лекций для отчета", 
            requestId, filteredLectures.size)
        
        if (filteredLectures.isEmpty()) {
            logger.warn("[{}] Не найдены запланированные лекции курса {} в семестре {} года {}", 
                requestId, courseId, semester, studyYear)
            return emptyList()
        }
        
        val report = filteredLectures.map { lecture ->
            logger.debug("[{}] Обработка лекции id={} name='{}'", 
                requestId, lecture.id, lecture.name)
            
            // Получаем количество студентов для каждой лекции
            val studentCount = getStudentCount(lecture.id, semester, studyYear, requestId)
            logger.debug("[{}] Лекция id={} имеет studentCount={}", 
                requestId, lecture.id, studentCount)
            
            LectureReportDTO(
                courseName = course.name,
                lectureId = lecture.id,
                lectureName = lecture.name,
                techEquipment = lecture.techEquipment,
                studentCount = studentCount,
                universityName = uniDoc?.name ?: "(не найдено)",
                instituteName = instName,
                departmentName = deptName,
                semester = semester,
                studyYear = studyYear
            )
        }

        logger.info("[{}] Отчет успешно сформирован: {} записей", 
            requestId, report.size)
        return report
    }

    /**
     * Валидация входных параметров
     */
    private fun validateParameters(courseId: Long, semester: Int, studyYear: Int) {
        val currentYear = java.time.Year.now().value
        
        if (courseId <= 0) {
            throw IllegalArgumentException("Некорректный ID курса: $courseId. ID должен быть положительным числом.")
        }
        
        if (semester !in 1..2) {
            throw IllegalArgumentException("Некорректный номер семестра: $semester. Допустимые значения: 1 или 2.")
        }
        
        if (studyYear < 2000 || studyYear > currentYear + 1) {
            throw IllegalArgumentException(
                "Некорректный год обучения: $studyYear. " +
                "Год должен быть в диапазоне 2000 - ${currentYear + 1}."
            )
        }
    }

    /**
     * Получение курса по ID с обработкой ошибок
     */
    private fun getCourseById(courseId: Long, requestId: String): Course? {
        try {
            logger.debug("[{}] Запрос курса из базы данных: courseId={}", 
                requestId, courseId)
            return courseRepository.findById(courseId)
        } catch (e: DataAccessException) {
            logger.error("[{}] Ошибка при получении курса из Postgres: {}", 
                requestId, e.message, e)
            throw PostgresException(
                message = "Ошибка при получении данных о курсе с ID $courseId",
                details = mapOf(
                    "requestId" to requestId,
                    "courseId" to courseId,
                    "errorType" to e.javaClass.simpleName
                ),
                cause = e
            )
        } catch (e: Exception) {
            logger.error("[{}] Необработанная ошибка при получении курса: {}", 
                requestId, e.message, e)
            throw e
        }
    }

    /**
     * Получение лекций по ID курса с обработкой ошибок
     */
    private fun getLecturesByCourseId(courseId: Long, requestId: String): List<Lecture> {
        try {
            logger.debug("[{}] Запрос лекций из базы данных: courseId={}", 
                requestId, courseId)
            return lectureRepository.findByCourseId(courseId)
        } catch (e: DataAccessException) {
            logger.error("[{}] Ошибка при получении лекций из Postgres: {}", 
                requestId, e.message, e)
            throw PostgresException(
                message = "Ошибка при получении лекций для курса с ID $courseId",
                details = mapOf(
                    "requestId" to requestId,
                    "courseId" to courseId,
                    "errorType" to e.javaClass.simpleName
                ),
                cause = e
            )
        } catch (e: Exception) {
            logger.error("[{}] Необработанная ошибка при получении лекций: {}", 
                requestId, e.message, e)
            throw e
        }
    }

    /**
     * Получение ID запланированных лекций для указанного семестра и года
     * с обработкой ошибок Neo4j
     */
    private fun getScheduledLectureIds(semester: Int, studyYear: Int, requestId: String): Set<Long> {
        logger.debug("[{}] Запрос ID лекций из Neo4j: semester={}, studyYear={}", 
            requestId, semester, studyYear)

        val cypher = """
            MATCH (g:Group)-[hs:HAS_SCHEDULE]->(l:Lecture)
            WHERE hs.date.year = ${'$'}year AND (
              (${'$'}sem = 1 AND hs.date.month >= 9 AND hs.date.month <= 12) OR
              (${'$'}sem = 2 AND hs.date.month >= 1 AND hs.date.month <= 6)
            )
            RETURN DISTINCT l.id AS id
        """

        val ids = HashSet<Long>()
        try {
            neo4jDriver.session().use { session ->
                val params = Values.parameters("year", studyYear, "sem", semester)
                logger.debug("[{}] Выполнение Neo4j запроса: {}, params: {}", 
                    requestId, cypher.replace("\n", " "), params)
                
                val result = session.run(cypher, params)
                while (result.hasNext()) {
                    ids.add(result.next().get("id").asLong())
                }
                
                logger.debug("[{}] Получены ID лекций из Neo4j: {}", requestId, ids)
            }
        } catch (e: Neo4jDriverException) {
            logger.error("[{}] Ошибка Neo4j при получении ID лекций: {}", 
                requestId, e.message, e)
            throw Neo4jException(
                message = "Ошибка при получении данных из Neo4j о запланированных лекциях",
                details = mapOf(
                    "requestId" to requestId,
                    "semester" to semester,
                    "studyYear" to studyYear,
                    "query" to cypher.replace("\n", " ")
                ),
                cause = e
            )
        } catch (e: Exception) {
            logger.error("[{}] Необработанная ошибка при получении ID лекций: {}", 
                requestId, e.message, e)
            throw e
        }

        return ids
    }

    /**
     * Получение количества студентов для указанной лекции в семестре и году
     * с обработкой ошибок
     */
    private fun getStudentCount(lectureId: Long, semester: Int, studyYear: Int, requestId: String): Long {
        logger.debug("[{}] Запрос количества студентов из Neo4j: lectureId={}, semester={}, studyYear={}", 
            requestId, lectureId, semester, studyYear)

        val cypher = """
            MATCH (l:Lecture {id: ${'$'}lid})<-[hs:HAS_SCHEDULE]-(g:Group)<-[:BELONGS_TO]-(s:Student)
            WHERE hs.date.year = ${'$'}year AND (
              (${'$'}sem = 1 AND hs.date.month >= 9 AND hs.date.month <= 12) OR
              (${'$'}sem = 2 AND hs.date.month >= 1 AND hs.date.month <= 6)
            )
            RETURN count(DISTINCT s) AS cnt
        """

        try {
            neo4jDriver.session().use { session ->
                val params = Values.parameters(
                    "lid", lectureId,
                    "year", studyYear,
                    "sem", semester
                )
                
                logger.debug("[{}] Выполнение Neo4j запроса: {}, params: {}", 
                    requestId, cypher.replace("\n", " "), params)
                
                val result = session.run(cypher, params)
                if (result.hasNext()) {
                    val count = result.next().get("cnt").asLong()
                    logger.debug("[{}] Получено количество студентов для лекции {}: {}", 
                        requestId, lectureId, count)
                    return count
                }
            }
        } catch (e: Neo4jDriverException) {
            logger.error("[{}] Ошибка Neo4j при получении количества студентов: {}", 
                requestId, e.message, e)
            throw Neo4jException(
                message = "Ошибка при получении данных из Neo4j о количестве студентов",
                details = mapOf(
                    "requestId" to requestId,
                    "lectureId" to lectureId,
                    "semester" to semester,
                    "studyYear" to studyYear,
                    "query" to cypher.replace("\n", " ")
                ),
                cause = e
            )
        } catch (e: Exception) {
            logger.error("[{}] Необработанная ошибка при получении количества студентов: {}", 
                requestId, e.message, e)
            throw e
        }

        logger.warn("[{}] Не найдены запланированные занятия для лекции {} в семестре {} года {}", 
            requestId, lectureId, semester, studyYear)
        return 0L
    }

    /**
     * Получение документа университета по ID кафедры с обработкой ошибок
     */
    private fun getUniversityByDepartmentId(departmentId: Long, requestId: String): UniversityDocument? {
        logger.debug("[{}] Запрос информации об университете из MongoDB: departmentId={}", 
            requestId, departmentId)
        
        try {
            val query = Query.query(
                Criteria.where("institutes.departments.id").`is`(departmentId.toInt())
            )
            
            logger.debug("[{}] Выполнение MongoDB запроса: query={}", 
                requestId, query)
            
            val result = mongoTemplate.findOne(query, UniversityDocument::class.java)
            
            if (result == null) {
                logger.warn("[{}] Документ университета не найден для departmentId={}", 
                    requestId, departmentId)
            } else {
                logger.debug("[{}] Получен документ университета: id={}, name='{}'", 
                    requestId, result.id, result.name)
            }
            
            return result
        } catch (e: MongoException) {
            logger.error("[{}] Ошибка MongoDB при получении информации об университете: {}", 
                requestId, e.message, e)
            throw MongoDbException(
                message = "Ошибка при получении данных из MongoDB об университете",
                details = mapOf(
                    "requestId" to requestId,
                    "departmentId" to departmentId
                ),
                cause = e
            )
        } catch (e: Exception) {
            logger.error("[{}] Необработанная ошибка при получении информации об университете: {}", 
                requestId, e.message, e)
            throw e
        }
    }
    
    /**
     * Генерация уникального ID запроса для связывания логов
     */
    private fun generateRequestId(): String {
        return UUID.randomUUID().toString().substring(0, 8)
    }
}




