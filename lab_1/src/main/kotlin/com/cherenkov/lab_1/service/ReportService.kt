package com.cherenkov.lab_1.service

import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders.multiMatch
import com.cherenkov.generated.jooq.tables.Attendance.Companion.ATTENDANCE
import com.cherenkov.generated.jooq.tables.Department.Companion.DEPARTMENT
import com.cherenkov.generated.jooq.tables.Groups.Companion.GROUPS
import com.cherenkov.generated.jooq.tables.Institute.Companion.INSTITUTE
import com.cherenkov.generated.jooq.tables.Schedule.Companion.SCHEDULE
import com.cherenkov.generated.jooq.tables.Student.Companion.STUDENT
import com.cherenkov.generated.jooq.tables.University.Companion.UNIVERSITY
import com.cherenkov.generated.jooq.tables.Lecture.Companion.LECTURE
import com.cherenkov.generated.jooq.tables.Course.Companion.COURSE
import com.cherenkov.lab_1.dto.*
import com.cherenkov.lab_1.exceptions.*
import com.cherenkov.lab_1.mappers.toList123
import org.springframework.data.elasticsearch.core.query.Criteria
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.stereotype.Service
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.springframework.dao.DataAccessException
import org.springframework.data.elasticsearch.NoSuchIndexException
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate
import org.springframework.data.elasticsearch.client.elc.NativeQuery
import org.springframework.data.elasticsearch.core.query.CriteriaQuery
import org.springframework.data.elasticsearch.core.query.Query
import org.springframework.data.redis.RedisConnectionFailureException
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import org.slf4j.LoggerFactory
import org.jooq.Record
import org.jooq.SelectJoinStep
import org.jooq.Table
import org.jooq.exception.DataAccessException as JooqDataAccessException
import org.springframework.data.elasticsearch.core.SearchHits

@Service
class ReportService(
    private val elasticsearchTemplate: ElasticsearchTemplate,
    private val dsl: DSLContext,
    private val redisOperations: RedisTemplate<String, Any>,
) {
    private val logger = LoggerFactory.getLogger(ReportService::class.java)

    fun generateReport(request: ReportRequest): ReportResult {
        logger.info("Начало генерации отчета с параметрами: term={}, startDate={}, endDate={}", 
                    request.term, request.startDate, request.endDate)
        
        try {
            validateReportRequest(request)
            
            // Поиск лекций по заданному термину используя Elasticsearch (оптимально для полнотекстового поиска)
            logger.debug("Поиск лекций по термину: {}", request.term)
            val lectures = searchLecturesByTerm(request.term)
            logger.info("Найдено {} лекций по термину '{}'", lectures.size, request.term)
            
            if (lectures.isEmpty()) {
                logger.warn("Не найдено лекций по термину: {}", request.term)
                return ReportResult(
                    status = "WARNING",
                    message = "Не найдено лекций по указанному термину",
                    data = emptyList()
                )
            }
            
            // Поиск студентов с низкой посещаемостью с оптимизированным запросом
            logger.debug("Поиск студентов с низкой посещаемостью для {} лекций", lectures.size)
            val attendances = findLowAttendanceStudents(lectures.map { it.lectureId }, request.startDate, request.endDate)
            logger.info("Найдено {} студентов с низкой посещаемостью", attendances.size)
            
            if (attendances.isEmpty()) {
                logger.warn("Не найдено студентов с низкой посещаемостью для указанного периода")
                return ReportResult(
                    status = "WARNING",
                    message = "Не найдено студентов с низкой посещаемостью для указанного периода",
                    data = emptyList()
                )
            }
            
            // Получение полной информации о студентах одним оптимизированным запросом
            logger.debug("Получение подробной информации о {} студентах", attendances.size)
            val attendancesAndInfos = getStudentsInfo(attendances)
            logger.info("Получена информация о {} студентах", attendancesAndInfos.size)
            
            if (attendancesAndInfos.isEmpty()) {
                logger.warn("Не удалось получить информацию о студентах")
                return ReportResult(
                    status = "WARNING",
                    message = "Не удалось получить информацию о студентах",
                    data = emptyList()
                )
            }
            
            // Формирование отчета с дополнительными данными из Redis
            logger.debug("Формирование итогового отчета")
            val results = attendancesAndInfos.mapNotNull { (studentInfo, attendance) ->
                try {
                    val redisData = fetchRedisData(studentInfo.redisKey)
                    val report = StudentReport(
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
                        groupNameFromRedis = redisData?.groupName
                    )
                    logger.debug("Создан отчет для студента: {}, посещаемость: {}%", 
                                studentInfo.fullName, attendance.percentage)
                    report
                } catch (e: RedisAccessException) {
                    logger.error("Ошибка Redis для студента {}: {}", 
                                studentInfo.studentNumber, e.message, e)
                    null
                } catch (e: Exception) {
                    logger.error("Непредвиденная ошибка при получении данных для студента {}: {}", 
                                studentInfo.studentNumber, e.message, e)
                    null
                }
            }
            
            logger.info("Отчет успешно сформирован, количество записей: {}", results.size)
            return ReportResult(
                status = "SUCCESS",
                message = "Отчет успешно сформирован",
                data = results
            )
            
        } catch (e: ValidationException) {
            logger.warn("Ошибка валидации запроса: {}", e.message)
            return ReportResult(
                status = "ERROR",
                message = e.message ?: "Ошибка валидации запроса",
                data = emptyList()
            )
        } catch (e: ElasticsearchAccessException) {
            logger.error("Ошибка доступа к Elasticsearch: {}", e.message, e)
            return ReportResult(
                status = "ERROR",
                message = e.message ?: "Ошибка доступа к Elasticsearch",
                data = emptyList()
            )
        } catch (e: DatabaseAccessException) {
            logger.error("Ошибка доступа к базе данных: {}", e.message, e)
            return ReportResult(
                status = "ERROR",
                message = e.message ?: "Ошибка доступа к базе данных",
                data = emptyList()
            )
        } catch (e: RedisAccessException) {
            logger.error("Ошибка доступа к Redis: {}", e.message, e)
            return ReportResult(
                status = "ERROR",
                message = e.message ?: "Ошибка доступа к Redis",
                data = emptyList()
            )
        } catch (e: ReportGenerationException) {
            logger.error("Ошибка генерации отчета: {}", e.message, e)
            return ReportResult(
                status = "ERROR",
                message = e.message ?: "Ошибка при генерации отчета",
                data = emptyList()
            )
        } catch (e: Exception) {
            logger.error("Непредвиденная ошибка при формировании отчета: {}", e.message, e)
            return ReportResult(
                status = "ERROR",
                message = "Непредвиденная ошибка при формировании отчета: ${e.message}",
                data = emptyList()
            )
        }
    }
    
    private fun validateReportRequest(request: ReportRequest) {
        logger.debug("Валидация параметров запроса отчета")
        
        val errors = mutableListOf<String>()
        
        if (request.term.isBlank()) {
            errors.add("Поисковый запрос не может быть пустым")
            logger.warn("Пустой поисковый запрос в запросе отчета")
        }
        
        if (request.startDate.isAfter(request.endDate)) {
            errors.add("Дата начала не может быть позже даты окончания")
            logger.warn("Неверный диапазон дат: начало ({}) позже конца ({})", request.startDate, request.endDate)
        }
        
        val now = Instant.now()
        if (request.endDate.isAfter(now)) {
            errors.add("Дата окончания не может быть в будущем")
            logger.warn("Дата окончания в будущем: {}", request.endDate)
        }
        
        if (errors.isNotEmpty()) {
            val errorMessage = errors.joinToString("; ")
            logger.warn("Ошибки валидации запроса отчета: {}", errorMessage)
            throw ValidationException(errorMessage)
        }
        
        logger.debug("Валидация параметров запроса отчета успешно пройдена")
    }

    private fun searchLecturesByTerm(term: String): List<LectureMaterial> {
        logger.debug("Выполнение поиска в Elasticsearch по термину: {}", term)
        try {
            val criteria = Criteria("description").matches(term)
            val query = CriteriaQuery(criteria)

            logger.debug("Запрос к Elasticsearch: {}", query)
            val answer = elasticsearchTemplate.search(query, LectureMaterial::class.java)
            val result = answer.toList123()

            logger.debug("Результат поиска в Elasticsearch: найдено {} записей", result.size)
            return result
        } catch (e: NoSuchIndexException) {
            logger.error("Ошибка индекса Elasticsearch: {}", e.message, e)
            throw ElasticsearchAccessException("Индекс не найден", e)
        } catch (e: Exception) {
            logger.error("Ошибка при поиске лекций в Elasticsearch: {}", e.message, e)
            throw ElasticsearchAccessException("Ошибка при выполнении поиска: ${e.message}", e)
        }
    }

    private fun findLowAttendanceStudents(
        lectureIds: List<Long>,
        start: Instant,
        end: Instant
    ): List<StudentAttendance> {
        logger.debug("Поиск студентов с низкой посещаемостью. Лекции: {}, период: {} - {}", 
                    lectureIds, start, end)
        
        try {
            val a = ATTENDANCE.`as`("a")
            val s = SCHEDULE.`as`("s")

            val startDateTime = LocalDateTime.ofInstant(start, ZoneId.systemDefault())
            val endDateTime = LocalDateTime.ofInstant(end, ZoneId.systemDefault())
            
            logger.debug("Период для запроса: {} - {}", startDateTime, endDateTime)

            // Оптимизированный запрос с использованием партиционирования по неделям (week_start)
            // В таблице attendance есть партиции по неделям, используем поле week_start для оптимизации
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
                .and(a.WEEK_START.between(
                    startDateTime.toLocalDate(), 
                    endDateTime.toLocalDate()
                )) // Использование партиционирования по неделям
                .groupBy(a.ID_STUDENT)
                .having(DSL.count().ge(3)) // Фильтр для более точной статистики - минимум 3 занятия
                .orderBy(DSL.field("percentage").asc())
                .limit(10)
            
            logger.debug("Выполнение SQL-запроса для поиска посещаемости: {}", query)
            val result = query.fetch()
            
            val attendances = result.map { record ->
                val studentNumber = record[a.ID_STUDENT]!!
                val total = record["total", Long::class.java]!!
                val attended = record["attended", Long::class.java]!!
                val percentage = record["percentage", Double::class.java]!!
                
                logger.debug("Студент {}: всего занятий {}, посещено {}, процент {}%", 
                            studentNumber, total, attended, percentage)
                
                StudentAttendance(
                    studentNumber,
                    total,
                    attended,
                    percentage
                )
            }
            
            logger.debug("Найдено {} студентов с низкой посещаемостью", attendances.size)
            return attendances
        } catch (e: JooqDataAccessException) {
            logger.error("Ошибка JOOQ при доступе к базе данных: {}", e.message, e)
            throw DatabaseAccessException("Ошибка при выполнении запроса к базе данных", e)
        } catch (e: DataAccessException) {
            logger.error("Ошибка доступа к базе данных: {}", e.message, e)
            throw DatabaseAccessException("Ошибка при выполнении запроса к базе данных", e)
        } catch (e: Exception) {
            logger.error("Непредвиденная ошибка при поиске данных о посещаемости: {}", e.message, e)
            throw ReportGenerationException("Ошибка при получении данных о посещаемости: ${e.message}", e)
        }
    }

    private fun getStudentsInfo(attendances: List<StudentAttendance>): List<Pair<StudentInfo, StudentAttendance>> {
        val studentNumbers = attendances.map { it.studentNumber }
        logger.debug("Получение информации о студентах: {}", studentNumbers)
        
        try {
            // Оптимизированный запрос с JOIN всех необходимых таблиц
            // Иерархия: student → groups → department → institute → university
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

            logger.debug("Выполнение SQL-запроса для получения информации о студентах: {}", query)
            val result = query.fetch()
            logger.debug("Получены данные о {} студентах из базы данных", result.size)
            
            // Проверка, что найдены все студенты
            if (result.size < studentNumbers.size) {
                val foundNumbers = result.map { it[STUDENT.STUDENT_NUMBER] }
                val notFoundNumbers = studentNumbers.filter { !foundNumbers.contains(it) }
                logger.warn("Не найдена информация для {} студентов: {}", notFoundNumbers.size, notFoundNumbers)
            }

            // Создаём мапу для быстрого доступа к данным о посещаемости
            val attendanceByStudentNumber = attendances.associateBy { it.studentNumber }

            return result.mapNotNull { record ->
                val studentNumber = record[STUDENT.STUDENT_NUMBER]!!
                val fullName = record[STUDENT.FULLNAME]!!
                val email = record[STUDENT.EMAIL]
                val groupName = record["group_name", String::class.java]!!
                val departmentName = record["department_name", String::class.java]!!
                val instituteName = record["institute_name", String::class.java]!!
                val universityName = record["university_name", String::class.java]!!
                val redisKey = record[STUDENT.REDIS_KEY]
                
                logger.debug("Студент: {}, группа: {}, кафедра: {}, институт: {}, университет: {}", 
                            fullName, groupName, departmentName, instituteName, universityName)
                
                val studentInfo = StudentInfo(
                    studentNumber,
                    fullName,
                    email,
                    groupName,
                    departmentName,
                    instituteName,
                    universityName,
                    redisKey
                )
                
                // Получаем данные о посещаемости
                val attendance = attendanceByStudentNumber[studentNumber]
                if (attendance != null) {
                    studentInfo to attendance
                } else {
                    logger.warn("Не найдена информация о посещаемости для студента {}", studentNumber)
                    null
                }
            }
        } catch (e: JooqDataAccessException) {
            logger.error("Ошибка JOOQ при получении информации о студентах: {}", e.message, e)
            throw DatabaseAccessException("Ошибка при получении информации о студентах из базы данных", e)
        } catch (e: Exception) {
            logger.error("Ошибка при получении информации о студентах: {}", e.message, e)
            throw ReportGenerationException("Ошибка при получении информации о студентах: ${e.message}", e)
        }
    }

    private fun fetchRedisData(redisKey: String?): RedisData? {
        if (redisKey == null) {
            logger.debug("Redis ключ не указан для студента")
            return null
        }
        
        logger.debug("Получение данных из Redis по ключу: {}", redisKey)
        try {
            // Redis хранит данные о студентах в виде хеш-таблиц с полями:
            // fullname, email, group_id, group_name, redis_key
            val dataMap = redisOperations.opsForHash<String, String>().entries(redisKey)
            logger.debug("Получены данные из Redis: {}", dataMap)
            
            return if (dataMap.isNotEmpty()) {
                val groupName = dataMap["group_name"]
                logger.debug("Найдены данные в Redis: group_name={}", groupName)
                RedisData(groupName)
            } else {
                logger.warn("В Redis нет данных по ключу: {}", redisKey)
                null
            }
        } catch (e: RedisConnectionFailureException) {
            logger.error("Ошибка подключения к Redis: {}", e.message, e)
            throw RedisAccessException("Ошибка подключения к Redis", e)
        } catch (e: Exception) {
            logger.error("Ошибка при получении данных из Redis: {}", e.message, e)
            throw RedisAccessException("Ошибка при получении данных из Redis: ${e.message}", e)
        }
    }
}




