package com.cherenkov.lab_3.service

import com.cherenkov.lab_3.dto.ReportDTO
import com.cherenkov.lab_3.dto.RedisStudentInfo
import com.cherenkov.lab_3.model.UniversityDocument
import com.cherenkov.lab_3.repository.AttendanceRepository
import com.cherenkov.lab_3.repository.GroupRepository
import com.cherenkov.lab_3.repository.ScheduleRepository
import org.slf4j.LoggerFactory
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.neo4j.core.Neo4jClient
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class ReportService(
    private val groupRepo: GroupRepository,
    private val scheduleRepo: ScheduleRepository,
    private val attendanceRepo: AttendanceRepository,
    private val redisTemplate: RedisTemplate<String, Any>,
    private val neo4jClient: Neo4jClient,
    private val mongoTemplate: MongoTemplate
) {
    private val log = LoggerFactory.getLogger(ReportService::class.java)

    /**
     * Получить отчет по группе студентов
     * Отчет включает информацию о группе, студентах, количестве запланированных и посещенных часов
     */
    fun getReportByGroup(groupId: Long): List<ReportDTO> {
        log.info("Начинаем формирование отчета для группы {}", groupId)

        // Получаем ID лекций специальных дисциплин
        val specialIds = getSpecialLectureIds(groupId)
        log.debug("Найдено {} специальных лекций: {}", specialIds.size, specialIds)

        // Получаем запланированные часы
        val plannedHours = getPlannedHours(groupId, specialIds)
        log.debug("Запланированное количество часов для группы {}: {}", groupId, plannedHours)

        // Получаем посещенные часы для каждого студента
        val attended = getAttendedHours(groupId, specialIds)
        log.debug("Количество посещенных часов по студентам: {} записей", attended.size)

        // Получаем всех студентов, для которых есть данные по посещаемости
        val students = attended.keys

        // Получаем информацию о группе
        val group = groupRepo.findById(groupId)
            .orElseThrow {
                log.error("Группа с ID {} не найдена", groupId)
                RuntimeException("Группа не найдена")
            }

        // Получаем данные о курсах для специальных лекций
        val courseInfo = getCourseInfo(specialIds)

        // Формируем отчет для каждого студента
        val report = students.map { studentNumber ->
            ReportDTO().apply {
                groupName = group.name
                this.studentNumber = studentNumber
                this.plannedHours = plannedHours
                attendedHours = attended.getOrDefault(studentNumber, 0)
                
                // Обогащаем отчет информацией о студенте из Redis
                enrichStudentInfo(this, studentNumber)
                
                // Обогащаем отчет информацией об иерархии из MongoDB
                val deptId = group.get("id_department", Int::class.java).toLong()
                enrichHierarchy(this, deptId)

            }
        }

        log.info("Формирование отчета завершено: {} записей для группы {}", report.size, groupId)
        return report
    }

    /**
     * Получить ID специальных лекций для группы (из PostgreSQL)
     */
    fun getSpecialLectureIds(groupId: Long): MutableList<Int?> {
        val group = groupRepo.findById(groupId)
            .orElseThrow { RuntimeException("Группа не найдена") }
        val deptId = group.get("id_department", Int::class.java).toLong()
        return scheduleRepo.findSpecialLectureIds(groupId, deptId)
    }

    /**
     * Получить количество посещенных часов для каждого студента (из PostgreSQL)
     */
    fun getAttendedHours(groupId: Long, lectureIds: MutableList<Int?>): Map<String, Int> {
        return attendanceRepo.findAttendedHoursByGroupAndLectures(groupId, lectureIds)
            .associate { it.first to it.second }
    }

    /**
     * Получить общее количество запланированных часов для группы (из Neo4j)
     */
    @Transactional(readOnly = true)
    fun getPlannedHours(groupId: Long, lectureIds: MutableList<Int?>): Int {
        if (lectureIds.isEmpty()) {
            log.debug("Neo4j логика: пустой список лекций → запланированные часы = 0")
            return 0
        }

        return neo4jClient.query(
            """
            MATCH (g:Group {id: ${'$'}groupId})-[r:HAS_SCHEDULE]->(l:Lecture)
              WHERE l.id IN ${'$'}ids
            RETURN COUNT(r) * 2 AS hours
            """
        )
        .bind(groupId).to("groupId")
        .bind(lectureIds).to("ids")
        .fetch().one()
        .map { record -> (record["hours"] as Number).toInt() }
        .orElse(0)
    }

    /**
     * Получить информацию о курсах из специальных лекций
     */
    fun getCourseInfo(lectureIds: MutableList<Int?>): String {
        if (lectureIds.isEmpty()) return "Специальные дисциплины"
        
        // Здесь можно добавить запрос к БД для получения названий курсов
        // Для простоты сейчас просто возвращаем строку с информацией
        return "Специальные дисциплины (${lectureIds.size} лекций)"
    }

    /**
     * Обогатить отчет информацией о студенте из Redis
     */
    fun enrichStudentInfo(dto: ReportDTO, studentNumber: String) {
        val studentInfo = redisTemplate.opsForValue()
            .get("student:$studentNumber") as? RedisStudentInfo
            
        studentInfo?.let {
            dto.studentName = it.fullname
            dto.email = it.email
        }
    }

    /**
     * Обогатить отчет информацией об иерархии университета из MongoDB
     */
    fun enrichHierarchy(dto: ReportDTO, deptId: Long) {
        try {
            log.debug("Поиск иерархии для departmentId: {}", deptId)
            
            // Ищем университет, содержащий кафедру с указанным ID
            val university = mongoTemplate.findAll(UniversityDocument::class.java)
                .find { uni ->
                    uni.institutes.any { inst ->
                        inst.departments.any { dept ->
                            dept.id == deptId.toInt()
                        }
                    }
                }

            if (university != null) {
                dto.university = university.name
                
                // Находим институт, содержащий кафедру
                val institute = university.institutes.find { inst ->
                    inst.departments.any { dept -> dept.id == deptId.toInt() }
                }
                
                if (institute != null) {
                    dto.institute = institute.name
                    
                    // Находим кафедру
                    val department = institute.departments.find { dept ->
                        dept.id == deptId.toInt()
                    }
                    
                    if (department != null) {
                        dto.department = department.name
                        log.debug("Найдена иерархия: университет={}, институт={}, кафедра={}", 
                                university.name, institute.name, department.name)
                        return
                    }
                }
            }
            
            log.warn("Не удалось найти иерархию для departmentId: {}", deptId)
            setDefaultHierarchy(dto, deptId)
            
        } catch (e: Exception) {
            log.error("Ошибка при обогащении данными иерархии: {}", e.message)
            setDefaultHierarchy(dto, deptId)
        }
    }

    /**
     * Установить значения иерархии по умолчанию
     */
    private fun setDefaultHierarchy(dto: ReportDTO, deptId: Long) {
        dto.university = "МИРЭА - Российский технологический университет"
        dto.institute = "Институт информационных технологий"
        dto.department = "Кафедра #$deptId"
    }
}




