package com.cherenkov.lab_1.dto

/**
 * Класс для возврата результатов отчета с дополнительной информацией о статусе
 *
 * @param status Статус выполнения: "SUCCESS", "WARNING", "ERROR"
 * @param message Сообщение о результате или причине ошибки
 * @param data Список данных отчета (может быть пустым в случае ошибки)
 */
data class ReportResult(
    val status: String,
    val message: String,
    val data: List<StudentReport>
) 