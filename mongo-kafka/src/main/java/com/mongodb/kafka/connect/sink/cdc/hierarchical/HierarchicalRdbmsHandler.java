package com.mongodb.kafka.connect.sink.cdc.hierarchical;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.ArrayList;
import java.io.*;
import java.nio.file.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.DeleteOneModel;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.cdc.CdcHandler;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class HierarchicalRdbmsHandler extends CdcHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(HierarchicalRdbmsHandler.class);
    private static final String ID_FIELD = "_id";
    private static final String NAME_FIELD = "name";
    private static final String INSTITUTES_FIELD = "institutes";
    private static final String DEPARTMENTS_FIELD = "departments";
    private static final String UNIVERSITY_ID_FIELD = "id_university";
    private static final String INSTITUTE_ID_FIELD = "id_institute";

    // Пути к файлам сериализации
    private static final String CACHE_DIR = "/data/cache";
    private static final String UNIVERSITIES_CACHE_FILE = CACHE_DIR + "/universities_cache.json";
    private static final String INSTITUTES_CACHE_FILE = CACHE_DIR + "/institutes_cache.json";

    // Кэш для хранения университетов и институтов
    private static final Map<Integer, BsonDocument> UNIVERSITIES_CACHE = new ConcurrentHashMap<>();
    private static final Map<Integer, BsonDocument> INSTITUTES_CACHE = new ConcurrentHashMap<>();

    private static final String UNIVERSITY_TABLE = "university";
    private static final String INSTITUTE_TABLE = "institute";
    private static final String DEPARTMENT_TABLE = "department";

    // Префикс для временных ID университетов
    private static final int TEMP_UNIVERSITY_ID_PREFIX = 1000000;

    // Gson для сериализации/десериализации
    private static final Gson gson = new GsonBuilder()
            .setPrettyPrinting()
            .create();

    public HierarchicalRdbmsHandler(final MongoSinkTopicConfig config) {
        super(config);
        initializeCacheDirectory();
        loadCaches();
    }

    /**
     * Инициализация директории для кэша
     */
    private void initializeCacheDirectory() {
        try {
            Files.createDirectories(Paths.get(CACHE_DIR));
            LOGGER.info("Cache directory initialized at: {}", CACHE_DIR);
        } catch (IOException e) {
            LOGGER.error("Failed to create cache directory: {}", CACHE_DIR, e);
        }
    }

    /**
     * Загрузка кэшей из файлов
     */
    private void loadCaches() {
        try {
            // Загрузка кэша университетов
            if (Files.exists(Paths.get(UNIVERSITIES_CACHE_FILE))) {
                String universitiesJson = new String(Files.readAllBytes(Paths.get(UNIVERSITIES_CACHE_FILE)));
                Map<Integer, BsonDocument> loadedUniversities = gson.fromJson(universitiesJson,
                        new TypeToken<Map<Integer, BsonDocument>>(){}.getType());
                UNIVERSITIES_CACHE.putAll(loadedUniversities);
                LOGGER.info("Loaded {} universities from cache", loadedUniversities.size());
            }

            // Загрузка кэша институтов
            if (Files.exists(Paths.get(INSTITUTES_CACHE_FILE))) {
                String institutesJson = new String(Files.readAllBytes(Paths.get(INSTITUTES_CACHE_FILE)));
                Map<Integer, BsonDocument> loadedInstitutes = gson.fromJson(institutesJson,
                        new TypeToken<Map<Integer, BsonDocument>>(){}.getType());
                INSTITUTES_CACHE.putAll(loadedInstitutes);
                LOGGER.info("Loaded {} institutes from cache", loadedInstitutes.size());
            }
        } catch (IOException e) {
            LOGGER.error("Failed to load caches from files", e);
        }
    }

    /**
     * Сохранение кэшей в файлы
     */
    private void saveCaches() {
        try {
            // Сохранение кэша университетов
            String universitiesJson = gson.toJson(UNIVERSITIES_CACHE);
            Files.write(Paths.get(UNIVERSITIES_CACHE_FILE), universitiesJson.getBytes());

            // Сохранение кэша институтов
            String institutesJson = gson.toJson(INSTITUTES_CACHE);
            Files.write(Paths.get(INSTITUTES_CACHE_FILE), institutesJson.getBytes());

            LOGGER.info("Caches saved successfully");
        } catch (IOException e) {
            LOGGER.error("Failed to save caches to files", e);
        }
    }

    @Override
    public Optional<WriteModel<BsonDocument>> handle(final SinkDocument doc) {
        BsonDocument keyDoc = doc.getKeyDoc().orElseGet(BsonDocument::new);
        BsonDocument valueDoc = doc.getValueDoc().orElseGet(BsonDocument::new);

        LOGGER.debug("Processing document: {}", valueDoc.toJson());

        try {
            // Проверяем, не является ли это tombstone сообщением
            if (valueDoc.isEmpty()) {
                LOGGER.debug("Skipping tombstone message with key: {}", keyDoc.toJson());
                return Optional.empty();
            }

            // Получаем операцию
            String operation = valueDoc.getString("op").getValue();

            // Получаем данные из секции after или before в зависимости от операции
            BsonDocument payload;
            if (operation.equals("d")) {
                // Для удаления берем данные из before
                if (!valueDoc.containsKey("before") || valueDoc.get("before").isNull()) {
                    // Если секция before пустая, берем ID из ключа
                    BsonDocument keyPayload = keyDoc.getDocument("payload");
                    payload = new BsonDocument("id", keyPayload.get("id"));
                } else {
                    payload = valueDoc.getDocument("before");
                }
            } else if (operation.equals("u")) {
                // Для обновления берем данные из after, но сохраняем id из before
                BsonDocument beforeDoc = valueDoc.getDocument("before");
                BsonDocument afterDoc = valueDoc.getDocument("after");

                // Копируем все поля из after
                payload = afterDoc.clone();

                // Если в after нет id, берем его из before
                if (!afterDoc.containsKey("id") && beforeDoc.containsKey("id")) {
                    payload.put("id", beforeDoc.get("id"));
                }

                // Если все еще нет id, пробуем взять из ключа
                if (!payload.containsKey("id")) {
                    BsonDocument keyPayload = keyDoc.getDocument("payload");
                    if (keyPayload.containsKey("id")) {
                        payload.put("id", keyPayload.get("id"));
                    }
                }
            } else {
                // Для create и read берем данные из after
                payload = valueDoc.getDocument("after");
            }

            // Получаем имя таблицы из source
            String tableName = valueDoc.getDocument("source").getString("table").getValue();

            // В зависимости от операции и таблицы вызываем соответствующий обработчик
            Optional<WriteModel<BsonDocument>> result;

            switch (tableName) {
                case UNIVERSITY_TABLE:
                    if (operation.equals("d")) {
                        result = handleUniversityDelete(payload);
                    } else {
                        result = handleUniversity(payload);
                    }
                    break;

                case INSTITUTE_TABLE:
                    if (operation.equals("d")) {
                        result = handleInstituteDelete(payload);
                    } else {
                        result = handleInstitute(payload);
                    }
                    break;

                case DEPARTMENT_TABLE:
                    if (operation.equals("d")) {
                        result = handleDepartmentDelete(payload);
                    } else {
                        result = handleDepartment(payload);
                    }
                    break;

                default:
                    LOGGER.warn("Unknown table: {}", tableName);
                    return Optional.empty();
            }

            // После обработки любой операции пытаемся очистить временные университеты
            List<WriteModel<BsonDocument>> deleteOps = cleanupTemporaryUniversities();
            if (!deleteOps.isEmpty() && result.isPresent()) {
                WriteModel<BsonDocument> updateOp = result.get();
                List<WriteModel<BsonDocument>> allOps = new ArrayList<>();
                allOps.add(updateOp);
                allOps.addAll(deleteOps);
            }

            // Сохраняем кэши после каждой операции
            saveCaches();

            return result;

        } catch (Exception e) {
            LOGGER.error("Error processing document", e);
            return Optional.empty();
        }
    }

    // Вспомогательный метод для получения Integer из BsonValue, независимо от его типа
    private Integer getIntegerValue(BsonDocument doc, String key) {
        BsonValue value = doc.get(key);
        if (value == null) {
            return null;
        }

        if (value.isInt32()) {
            return value.asInt32().getValue();
        } else if (value.isInt64()) {
            return (int) value.asInt64().getValue();
        } else {
            LOGGER.warn("Value for key {} is not a number: {}", key, value);
            return null;
        }
    }

    private Optional<WriteModel<BsonDocument>> handleUniversity(BsonDocument payload) {
        int universityId = getIntegerValue(payload, "id");
        String universityName = payload.getString("name").getValue();

        // Создаем документ университета
        BsonDocument university = new BsonDocument()
                .append(ID_FIELD, new BsonInt32(universityId))
                .append(NAME_FIELD, new BsonString(universityName))
                .append(INSTITUTES_FIELD, new BsonArray());

        // Проверяем, есть ли временные университеты для институтов этого университета
        for (Map.Entry<Integer, BsonDocument> entry : UNIVERSITIES_CACHE.entrySet()) {
            // Проверяем только временные университеты
            if (entry.getKey() >= TEMP_UNIVERSITY_ID_PREFIX) {
                BsonDocument tempUniversity = entry.getValue();
                BsonArray tempInstitutes = tempUniversity.getArray(INSTITUTES_FIELD);

                // Для каждого института во временном университете проверяем, 
                // есть ли соответствующий институт в базе
                for (BsonValue instituteValue : tempInstitutes) {
                    if (instituteValue.isDocument()) {
                        BsonDocument tempInstitute = instituteValue.asDocument();
                        int instituteId = getIntegerValue(tempInstitute, ID_FIELD);

                        // Ищем реальный институт для этого университета
                        for (Map.Entry<Integer, BsonDocument> instEntry : INSTITUTES_CACHE.entrySet()) {
                            if (instEntry.getKey().equals(instituteId)) {
                                BsonDocument realInstitute = instEntry.getValue();
                                // Если это институт для текущего университета, 
                                // нужно перенести кафедры из временного института
                                if (realInstitute.containsKey(UNIVERSITY_ID_FIELD) &&
                                        Integer.valueOf(getIntegerValue(realInstitute, UNIVERSITY_ID_FIELD)).equals(universityId)) {
                                    // Переносим департаменты из временного института в реальный
                                    if (tempInstitute.containsKey(DEPARTMENTS_FIELD)) {
                                        BsonArray departments = tempInstitute.getArray(DEPARTMENTS_FIELD);
                                        if (realInstitute.containsKey(DEPARTMENTS_FIELD)) {
                                            BsonArray realDepartments = realInstitute.getArray(DEPARTMENTS_FIELD);
                                            for (BsonValue deptValue : departments) {
                                                realDepartments.add(deptValue);
                                            }
                                        } else {
                                            realInstitute.append(DEPARTMENTS_FIELD, departments);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Получаем все институты для этого университета из кэша
        for (Map.Entry<Integer, BsonDocument> entry : INSTITUTES_CACHE.entrySet()) {
            BsonDocument institute = entry.getValue();
            if (institute.containsKey(UNIVERSITY_ID_FIELD) &&
                    Integer.valueOf(getIntegerValue(institute, UNIVERSITY_ID_FIELD)).equals(universityId)) {

                // Добавляем институт в массив, если его там еще нет
                BsonArray institutes = university.getArray(INSTITUTES_FIELD);
                boolean found = false;
                for (BsonValue value : institutes) {
                    if (value.isDocument()) {
                        BsonDocument existingInstitute = value.asDocument();
                        if (Integer.valueOf(getIntegerValue(existingInstitute, ID_FIELD)).equals(entry.getKey())) {
                            found = true;
                            // Обновляем существующий институт
                            String instituteName = institute.getString(NAME_FIELD).getValue();
                            existingInstitute.put(NAME_FIELD, new BsonString(instituteName));

                            // Копируем департаменты, если они есть
                            if (institute.containsKey(DEPARTMENTS_FIELD)) {
                                existingInstitute.put(DEPARTMENTS_FIELD, institute.getArray(DEPARTMENTS_FIELD));
                            } else if (!existingInstitute.containsKey(DEPARTMENTS_FIELD)) {
                                existingInstitute.put(DEPARTMENTS_FIELD, new BsonArray());
                            }
                            break;
                        }
                    }
                }

                if (!found) {
                    // Клонируем институт без ссылки на университет
                    BsonDocument instituteCopy = new BsonDocument()
                            .append(ID_FIELD, new BsonInt32(entry.getKey()))
                            .append(NAME_FIELD, institute.getString(NAME_FIELD));

                    // Копируем департаменты, если они есть
                    if (institute.containsKey(DEPARTMENTS_FIELD)) {
                        instituteCopy.append(DEPARTMENTS_FIELD, institute.getArray(DEPARTMENTS_FIELD));
                    } else {
                        instituteCopy.append(DEPARTMENTS_FIELD, new BsonArray());
                    }

                    institutes.add(instituteCopy);
                }
            }
        }

        // Сохраняем в кэш
        UNIVERSITIES_CACHE.put(universityId, university);

        LOGGER.debug("University processed: {}", university.toJson());

        // Создаем операцию замены
        ReplaceOptions options = new ReplaceOptions().upsert(true);
        return Optional.of(new ReplaceOneModel<>(
                new BsonDocument(ID_FIELD, new BsonInt32(universityId)),
                university,
                options));
    }

    private Optional<WriteModel<BsonDocument>> handleInstitute(BsonDocument payload) {
        int instituteId = getIntegerValue(payload, "id");
        String instituteName = payload.getString("name").getValue();
        int universityId = getIntegerValue(payload, "id_university");

        // Проверяем существование временного института с этим ID
        BsonDocument existingTempInstitute = null;
        for (Map.Entry<Integer, BsonDocument> entry : UNIVERSITIES_CACHE.entrySet()) {
            if (entry.getKey() >= TEMP_UNIVERSITY_ID_PREFIX) {
                BsonArray institutes = entry.getValue().getArray(INSTITUTES_FIELD);
                for (BsonValue value : institutes) {
                    if (value.isDocument()) {
                        BsonDocument institute = value.asDocument();
                        if (Integer.valueOf(getIntegerValue(institute, ID_FIELD)).equals(instituteId)) {
                            existingTempInstitute = institute;
                            break;
                        }
                    }
                }
                if (existingTempInstitute != null) break;
            }
        }

        // Проверяем, существует ли институт в реальном университете
        BsonDocument existingRealInstitute = null;
        for (Map.Entry<Integer, BsonDocument> entry : UNIVERSITIES_CACHE.entrySet()) {
            if (entry.getKey() < TEMP_UNIVERSITY_ID_PREFIX) { // Только реальные университеты
                BsonDocument realUniversity = entry.getValue();
                BsonArray institutes = realUniversity.getArray(INSTITUTES_FIELD);

                for (BsonValue value : institutes) {
                    if (value.isDocument()) {
                        BsonDocument institute = value.asDocument();
                        if (Integer.valueOf(getIntegerValue(institute, ID_FIELD)).equals(instituteId)) {
                            existingRealInstitute = institute;
                            break;
                        }
                    }
                }

                if (existingRealInstitute != null) break;
            }
        }

        // Также проверяем кэш институтов
        BsonDocument existingInstitute = INSTITUTES_CACHE.get(instituteId);

        // Создаем документ института
        BsonDocument institute = new BsonDocument()
                .append(ID_FIELD, new BsonInt32(instituteId))
                .append(NAME_FIELD, new BsonString(instituteName))
                .append(UNIVERSITY_ID_FIELD, new BsonInt32(universityId));

        // Добавляем массив департаментов, сохраняя существующие, если они были
        BsonArray departments = new BsonArray();

        // Порядок приоритета: реальный институт -> институт в кэше -> временный институт
        if (existingRealInstitute != null && existingRealInstitute.containsKey(DEPARTMENTS_FIELD)) {
            departments = existingRealInstitute.getArray(DEPARTMENTS_FIELD);
            LOGGER.debug("Preserved {} departments from existing real institute {}", departments.size(), instituteId);
        } else if (existingInstitute != null && existingInstitute.containsKey(DEPARTMENTS_FIELD)) {
            departments = existingInstitute.getArray(DEPARTMENTS_FIELD);
            LOGGER.debug("Preserved {} departments from cached institute {}", departments.size(), instituteId);
        } else if (existingTempInstitute != null && existingTempInstitute.containsKey(DEPARTMENTS_FIELD)) {
            departments = existingTempInstitute.getArray(DEPARTMENTS_FIELD);
            LOGGER.debug("Preserved {} departments from temporary institute {}", departments.size(), instituteId);
        }

        institute.append(DEPARTMENTS_FIELD, departments);

        // Сохраняем в кэш
        INSTITUTES_CACHE.put(instituteId, institute);

        // Получаем или создаем документ университета
        BsonDocument university = UNIVERSITIES_CACHE.getOrDefault(universityId,
                new BsonDocument()
                        .append(ID_FIELD, new BsonInt32(universityId))
                        .append(NAME_FIELD, new BsonString("Unknown University"))
                        .append(INSTITUTES_FIELD, new BsonArray()));

        // Добавляем институт в массив институтов университета, если его там еще нет
        BsonArray institutes = university.getArray(INSTITUTES_FIELD);
        boolean found = false;
        for (BsonValue value : institutes) {
            if (value.isDocument()) {
                BsonDocument existingInst = value.asDocument();
                if (Integer.valueOf(getIntegerValue(existingInst, ID_FIELD)).equals(instituteId)) {
                    found = true;
                    existingInst.put(NAME_FIELD, new BsonString(instituteName));

                    // Обновляем департаменты, сохраняя существующие
                    existingInst.put(DEPARTMENTS_FIELD, departments);
                    break;
                }
            }
        }

        if (!found) {
            // Клонируем институт без ссылки на университет для вложенного документа
            BsonDocument instituteCopy = new BsonDocument()
                    .append(ID_FIELD, new BsonInt32(instituteId))
                    .append(NAME_FIELD, new BsonString(instituteName))
                    .append(DEPARTMENTS_FIELD, departments);

            institutes.add(instituteCopy);
        }

        // Обновляем кэш
        UNIVERSITIES_CACHE.put(universityId, university);

        LOGGER.debug("Institute processed: {} in University: {}", institute.toJson(), university.toJson());

        // Создаем операцию замены университета
        ReplaceOptions options = new ReplaceOptions().upsert(true);
        return Optional.of(new ReplaceOneModel<>(
                new BsonDocument(ID_FIELD, new BsonInt32(universityId)),
                university,
                options));
    }

    private Optional<WriteModel<BsonDocument>> handleDepartment(BsonDocument payload) {
        int departmentId = getIntegerValue(payload, "id");
        String departmentName = payload.getString("name").getValue();
        int instituteId = getIntegerValue(payload, "id_institute");

        // Создаем документ кафедры
        BsonDocument department = new BsonDocument()
                .append(ID_FIELD, new BsonInt32(departmentId))
                .append(NAME_FIELD, new BsonString(departmentName));

        // Получаем институт из кэша или создаем временный
        BsonDocument institute = INSTITUTES_CACHE.get(instituteId);
        if (institute == null) {
            LOGGER.info("Creating temporary institute with id {} for department {}.",
                    instituteId, departmentId);
            institute = new BsonDocument()
                    .append(ID_FIELD, new BsonInt32(instituteId))
                    .append(NAME_FIELD, new BsonString("Институт " + instituteId))
                    .append(DEPARTMENTS_FIELD, new BsonArray());
            INSTITUTES_CACHE.put(instituteId, institute);

            // Создаем временный университет для института
            int tempUniversityId = TEMP_UNIVERSITY_ID_PREFIX + instituteId;
            if (!UNIVERSITIES_CACHE.containsKey(tempUniversityId)) {
                BsonDocument tempUniversity = new BsonDocument()
                        .append(ID_FIELD, new BsonInt32(tempUniversityId))
                        .append(NAME_FIELD, new BsonString("Университет " + instituteId))
                        .append(INSTITUTES_FIELD, new BsonArray());
                UNIVERSITIES_CACHE.put(tempUniversityId, tempUniversity);
            }

            // Добавляем институт в университет
            BsonDocument tempUniversity = UNIVERSITIES_CACHE.get(tempUniversityId);
            tempUniversity.getArray(INSTITUTES_FIELD).add(institute);
        }

        // Находим университет для этого института
        int universityId = -1;

        // Сначала ищем в реальных институтах (не временных)
        if (institute.containsKey(UNIVERSITY_ID_FIELD)) {
            universityId = getIntegerValue(institute, UNIVERSITY_ID_FIELD);
        } else {
            // Ищем в кэше университетов
            for (Map.Entry<Integer, BsonDocument> entry : UNIVERSITIES_CACHE.entrySet()) {
                if (entry.getKey() < TEMP_UNIVERSITY_ID_PREFIX) { // Только реальные университеты
                    BsonArray institutes = entry.getValue().getArray(INSTITUTES_FIELD);
                    for (BsonValue value : institutes) {
                        if (value.isDocument()) {
                            BsonDocument existingInstitute = value.asDocument();
                            if (Integer.valueOf(getIntegerValue(existingInstitute, ID_FIELD)).equals(instituteId)) {
                                universityId = entry.getKey();
                                break;
                            }
                        }
                    }
                    if (universityId != -1) break;
                }
            }
        }

        // Если не нашли реальный университет, используем временный
        if (universityId == -1) {
            universityId = TEMP_UNIVERSITY_ID_PREFIX + instituteId;
            LOGGER.debug("Using temporary university with id {} for institute {}.",
                    universityId, instituteId);
        }

        // Получаем университет из кэша
        BsonDocument university = UNIVERSITIES_CACHE.get(universityId);

        // Добавляем кафедру в массив кафедр института
        BsonArray institutes = university.getArray(INSTITUTES_FIELD);
        for (BsonValue value : institutes) {
            if (value.isDocument()) {
                BsonDocument existingInstitute = value.asDocument();
                if (Integer.valueOf(getIntegerValue(existingInstitute, ID_FIELD)).equals(instituteId)) {
                    BsonArray departments = existingInstitute.getArray(DEPARTMENTS_FIELD);

                    // Проверяем, существует ли уже кафедра с таким ID
                    boolean found = false;
                    for (BsonValue deptValue : departments) {
                        if (deptValue.isDocument()) {
                            BsonDocument existingDept = deptValue.asDocument();
                            if (Integer.valueOf(getIntegerValue(existingDept, ID_FIELD)).equals(departmentId)) {
                                found = true;
                                existingDept.put(NAME_FIELD, new BsonString(departmentName));
                                break;
                            }
                        }
                    }

                    if (!found) {
                        departments.add(department);
                    }

                    break;
                }
            }
        }

        // Обновляем кэш
        UNIVERSITIES_CACHE.put(universityId, university);

        // Обновляем также институт в кэше с новыми департаментами
        for (BsonValue value : institutes) {
            if (value.isDocument()) {
                BsonDocument existingInstitute = value.asDocument();
                if (Integer.valueOf(getIntegerValue(existingInstitute, ID_FIELD)).equals(instituteId)) {
                    BsonDocument updatedInstitute = INSTITUTES_CACHE.get(instituteId);
                    if (updatedInstitute != null) {
                        updatedInstitute.put(DEPARTMENTS_FIELD, existingInstitute.getArray(DEPARTMENTS_FIELD));
                        INSTITUTES_CACHE.put(instituteId, updatedInstitute);
                    }
                    break;
                }
            }
        }

        LOGGER.debug("Department processed: {} in University: {}", department.toJson(), university.toJson());

        // Создаем операцию замены университета ТОЛЬКО если это не временный университет
        if (universityId < TEMP_UNIVERSITY_ID_PREFIX) {
            ReplaceOptions options = new ReplaceOptions().upsert(true);
            return Optional.of(new ReplaceOneModel<>(
                    new BsonDocument(ID_FIELD, new BsonInt32(universityId)),
                    university,
                    options));
        } else {
            // Проверяем, есть ли реальный университет для этого института
            // Если да, то переносим кафедру туда
            for (Map.Entry<Integer, BsonDocument> entry : UNIVERSITIES_CACHE.entrySet()) {
                if (entry.getKey() < TEMP_UNIVERSITY_ID_PREFIX) {
                    BsonDocument realUniversity = entry.getValue();

                    // Проверяем, привязан ли институт к этому университету
                    BsonDocument realInstitute = null;
                    for (BsonValue instValue : realUniversity.getArray(INSTITUTES_FIELD)) {
                        if (instValue.isDocument()) {
                            BsonDocument inst = instValue.asDocument();
                            if (Integer.valueOf(getIntegerValue(inst, ID_FIELD)).equals(instituteId)) {
                                realInstitute = inst;
                                break;
                            }
                        }
                    }

                    // Если нашли институт в реальном университете, добавляем туда кафедру
                    if (realInstitute != null) {
                        BsonArray realDepartments = realInstitute.getArray(DEPARTMENTS_FIELD);

                        // Проверяем, есть ли уже такая кафедра
                        boolean found = false;
                        for (BsonValue deptValue : realDepartments) {
                            if (deptValue.isDocument()) {
                                BsonDocument existingDept = deptValue.asDocument();
                                if (Integer.valueOf(getIntegerValue(existingDept, ID_FIELD)).equals(departmentId)) {
                                    found = true;
                                    existingDept.put(NAME_FIELD, new BsonString(departmentName));
                                    break;
                                }
                            }
                        }

                        // Если кафедры еще нет, добавляем
                        if (!found) {
                            realDepartments.add(department);
                        }

                        // Обновляем реальный университет
                        ReplaceOptions options = new ReplaceOptions().upsert(true);
                        return Optional.of(new ReplaceOneModel<>(
                                new BsonDocument(ID_FIELD, new BsonInt32(entry.getKey())),
                                realUniversity,
                                options));
                    }
                }
            }

            // Если реального университета нет, то кэшируем изменения,
            // но не создаем операцию замены для MongoDB
            LOGGER.debug("Cached department {} for temporary university {}. Waiting for real university.",
                    departmentId, universityId);
            return Optional.empty();
        }
    }

    /**
     * Удаляет временные университеты, если все их институты были перенесены в реальные университеты.
     * Этот метод вызывается после обработки каждого объекта.
     *
     * @return Список операций удаления для MongoDB
     */
    private List<WriteModel<BsonDocument>> cleanupTemporaryUniversities() {
        List<WriteModel<BsonDocument>> deleteOperations = new ArrayList<>();
        List<Integer> tempUniversitiesToRemove = new ArrayList<>();

        // Проверяем все временные университеты
        for (Map.Entry<Integer, BsonDocument> entry : UNIVERSITIES_CACHE.entrySet()) {
            if (entry.getKey() >= TEMP_UNIVERSITY_ID_PREFIX) {
                int tempUniversityId = entry.getKey();
                BsonDocument tempUniversity = entry.getValue();
                BsonArray tempInstitutes = tempUniversity.getArray(INSTITUTES_FIELD);

                // Проверяем, были ли все институты этого временного университета
                // перемещены в реальные университеты
                boolean allInstitutesTransferred = true;

                for (BsonValue instituteValue : tempInstitutes) {
                    if (instituteValue.isDocument()) {
                        BsonDocument tempInstitute = instituteValue.asDocument();
                        int instituteId = getIntegerValue(tempInstitute, ID_FIELD);

                        // Проверяем, существует ли этот институт в каком-либо реальном университете
                        boolean instituteExistsInRealUniversity = false;

                        for (Map.Entry<Integer, BsonDocument> uniEntry : UNIVERSITIES_CACHE.entrySet()) {
                            if (uniEntry.getKey() < TEMP_UNIVERSITY_ID_PREFIX) { // Только реальные университеты
                                BsonDocument realUniversity = uniEntry.getValue();
                                BsonArray realInstitutes = realUniversity.getArray(INSTITUTES_FIELD);

                                for (BsonValue realInstValue : realInstitutes) {
                                    if (realInstValue.isDocument()) {
                                        BsonDocument realInstitute = realInstValue.asDocument();
                                        if (Integer.valueOf(getIntegerValue(realInstitute, ID_FIELD)).equals(instituteId)) {
                                            instituteExistsInRealUniversity = true;
                                            break;
                                        }
                                    }
                                }

                                if (instituteExistsInRealUniversity) {
                                    break;
                                }
                            }
                        }

                        if (!instituteExistsInRealUniversity) {
                            allInstitutesTransferred = false;
                            break;
                        }
                    }
                }

                // Если все институты были перемещены, удаляем временный университет
                if (allInstitutesTransferred && !tempInstitutes.isEmpty()) {
                    tempUniversitiesToRemove.add(tempUniversityId);

                    // Создаем операцию удаления для MongoDB
                    deleteOperations.add(new DeleteOneModel<>(
                            new BsonDocument(ID_FIELD, new BsonInt32(tempUniversityId))));

                    LOGGER.info("Removing temporary university with ID {}", tempUniversityId);
                }
            }
        }

        // Удаляем временные университеты из кэша
        for (Integer id : tempUniversitiesToRemove) {
            UNIVERSITIES_CACHE.remove(id);
        }

        return deleteOperations;
    }

    /**
     * Обработка удаления университета
     *
     * @param payload Полезная нагрузка события
     * @return Операция удаления для MongoDB
     */
    private Optional<WriteModel<BsonDocument>> handleUniversityDelete(BsonDocument payload) {
        int universityId = getIntegerValue(payload, "id");

        LOGGER.info("Deleting university with id {}", universityId);

        // Удаляем из кэша
        UNIVERSITIES_CACHE.remove(universityId);

        // Удаляем все институты этого университета из кэша
        List<Integer> institutesToRemove = new ArrayList<>();
        for (Map.Entry<Integer, BsonDocument> entry : INSTITUTES_CACHE.entrySet()) {
            BsonDocument institute = entry.getValue();
            if (institute.containsKey(UNIVERSITY_ID_FIELD) &&
                    Integer.valueOf(getIntegerValue(institute, UNIVERSITY_ID_FIELD)).equals(universityId)) {
                institutesToRemove.add(entry.getKey());
            }
        }

        for (Integer instituteId : institutesToRemove) {
            INSTITUTES_CACHE.remove(instituteId);
        }

        // Создаем операцию удаления для MongoDB
        return Optional.of(new DeleteOneModel<>(
                new BsonDocument(ID_FIELD, new BsonInt32(universityId))));
    }

    /**
     * Обработка удаления института
     *
     * @param payload Полезная нагрузка события
     * @return Операция обновления для MongoDB
     */
    private Optional<WriteModel<BsonDocument>> handleInstituteDelete(BsonDocument payload) {
        int instituteId = getIntegerValue(payload, "id");
        int universityId = -1;

        LOGGER.info("Deleting institute with id {}", instituteId);

        // Находим соответствующий университет
        BsonDocument institute = INSTITUTES_CACHE.get(instituteId);
        if (institute != null && institute.containsKey(UNIVERSITY_ID_FIELD)) {
            universityId = getIntegerValue(institute, UNIVERSITY_ID_FIELD);
        } else {
            // Ищем университет, содержащий этот институт
            for (Map.Entry<Integer, BsonDocument> entry : UNIVERSITIES_CACHE.entrySet()) {
                if (entry.getKey() < TEMP_UNIVERSITY_ID_PREFIX) { // Только реальные университеты
                    BsonArray institutes = entry.getValue().getArray(INSTITUTES_FIELD);

                    for (BsonValue value : institutes) {
                        if (value.isDocument()) {
                            BsonDocument existingInstitute = value.asDocument();
                            if (Integer.valueOf(getIntegerValue(existingInstitute, ID_FIELD)).equals(instituteId)) {
                                universityId = entry.getKey();
                                break;
                            }
                        }
                    }

                    if (universityId != -1) break;
                }
            }
        }

        if (universityId == -1) {
            LOGGER.warn("Could not find university for institute {}", instituteId);
            return Optional.empty();
        }

        // Удаляем институт из кэша
        INSTITUTES_CACHE.remove(instituteId);

        // Получаем университет
        BsonDocument university = UNIVERSITIES_CACHE.get(universityId);
        if (university == null) {
            LOGGER.warn("University with id {} not found in cache", universityId);
            return Optional.empty();
        }

        // Удаляем институт из массива институтов университета
        BsonArray institutes = university.getArray(INSTITUTES_FIELD);
        BsonArray updatedInstitutes = new BsonArray();

        for (BsonValue value : institutes) {
            if (value.isDocument()) {
                BsonDocument existingInstitute = value.asDocument();
                if (!Integer.valueOf(getIntegerValue(existingInstitute, ID_FIELD)).equals(instituteId)) {
                    updatedInstitutes.add(existingInstitute);
                }
            }
        }

        university.put(INSTITUTES_FIELD, updatedInstitutes);

        // Обновляем кэш
        UNIVERSITIES_CACHE.put(universityId, university);

        // Создаем операцию обновления университета
        ReplaceOptions options = new ReplaceOptions().upsert(true);
        return Optional.of(new ReplaceOneModel<>(
                new BsonDocument(ID_FIELD, new BsonInt32(universityId)),
                university,
                options));
    }

    /**
     * Обработка удаления кафедры
     *
     * @param payload Полезная нагрузка события
     * @return Операция обновления для MongoDB
     */
    private Optional<WriteModel<BsonDocument>> handleDepartmentDelete(BsonDocument payload) {
        int departmentId = getIntegerValue(payload, "id");
        Integer instituteId = null;

        // Для удаления кафедры нам нужен её ID и ID института
        if (payload.containsKey("id_institute")) {
            int tempInstituteId = getIntegerValue(payload, "id_institute");
            if (tempInstituteId > 0) { // Если ID института > 0, используем его
                instituteId = tempInstituteId;
            }
        }

        // Будем хранить найденные объекты
        BsonDocument targetInstitute = null;
        BsonDocument targetUniversity = null;
        int universityId = -1;

        if (instituteId != null) {
            LOGGER.info("Deleting department id {} from institute id {}", departmentId, instituteId);

            // Ищем университет, содержащий нужный институт
            for (Map.Entry<Integer, BsonDocument> entry : UNIVERSITIES_CACHE.entrySet()) {
                BsonDocument university = entry.getValue();
                BsonArray institutes = university.getArray(INSTITUTES_FIELD);

                for (BsonValue value : institutes) {
                    if (value.isDocument()) {
                        BsonDocument institute = value.asDocument();
                        if (Integer.valueOf(getIntegerValue(institute, ID_FIELD)).equals(instituteId)) {
                            universityId = entry.getKey();
                            targetUniversity = university;
                            targetInstitute = institute;
                            break;
                        }
                    }
                }

                if (targetInstitute != null) break;
            }
        } else {
            // Если ID института не указан или = 0, ищем кафедру во всех университетах и институтах
            LOGGER.info("Deleting department id {} from all institutes", departmentId);

            // Обходим все университеты и институты в поиске кафедры
            for (Map.Entry<Integer, BsonDocument> uniEntry : UNIVERSITIES_CACHE.entrySet()) {
                BsonDocument university = uniEntry.getValue();
                BsonArray institutes = university.getArray(INSTITUTES_FIELD);

                boolean found = false;
                for (int i = 0; i < institutes.size(); i++) {
                    if (institutes.get(i).isDocument()) {
                        BsonDocument institute = institutes.get(i).asDocument();
                        if (institute.containsKey(DEPARTMENTS_FIELD)) {
                            BsonArray departments = institute.getArray(DEPARTMENTS_FIELD);

                            // Ищем кафедру с нужным ID
                            for (BsonValue deptValue : departments) {
                                if (deptValue.isDocument()) {
                                    BsonDocument department = deptValue.asDocument();
                                    if (Integer.valueOf(getIntegerValue(department, ID_FIELD)).equals(departmentId)) {
                                        universityId = uniEntry.getKey();
                                        targetUniversity = university;
                                        targetInstitute = institute;
                                        instituteId = getIntegerValue(institute, ID_FIELD);
                                        found = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if (found) break;
                }
                if (found) break;
            }
        }

        if (targetInstitute == null || targetUniversity == null) {
            LOGGER.warn("Could not find department {} in any institute or university", departmentId);
            return Optional.empty();
        }

        // Найдя институт и кафедру, удаляем кафедру из массива кафедр института
        if (targetInstitute.containsKey(DEPARTMENTS_FIELD)) {
            BsonArray departments = targetInstitute.getArray(DEPARTMENTS_FIELD);
            BsonArray updatedDepartments = new BsonArray();

            for (BsonValue value : departments) {
                if (value.isDocument()) {
                    BsonDocument department = value.asDocument();
                    if (!Integer.valueOf(getIntegerValue(department, ID_FIELD)).equals(departmentId)) {
                        updatedDepartments.add(department);
                    }
                }
            }

            LOGGER.info("Removing department {} from institute {}. Department count before: {}, after: {}",
                    departmentId, instituteId, departments.size(), updatedDepartments.size());

            targetInstitute.put(DEPARTMENTS_FIELD, updatedDepartments);

            // Обновляем институт в кэше INSTITUTES_CACHE, если он там есть
            if (instituteId != null) {
                BsonDocument storedInstitute = INSTITUTES_CACHE.get(instituteId);
                if (storedInstitute != null && storedInstitute.containsKey(DEPARTMENTS_FIELD)) {
                    BsonArray storedDepts = new BsonArray();
                    for (BsonValue dept : storedInstitute.getArray(DEPARTMENTS_FIELD)) {
                        if (dept.isDocument()) {
                            BsonDocument deptDoc = dept.asDocument();
                            if (!Integer.valueOf(getIntegerValue(deptDoc, ID_FIELD)).equals(departmentId)) {
                                storedDepts.add(deptDoc);
                            }
                        }
                    }
                    storedInstitute.put(DEPARTMENTS_FIELD, storedDepts);
                    INSTITUTES_CACHE.put(instituteId, storedInstitute);
                }
            }
        }

        // Создаем операцию обновления университета
        if (universityId >= TEMP_UNIVERSITY_ID_PREFIX) {
            // Это временный университет, не записываем изменения в MongoDB
            return Optional.empty();
        } else {
            ReplaceOptions options = new ReplaceOptions().upsert(true);
            return Optional.of(new ReplaceOneModel<>(
                    new BsonDocument(ID_FIELD, new BsonInt32(universityId)),
                    targetUniversity,
                    options));
        }
    }
}