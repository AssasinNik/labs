import random
import datetime
import time
import logging
import sys
from tqdm import tqdm

import psycopg2
from psycopg2 import sql
from faker import Faker

from pymongo import MongoClient
from neo4j import GraphDatabase
import redis
from elasticsearch import Elasticsearch

# Настройка логирования
def setup_logging():
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_format = '%(asctime)s [%(levelname)s] %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # Создаем логгер
    logger = logging.getLogger('data_generator')
    logger.setLevel(logging.INFO)
    
    # Консольный обработчик
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))
    
    # Файловый обработчик
    try:
        file_handler = logging.FileHandler(f'logs/data_generation_{timestamp}.log')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(log_format, date_format))
        logger.addHandler(file_handler)
    except:
        pass  # Игнорируем ошибки с файлом лога, если нет папки logs
    
    logger.addHandler(console_handler)
    return logger

# Инициализируем логгер
logger = setup_logging()

# Инициализация Faker (русская локализация)
fake = Faker("ru_RU")

# Параметры подключения (согласно docker-compose)
PG_CONN_PARAMS = {
    "host": "host.docker.internal",
    "port": 5433,  # проброшенный порт для PostgreSQL
    "user": "admin",
    "password": "secret",
    "dbname": "mydb",
}

MONGO_CONN_STRING = "mongodb://host.docker.internal:27017/"
NEO4J_URI = "bolt://host.docker.internal:7687"
NEO4J_AUTH = None  # если NEO4J_AUTH=none
REDIS_HOST = "host.docker.internal"
REDIS_PORT = 6379
ES_HOSTS = ["http://host.docker.internal:9200"]

# Реальные названия университетов и институтов для более осмысленных данных
UNIVERSITIES = [
    "РТУ МИРЭА", 
    "МГУ им. М.В. Ломоносова", 
    "МГТУ им. Н.Э. Баумана", 
    "Российский университет дружбы народов", 
    "Национальный исследовательский университет ИТМО"
]

INSTITUTES = [
    "Институт информационных технологий", 
    "Институт кибербезопасности", 
    "Институт экономики и управления", 
    "Институт тонких химических технологий", 
    "Институт искусственного интеллекта",
    "Институт радиоэлектроники и информатики", 
    "Физико-технологический институт", 
    "Институт международного образования"
]

DEPARTMENTS = [
    "Кафедра информатики", 
    "Кафедра кибербезопасности", 
    "Кафедра прикладной математики", 
    "Кафедра системного анализа", 
    "Кафедра программной инженерии", 
    "Кафедра информационных систем", 
    "Кафедра вычислительной техники", 
    "Кафедра сетевых технологий",
    "Кафедра искусственного интеллекта", 
    "Кафедра анализа данных"
]

# Реальные названия курсов для более осмысленных данных
COURSES = [
    "Программирование на Python", 
    "Алгоритмы и структуры данных", 
    "Базы данных", 
    "Операционные системы", 
    "Компьютерные сети", 
    "Машинное обучение", 
    "Анализ данных", 
    "Веб-разработка",
    "Основы кибербезопасности", 
    "Разработка мобильных приложений", 
    "Облачные технологии", 
    "Основы DevOps", 
    "Функциональное программирование", 
    "Администрирование сетевых функций",
    "Технологии блокчейн", 
    "Распределенные системы", 
    "Компьютерное зрение", 
    "Информационные системы", 
    "Теория информации", 
    "Компьютерная графика"
]

# Реальные названия лекций для более осмысленных данных
LECTURE_TOPICS = [
    "Введение и основные концепции", 
    "Архитектура и проектирование", 
    "Алгоритмы и оптимизация", 
    "Практическое применение", 
    "Инструменты разработки", 
    "Интеграция и масштабирование", 
    "Тестирование и отладка",
    "Docker - инструмент контейнеризации", 
    "Kubernetes и оркестрация контейнеров", 
    "CI/CD и автоматизация процессов разработки", 
    "Облачные инфраструктуры", 
    "Безопасность и защита данных",
    "Оптимизация производительности", 
    "Микросервисная архитектура", 
    "REST API и коммуникации", 
    "Работа с большими данными", 
    "Методологии разработки ПО", 
    "Аналитика и визуализация данных"
]

##########################################################################
# PostgreSQL: Создание схемы с партиционированием таблицы attendance
##########################################################################

def create_postgres_schema(conn):
    """
    Пересоздаёт схему PostgreSQL для таблиц:
    attendance, schedule, lecture, course, student, groups, department, institute, university.
    Создаются партиции для attendance на период с сентября 2023 до февраля 2024.
    """
    logger.info("Начинаю создание схемы PostgreSQL...")
    start_time = time.time()
    
    cur = conn.cursor()
    tables = [
        "attendance",
        "schedule",
        "lecture",
        "course",
        "student",
        "groups",
        "department",
        "institute",
        "university",
    ]
    
    logger.info("Удаляю существующие таблицы...")
    for i, table in enumerate(tables):
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(table)))
        if (i+1) % 3 == 0 or i == len(tables) - 1:
            logger.info(f"Удалено {i+1}/{len(tables)} таблиц ({((i+1)/len(tables))*100:.1f}%)")
    conn.commit()

    logger.info("Создаю новые таблицы и партиции...")
    schema_sql = """
    -- Университет
    CREATE TABLE university (
        id SERIAL PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );

    -- Институт
    CREATE TABLE institute (
        id SERIAL PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        id_university INT NOT NULL REFERENCES university(id),
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX idx_institute_university ON institute(id_university);

    -- Кафедра
    CREATE TABLE department (
        id SERIAL PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        id_institute INT NOT NULL REFERENCES institute(id),
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX idx_department_institute ON department(id_institute);

    -- Группа
    CREATE TABLE groups (
        id SERIAL PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        id_department INT NOT NULL REFERENCES department(id),
        mongo_id VARCHAR(100),
        formation_year INT,
        created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX idx_groups_department ON groups(id_department);

    -- Студент
    CREATE TABLE student (
        student_number VARCHAR(100) PRIMARY KEY,
        fullname VARCHAR(200) NOT NULL,
        email VARCHAR(255),
        id_group INT NOT NULL REFERENCES groups(id),
        redis_key VARCHAR(100),
        created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX idx_student_group ON student(id_group);

    -- Курс
    CREATE TABLE course (
        id SERIAL PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        id_department INT NOT NULL REFERENCES department(id),
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX idx_course_department ON course(id_department);

    -- Лекция
    CREATE TABLE lecture (
        id SERIAL PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        duration_hours INT DEFAULT 2 CHECK (duration_hours = 2),
        tech_equipment BOOLEAN DEFAULT FALSE,
        id_course INT NOT NULL REFERENCES course(id),
        elasticsearch_id VARCHAR(100),
        created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX idx_lecture_course ON lecture(id_course);

    -- Расписание (schedule)
    CREATE TABLE schedule (
        id SERIAL PRIMARY KEY,
        id_lecture INT NOT NULL REFERENCES lecture(id),
        id_group INT NOT NULL REFERENCES groups(id),
        timestamp TIMESTAMP NOT NULL,
        location VARCHAR(100),
        created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX idx_schedule_timestamp ON schedule(timestamp);
    CREATE INDEX idx_schedule_lecture_group ON schedule(id_lecture, id_group);

    -- Посещение (attendance) с партиционированием по неделям
    CREATE TABLE attendance (
        id SERIAL,
        timestamp TIMESTAMP NOT NULL,
        week_start DATE NOT NULL,
        id_student VARCHAR(100) NOT NULL REFERENCES student(student_number),
        id_schedule INT NOT NULL REFERENCES schedule(id),
        status BOOLEAN NOT NULL DEFAULT TRUE,
        PRIMARY KEY (id, week_start)
    ) PARTITION BY RANGE (week_start);
    CREATE INDEX idx_attendance_student ON attendance(id_student);
    CREATE INDEX idx_attendance_schedule ON attendance(id_schedule);

    -- Триггер для автоматического расчёта week_start по timestamp
    CREATE OR REPLACE FUNCTION set_week_start()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.week_start := DATE_TRUNC('week', NEW.timestamp)::DATE;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    CREATE TRIGGER trg_set_week_start
    BEFORE INSERT OR UPDATE ON attendance
    FOR EACH ROW
    EXECUTE FUNCTION set_week_start();

    -- Создание партиций для attendance
    CREATE TABLE attendance_2023_09 PARTITION OF attendance
        FOR VALUES FROM ('2023-09-01') TO ('2023-10-01');
    CREATE TABLE attendance_2023_10 PARTITION OF attendance
        FOR VALUES FROM ('2023-10-01') TO ('2023-11-01');
    CREATE TABLE attendance_2023_11 PARTITION OF attendance
        FOR VALUES FROM ('2023-11-01') TO ('2023-12-01');
    CREATE TABLE attendance_2023_12 PARTITION OF attendance
        FOR VALUES FROM ('2023-12-01') TO ('2024-01-01');
    CREATE TABLE attendance_2024_01 PARTITION OF attendance
        FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
    """
    cur.execute(schema_sql)
    conn.commit()
    cur.close()
    
    elapsed_time = time.time() - start_time
    logger.info(f"Схема PostgreSQL создана успешно за {elapsed_time:.2f} секунд")

##########################################################################
# Заполнение PostgreSQL данными (University → Institute → Department → Groups → Student,
# Course → Lecture → Schedule → Attendance)
##########################################################################

def populate_postgres(conn):
    """
    Заполняет PostgreSQL иерархически:
      - Университеты, институты, кафедры, группы, студенты.
      - Для каждой кафедры создаются курсы и лекции.
      - Для каждой лекции для всех групп кафедры создаётся расписание.
      - Для выборочных студентов добавляются записи посещаемости.
    Для каждой записи attendance вычисляется week_start на основе времени расписания.
    """
    logger.info("Начинаю заполнение базы данных PostgreSQL...")
    total_start_time = time.time()
    
    cur = conn.cursor()

    # Задаем параметры генерации
    num_universities = min(len(UNIVERSITIES), 3)  # Используем не более 3 университетов
    institutes_per_univ = 4
    departments_per_inst = 5
    groups_per_department = 5
    students_per_group = 100    # Итог: 3 * 4 * 5 * 5 * 100 ≈ 30000 студентов
    courses_per_department = 10
    lectures_per_course = 3
    
    estimated_students = num_universities * institutes_per_univ * departments_per_inst * groups_per_department * students_per_group
    logger.info(f"Планируется создать примерно {estimated_students} студентов")

    # Базовое время для расписания лекций
    base_datetime = datetime.datetime(2023, 9, 4, 9, 0, 0)

    # Списки для контроля вставки
    universities = []    # (id, name)
    institutes = {}      # {uni_id: [(inst_id, name), ...]}
    departments = {}     # {inst_id: [(dept_id, name), ...]}
    groups = {}          # {dept_id: [(group_id, name), ...]}

    # 1. Университеты - используем реальные названия
    logger.info("Создаю университеты...")
    start_time = time.time()
    for i in range(num_universities):
        uni_name = UNIVERSITIES[i]
        cur.execute("INSERT INTO university(name) VALUES (%s) RETURNING id;", (uni_name,))
        uni_id = cur.fetchone()[0]
        universities.append((uni_id, uni_name))
        logger.info(f"Создан университет: {uni_name} (ID: {uni_id})")
    conn.commit()
    elapsed_time = time.time() - start_time
    logger.info(f"Создано {len(universities)} университетов за {elapsed_time:.2f} секунд")

    # 2. Институты → Кафедры → Группы → Студенты
    logger.info("Создаю институты, кафедры, группы и студентов...")
    start_time = time.time()
    total_institutes = 0
    total_departments = 0
    total_groups = 0
    total_students = 0
    
    for uni_idx, (uni_id, uni_name) in enumerate(universities):
        logger.info(f"[{uni_idx+1}/{len(universities)}] Обрабатываю университет: {uni_name}")
        institutes[uni_id] = []
        
        for j in range(institutes_per_univ):
            inst_name = INSTITUTES[j % len(INSTITUTES)]
            cur.execute("INSERT INTO institute(name, id_university) VALUES (%s, %s) RETURNING id;", (inst_name, uni_id))
            inst_id = cur.fetchone()[0]
            institutes[uni_id].append((inst_id, inst_name))
            total_institutes += 1
            
            # Для каждого института создаем кафедры
            departments[inst_id] = []
            for k in range(departments_per_inst):
                dept_name = DEPARTMENTS[k % len(DEPARTMENTS)]
                cur.execute("INSERT INTO department(name, id_institute) VALUES (%s, %s) RETURNING id;", (dept_name, inst_id))
                dept_id = cur.fetchone()[0]
                departments[inst_id].append((dept_id, dept_name))
                total_departments += 1
                
                # Для каждой кафедры создаем группы
                groups[dept_id] = []
                for g in range(groups_per_department):
                    # Формируем названия групп в формате БСБО-XX-YY
                    formation_year = random.randint(2015, 2023)
                    year_suffix = str(formation_year)[-2:]
                    group_name = f"БСБО-{random.randint(1, 99):02d}-{year_suffix}"
                    
                    cur.execute("INSERT INTO groups(name, id_department, formation_year) VALUES (%s, %s, %s) RETURNING id;",
                                (group_name, dept_id, formation_year))
                    group_id = cur.fetchone()[0]
                    groups[dept_id].append((group_id, group_name))
                    total_groups += 1
                    
                    # Прогресс-бар для студентов
                    logger.info(f"Создаю {students_per_group} студентов для группы {group_name}...")
                    
                    # Для каждой группы создаются студенты с реалистичными данными
                    for s in range(students_per_group):
                        student_number = f"S{uni_id}{inst_id}{dept_id}{group_id}{s:04d}"
                        # Генерируем реалистичные ФИО с русскими фамилиями и именами
                        fullname = fake.name()
                        # Создаем реалистичный email на основе имени
                        name_parts = fullname.split()
                        if len(name_parts) >= 2:
                            # Транслитерация для создания email
                            email_name = name_parts[0].lower()
                            email_surname = name_parts[1].lower()
                            # Простая транслитерация для демонстрации
                            translit = {
                                'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
                                'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
                                'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
                                'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch', 'ъ': '',
                                'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya'
                            }
                            email_name_t = ''.join(translit.get(c, c) for c in email_name.lower())
                            email_surname_t = ''.join(translit.get(c, c) for c in email_surname.lower())
                            birth_year = formation_year - random.randint(17, 22)  # Примерный год рождения
                            email = f"{email_surname_t}{email_name_t[0]}{birth_year}@edu.mirea.ru"
                        else:
                            email = fake.email()
                            
                        redis_key = f"student:{student_number}"
                        cur.execute("INSERT INTO student(student_number, fullname, email, id_group, redis_key) VALUES (%s, %s, %s, %s, %s);",
                                    (student_number, fullname, email, group_id, redis_key))
                        total_students += 1
                        
                        # Выводим прогресс каждые 5000 студентов
                        if total_students % 5000 == 0:
                            progress = (total_students / estimated_students) * 100
                            logger.info(f"Прогресс: создано {total_students}/{estimated_students} студентов ({progress:.1f}%)")
                            conn.commit()  # Промежуточный коммит для сохранения данных
            
            # Выводим статистику каждые 2 института
            if total_institutes % 2 == 0:
                logger.info(f"Статистика: институты={total_institutes}, кафедры={total_departments}, группы={total_groups}, студенты={total_students}")
                
    conn.commit()
    elapsed_time = time.time() - start_time
    logger.info(f"Созданы: {total_institutes} институтов, {total_departments} кафедр, {total_groups} групп, {total_students} студентов за {elapsed_time:.2f} секунд")

    # 3. Курсы, лекции, расписание и посещаемость
    logger.info("Создаю курсы, лекции, расписание и данные о посещаемости...")
    start_time = time.time()
    total_courses = 0
    total_lectures = 0
    total_schedules = 0
    total_attendances = 0
    
    # Создаем словарь названий институтов для более информативных логов
    institutes_names = {}
    cur.execute("SELECT id, name FROM institute;")
    for inst_id, inst_name in cur.fetchall():
        institutes_names[inst_id] = inst_name
    
    for inst_id, depts in departments.items():
        inst_name = institutes_names.get(inst_id, f"Институт ID: {inst_id}")
        logger.info(f"Обрабатываю курсы и лекции для института: {inst_name}")
        
        for dept_idx, (dept_id, dept_name) in enumerate(depts):
            logger.info(f"[{dept_idx+1}/{len(depts)}] Кафедра: {dept_name} (ID: {dept_id}, Институт: {inst_name})")
            
            # Используем реальные названия курсов
            available_courses = list(COURSES)  # Копируем список
            random.shuffle(available_courses)  # Перемешиваем для разнообразия
            
            for c in range(min(courses_per_department, len(available_courses))):
                course_name = available_courses[c]
                cur.execute("INSERT INTO course(name, id_department) VALUES (%s, %s) RETURNING id;", (course_name, dept_id))
                course_id = cur.fetchone()[0]
                total_courses += 1
                
                # Для курса создаются лекции с осмысленными названиями
                available_lectures = list(LECTURE_TOPICS)  # Копируем список
                random.shuffle(available_lectures)  # Перемешиваем для разнообразия
                
                for l in range(min(lectures_per_course, len(available_lectures))):
                    lecture_name = f"{available_lectures[l]} ({course_name})"
                    tech_equipment = random.choice([True, False])
                    cur.execute("INSERT INTO lecture(name, duration_hours, tech_equipment, id_course) VALUES (%s, %s, %s, %s) RETURNING id;",
                                (lecture_name, 2, tech_equipment, course_id))
                    lecture_id = cur.fetchone()[0]
                    total_lectures += 1
                    
                    # Для каждой лекции создаём расписание для всех групп данной кафедры
                    cur.execute("SELECT id, name FROM groups WHERE id_department = %s;", (dept_id,))
                    dept_groups = cur.fetchall()
                    
                    logger.info(f"Создаю расписания и посещаемость для лекции: {lecture_name[:30]}...")
                    
                    for group in dept_groups:
                        group_id, group_name = group
                        
                        # Создаем несколько занятий для каждой лекции с разными датами
                        for week_offset in range(0, 15, 2):  # Каждые две недели
                            # Вычисляем день недели (1-5, пн-пт) и время (9:00, 11:00, 14:00, 16:00)
                            weekday = random.randint(1, 5)  # 1-5 (пн-пт)
                            hour = random.choice([9, 11, 14, 16])
                            
                            # Вычисляем конкретную дату
                            schedule_time = base_datetime + datetime.timedelta(weeks=week_offset, days=weekday-1)
                            schedule_time = schedule_time.replace(hour=hour, minute=0, second=0)
                            
                            location = f"А-{random.randint(1, 5)}{random.randint(0, 9)}{random.randint(0, 9)}"
                            cur.execute("INSERT INTO schedule(id_lecture, id_group, timestamp, location) VALUES (%s, %s, %s, %s) RETURNING id;",
                                        (lecture_id, group_id, schedule_time, location))
                            schedule_id = cur.fetchone()[0]
                            total_schedules += 1
                            
                            # Создаем записи посещаемости для студентов группы
                            # С более реалистичным распределением (70-90% посещаемость)
                            cur.execute("SELECT student_number FROM student WHERE id_group = %s;", (group_id,))
                            student_numbers = [row[0] for row in cur.fetchall()]
                            for stud_num in student_numbers:
                                attendance_probability = random.uniform(0.7, 0.9)  # 70-90% вероятность посещения
                                
                                if random.random() < attendance_probability:
                                    attendance_status = True
                                else:
                                    attendance_status = False
                                
                                # Вычисляем week_start перед вставкой - берем начало недели (понедельник) для текущей даты
                                # Используем DATE_TRUNC для получения начала недели, что соответствует логике триггера
                                week_start = (schedule_time - datetime.timedelta(days=schedule_time.weekday())).date()
                                    
                                cur.execute(
                                    "INSERT INTO attendance(timestamp, week_start, id_student, id_schedule, status) VALUES (%s, %s, %s, %s, %s);",
                                    (schedule_time, week_start, stud_num, schedule_id, attendance_status)
                                )
                                total_attendances += 1
                            
                            # Выводим прогресс создания attendance
                            if total_attendances % 50000 == 0:
                                logger.info(f"Прогресс: создано {total_attendances} записей о посещаемости")
                                conn.commit()  # Промежуточный коммит
                
                # Выводим прогресс создания курсов
                if total_courses % 20 == 0:
                    logger.info(f"Прогресс: создано {total_courses} курсов, {total_lectures} лекций, {total_schedules} расписаний")
                    conn.commit()  # Промежуточный коммит
                    
    conn.commit()
    elapsed_time = time.time() - start_time
    logger.info(f"Созданы: {total_courses} курсов, {total_lectures} лекций, {total_schedules} расписаний, {total_attendances} записей о посещаемости за {elapsed_time:.2f} секунд")
    
    total_elapsed = time.time() - total_start_time
    logger.info(f"Заполнение PostgreSQL завершено за {total_elapsed:.2f} секунд")
    cur.close()

##########################################################################
# MongoDB: Заполнение коллекции «universities» вложенной структурой
##########################################################################

def populate_mongodb(pg_conn):
    """
    Извлекает из PostgreSQL данные по университетам, институтам, кафедрам и группам,
    формирует вложенную структуру и загружает в MongoDB.
    """
    logger.info("Начинаю заполнение MongoDB...")
    start_time = time.time()
    
    cur = pg_conn.cursor()
    
    # Получаем всю иерархию: университеты -> институты -> кафедры -> группы
    logger.info("Извлекаю данные из PostgreSQL...")
    cur.execute("""
        SELECT u.id, u.name, i.id, i.name, d.id, d.name, g.id, g.name, g.formation_year
        FROM university u
        JOIN institute i ON i.id_university = u.id
        JOIN department d ON d.id_institute = i.id
        JOIN groups g ON g.id_department = d.id
        ORDER BY u.id, i.id, d.id, g.id;
    """)
    rows = cur.fetchall()
    logger.info(f"Получено {len(rows)} записей из PostgreSQL")
    cur.close()

    # Строим иерархию данных
    logger.info("Строю иерархические документы для MongoDB...")
    mongo_data = {}
    universities_count = 0
    institutes_count = 0
    departments_count = 0
    groups_count = 0
    
    for u_id, u_name, i_id, i_name, d_id, d_name, g_id, g_name, g_year in rows:
        # Инициализируем университет, если его еще нет
        if u_id not in mongo_data:
            mongo_data[u_id] = {
                "id": u_id,
                "name": u_name,
                "institutes": {}
            }
            universities_count += 1
        
        # Инициализируем институт, если его еще нет в университете
        if i_id not in mongo_data[u_id]["institutes"]:
            mongo_data[u_id]["institutes"][i_id] = {
                "id": i_id,
                "name": i_name,
                "departments": {}
            }
            institutes_count += 1
        
        # Инициализируем кафедру, если ее еще нет в институте
        if d_id not in mongo_data[u_id]["institutes"][i_id]["departments"]:
            mongo_data[u_id]["institutes"][i_id]["departments"][d_id] = {
                "id": d_id,
                "name": d_name,
                "groups": []
            }
            departments_count += 1
        
        # Добавляем группу в кафедру
        mongo_data[u_id]["institutes"][i_id]["departments"][d_id]["groups"].append({
            "id": g_id,
            "name": g_name,
            "formation_year": g_year,
            "mongo_id": f"mongo_group_{g_id}"
        })
        groups_count += 1
        
        # Логируем прогресс
        if groups_count % 100 == 0:
            logger.info(f"Обработано {groups_count} групп")

    # Преобразуем словари в списки для финального JSON
    logger.info("Преобразую структуру данных для записи в MongoDB...")
    documents = []
    for uni_id, uni_data in mongo_data.items():
        # Преобразуем словарь институтов в список
        institutes_list = []
        for inst_id, inst_data in uni_data["institutes"].items():
            # Преобразуем словарь кафедр в список
            departments_list = []
            for dept_id, dept_data in inst_data["departments"].items():
                departments_list.append(dept_data)
            inst_data["departments"] = departments_list
            institutes_list.append(inst_data)
        uni_data["institutes"] = institutes_list
        documents.append(uni_data)

    # Записываем в MongoDB
    logger.info(f"Загружаю {len(documents)} университетов в MongoDB...")
    client = MongoClient(MONGO_CONN_STRING)
    db = client["university"]
    collection = db["universities"]
    
    # Очищаем коллекцию перед загрузкой
    logger.info("Очищаю существующую коллекцию в MongoDB...")
    collection.delete_many({})
    
    if documents:
        collection.insert_many(documents)
        logger.info(f"Загружено {len(documents)} документов в MongoDB")
    
    # Обновляем mongo_id в PostgreSQL
    logger.info("Обновляю mongo_id в PostgreSQL...")
    update_mongo_ids = """
    UPDATE groups g
    SET mongo_id = 'mongo_group_' || g.id
    WHERE g.mongo_id IS NULL OR g.mongo_id != 'mongo_group_' || g.id;
    """
    cur = pg_conn.cursor()
    cur.execute(update_mongo_ids)
    pg_conn.commit()
    cur.close()
    
    elapsed_time = time.time() - start_time
    logger.info(f"MongoDB заполнена успешно: {universities_count} университетов, {institutes_count} институтов, {departments_count} кафедр, {groups_count} групп за {elapsed_time:.2f} секунд")

##########################################################################
# Neo4j: Полное заполнение: создаются узлы для кафедр, лекций, групп и студентов;
# устанавливаются отношения:
#  – Lecture -[ORIGINATES_FROM]-> Department (связь через course)
#  – Group -[HAS_SCHEDULE]-> Lecture (на основании расписания)
#  – Student -[BELONGS_TO]-> Group
##########################################################################

def populate_neo4j(pg_conn):
    """
    Полностью переносит данные из PostgreSQL в Neo4j:
      1. Создаются узлы Department.
      2. Создаются узлы Lecture с отношением к соответствующей кафедре (по данным JOIN lecture+course).
      3. Создаются узлы Group.
      4. Создаются узлы Student (в batch-режиме, если их много).
      5. Создаются отношения:
           - (Lecture)-[:ORIGINATES_FROM]->(Department)
           - (Group)-[:HAS_SCHEDULE]->(Lecture) (на основании расписания)
           - (Student)-[:BELONGS_TO]->(Group)
    """
    logger.info("Начинаю заполнение Neo4j...")
    start_time = time.time()
    
    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    with driver.session() as session:
        # Очищаем базу Neo4j
        logger.info("Очищаю существующие данные в Neo4j...")
        session.run("MATCH (n) DETACH DELETE n")
        
        # 1. Создаем узлы Department
        logger.info("Извлекаю данные о кафедрах из PostgreSQL...")
        cur = pg_conn.cursor()
        cur.execute("SELECT id, name FROM department;")
        depts = cur.fetchall()  # (id, name)
        cur.close()
        
        logger.info(f"Создаю {len(depts)} узлов Department в Neo4j...")
        dept_nodes = [{"id": d[0], "name": d[1], "neo_id": f"neo_dept_{d[0]}"} for d in depts]
        session.run("UNWIND $nodes AS node CREATE (d:Department {id: node.id, name: node.name, neo_id: node.neo_id})", nodes=dept_nodes)
        logger.info(f"Создано {len(dept_nodes)} узлов Department")
        
        # 2. Создаем узлы Lecture и связываем с Department
        logger.info("Извлекаю данные о лекциях из PostgreSQL...")
        cur = pg_conn.cursor()
        cur.execute("""
            SELECT l.id, l.name, c.id_department
            FROM lecture l JOIN course c ON l.id_course = c.id;
        """)
        lectures = cur.fetchall()  # (lecture_id, name, dept_id)
        cur.close()
        
        logger.info(f"Создаю {len(lectures)} узлов Lecture в Neo4j...")
        lecture_nodes = [{"id": lec[0], "name": lec[1]} for lec in lectures]
        session.run("UNWIND $nodes AS node CREATE (l:Lecture {id: node.id, name: node.name})", nodes=lecture_nodes)
        
        logger.info("Создаю отношения (Lecture)-[:ORIGINATES_FROM]->(Department)...")
        lecture_relations = 0
        for i, lec in enumerate(lectures):
            lec_id, _, dept_id = lec
            session.run("""
                MATCH (l:Lecture {id: $lec_id}), (d:Department {id: $dept_id})
                CREATE (l)-[:ORIGINATES_FROM]->(d)
            """, lec_id=lec_id, dept_id=dept_id)
            lecture_relations += 1
            if (i+1) % 100 == 0 or i == len(lectures) - 1:
                progress = ((i+1) / len(lectures)) * 100
                logger.info(f"Создано {i+1}/{len(lectures)} отношений для лекций ({progress:.1f}%)")
        
        # 3. Создаем узлы Group
        logger.info("Извлекаю данные о группах из PostgreSQL...")
        cur = pg_conn.cursor()
        cur.execute("SELECT id, name, mongo_id FROM groups;")
        groups = cur.fetchall()  # (id, name, mongo_id)
        cur.close()
        
        logger.info(f"Создаю {len(groups)} узлов Group в Neo4j...")
        group_nodes = [{"id": g[0], "name": g[1], "mongo_id": g[2]} for g in groups]
        session.run("UNWIND $nodes AS node CREATE (g:Group {id: node.id, name: node.name, mongo_id: node.mongo_id})", nodes=group_nodes)
        logger.info(f"Создано {len(group_nodes)} узлов Group")
        
        # 4. Создаем узлы Student и связываем с Group (batch-режим для 30000+ записей)
        logger.info("Извлекаю данные о студентах из PostgreSQL...")
        cur = pg_conn.cursor()
        cur.execute("SELECT student_number, fullname, redis_key, id_group FROM student;")
        students = cur.fetchall()  # (student_number, fullname, redis_key, group_id)
        cur.close()
        
        logger.info(f"Создаю {len(students)} узлов Student в Neo4j (пакетами)...")
        student_nodes = [{"student_number": s[0], "fullname": s[1], "redis_key": s[2], "id_group": s[3]} for s in students]
        batch_size = 1000
        batches_count = (len(student_nodes) + batch_size - 1) // batch_size  # Округление вверх
        
        for i in range(0, len(student_nodes), batch_size):
            batch = student_nodes[i:i+batch_size]
            session.run("""
                UNWIND $nodes AS node
                CREATE (st:Student {student_number: node.student_number, fullname: node.fullname, redis_key: node.redis_key})
            """, nodes=batch)
            
            batch_num = (i // batch_size) + 1
            progress = (batch_num / batches_count) * 100
            logger.info(f"Создано узлов Student: пакет {batch_num}/{batches_count} ({progress:.1f}%)")
        
        # Создаем отношения (Student)-[:BELONGS_TO]->(Group)
        logger.info("Создаю отношения (Student)-[:BELONGS_TO]->(Group)...")
        student_relations = 0
        
        for i, s in enumerate(student_nodes):
            session.run("""
                MATCH (st:Student {student_number: $student_number}), (g:Group {id: $group_id})
                CREATE (st)-[:BELONGS_TO]->(g)
            """, student_number=s["student_number"], group_id=s["id_group"])
            student_relations += 1
            
            if (i+1) % 5000 == 0 or i == len(student_nodes) - 1:
                progress = ((i+1) / len(student_nodes)) * 100
                logger.info(f"Создано {i+1}/{len(student_nodes)} отношений для студентов ({progress:.1f}%)")
        
        # 5. Создаем отношения (Group)-[:HAS_SCHEDULE]->(Lecture) на основании расписания
        logger.info("Извлекаю данные о расписании из PostgreSQL...")
        cur = pg_conn.cursor()
        cur.execute("SELECT DISTINCT id_group, id_lecture FROM schedule;")
        schedule_pairs = cur.fetchall()  # (group_id, lecture_id)
        cur.close()
        
        logger.info(f"Создаю {len(schedule_pairs)} отношений (Group)-[:HAS_SCHEDULE]->(Lecture)...")
        schedule_relations = 0
        
        for i, pair in enumerate(schedule_pairs):
            group_id, lecture_id = pair
            session.run("""
                MATCH (g:Group {id: $group_id}), (l:Lecture {id: $lecture_id})
                CREATE (g)-[:HAS_SCHEDULE]->(l)
            """, group_id=group_id, lecture_id=lecture_id)
            schedule_relations += 1
            
            if (i+1) % 1000 == 0 or i == len(schedule_pairs) - 1:
                progress = ((i+1) / len(schedule_pairs)) * 100
                logger.info(f"Создано {i+1}/{len(schedule_pairs)} отношений для расписаний ({progress:.1f}%)")
    
    driver.close()
    
    elapsed_time = time.time() - start_time
    logger.info(f"Neo4j заполнен успешно: {len(dept_nodes)} кафедр, {len(lecture_nodes)} лекций, {len(group_nodes)} групп, {len(student_nodes)} студентов, {lecture_relations + student_relations + schedule_relations} отношений за {elapsed_time:.2f} секунд")

##########################################################################
# Redis: Запись данных о студентах в виде hash
##########################################################################

def populate_redis(pg_conn):
    """
    Извлекает данные по студентам из PostgreSQL и сохраняет их в Redis.
    Ключ – redis_key, значения – поля fullname, email, group_id, group_name.
    """
    logger.info("Начинаю заполнение Redis...")
    start_time = time.time()
    
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    # Очистка Redis перед заполнением
    logger.info("Очищаю существующие данные в Redis...")
    r.flushdb()
    
    # Получаем информацию о студентах включая название группы
    logger.info("Извлекаю данные о студентах из PostgreSQL...")
    cur = pg_conn.cursor()
    cur.execute("""
        SELECT s.student_number, s.fullname, s.email, s.id_group, g.name as group_name, s.redis_key 
        FROM student s
        JOIN groups g ON s.id_group = g.id
    """)
    students = cur.fetchall()
    logger.info(f"Получено {len(students)} записей о студентах")
    cur.close()

    # Заполняем Redis
    logger.info(f"Загружаю {len(students)} записей в Redis...")
    records_processed = 0
    
    for stud in students:
        student_number, fullname, email, id_group, group_name, redis_key = stud
        r.hset(redis_key, mapping={
            "fullname": fullname,
            "email": email,
            "group_id": id_group,
            "group_name": group_name,
            "redis_key": redis_key
        })
        records_processed += 1
        
        if records_processed % 5000 == 0 or records_processed == len(students):
            progress = (records_processed / len(students)) * 100
            logger.info(f"Загружено {records_processed}/{len(students)} записей в Redis ({progress:.1f}%)")
    
    elapsed_time = time.time() - start_time
    logger.info(f"Redis заполнен успешно: {records_processed} записей за {elapsed_time:.2f} секунд")

##########################################################################
# Elasticsearch: Индексирование лекций
##########################################################################

def populate_elasticsearch(pg_conn):
    """
    Извлекает данные по лекциям из PostgreSQL и индексирует их в Elasticsearch.
    Каждый документ включает id, имя лекции, подробное описание и дату создания.
    """
    logger.info("Начинаю заполнение Elasticsearch...")
    start_time = time.time()
    
    # Подключаемся к Elasticsearch
    logger.info("Подключаюсь к Elasticsearch...")
    es = Elasticsearch(ES_HOSTS)
    
    # Получаем лекции с информацией о курсе
    logger.info("Извлекаю данные о лекциях из PostgreSQL...")
    cur = pg_conn.cursor()
    cur.execute("""
        SELECT l.id, l.name, c.name as course_name, l.tech_equipment, l.created_at 
        FROM lecture l
        JOIN course c ON l.id_course = c.id
    """)
    lectures = cur.fetchall()
    logger.info(f"Получено {len(lectures)} лекций")
    cur.close()

    # Удаляем индекс, если существует, и создаем новый
    if es.indices.exists(index="lectures"):
        logger.info("Удаляю существующий индекс 'lectures'...")
        es.indices.delete(index="lectures")
    
    # Создаем индекс с маппингом
    logger.info("Создаю новый индекс 'lectures' с маппингом...")
    es.indices.create(
        index="lectures",
        body={
            "mappings": {
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "text", "analyzer": "standard"},
                    "description": {"type": "text", "analyzer": "standard"},
                    "course_name": {"type": "keyword"},
                    "tech_equipment": {"type": "boolean"},
                    "created_at": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                    "lecture_id": {"type": "integer"}
                }
            }
        },
        ignore=400
    )

    # Генерация тематических описаний для лекций на основе их названий
    logger.info("Подготавливаю шаблоны описаний лекций...")
    lecture_descriptions = {
        "Docker": "Docker - успешная технология контейнеризации, которая позволяет упаковывать приложения и их зависимости в изолированные контейнеры. Это делает их переносимыми и согласованными в любой среде. Контейнеры запускаются в изолированном пространстве, совместно используя ядро операционной системы.",
        "Kubernetes": "Kubernetes - это платформа для управления контейнерными приложениями в масштабе. Она автоматизирует управление и развертывание, обеспечивает высокую доступность, масштабируемость и управление ресурсами.",
        "CI/CD": "Continuous Integration и Continuous Delivery - это методологии разработки, которые обеспечивают автоматическую сборку, тестирование и доставку кода в рабочую среду, что позволяет команде разработчиков работать быстрее и качественнее.",
        "Python": "Python - один из самых популярных языков программирования, известный своей простотой, читаемостью и обширной экосистемой библиотек. Широко используется в анализе данных, машинном обучении, веб-разработке и автоматизации.",
        "DevOps": "DevOps - это набор практик, которые объединяют разработку программного обеспечения и IT-операции с целью сокращения жизненного цикла системы и обеспечения непрерывной поставки высококачественного программного обеспечения.",
        "Database": "Базы данных - это организованные коллекции структурированных данных. Они обеспечивают эффективное хранение, поиск, обновление и управление информацией в приложениях и информационных системах.",
        "Security": "Безопасность и защита данных - важнейший аспект современных информационных систем. Включает аутентификацию, авторизацию, шифрование, защиту от атак и уязвимостей, соответствие нормативным требованиям.",
        "API": "API (интерфейс программирования приложений) определяет взаимодействие между программными компонентами. RESTful API строится на принципах REST и использует HTTP методы для CRUD операций над ресурсами.",
        "Architecture": "Архитектура программного обеспечения - это высокоуровневая структура системы, определяющая ее компоненты, их взаимодействие и ограничения. Включает принципы проектирования, паттерны и стили архитектуры.",
        "Algorithm": "Алгоритмы - это последовательности шагов для решения вычислительных задач. Правильный выбор алгоритма существенно влияет на эффективность программы, особенно при работе с большими объемами данных."
    }

    # Индексируем лекции
    logger.info(f"Индексирую {len(lectures)} лекций в Elasticsearch...")
    records_processed = 0
    
    for lec in lectures:
        lec_id, name, course_name, tech_equipment, created_at = lec
        
        # Генерируем осмысленное описание на основе ключевых слов в названии лекции
        description = ""
        for keyword, template in lecture_descriptions.items():
            if keyword in name or keyword in course_name:
                description = template
                break
        
        # Если подходящего шаблона не найдено, используем общее описание с элементами из названия
        if not description:
            description = f"Лекция посвящена теме '{name}' в рамках курса '{course_name}'. "
            description += "Рассматриваются основные концепции, методологии и практические аспекты применения. "
            description += "Студенты получат теоретические знания и практические навыки в данной области."
        
        # Если описание слишком длинное, обрезаем его
        if len(description) > 200:
            description = description[:197] + "..."
        
        doc = {
            "id": lec_id,
            "name": name,
            "description": description,
            "course_name": course_name,
            "tech_equipment": tech_equipment,
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "lecture_id": lec_id
        }
        es.index(index="lectures", id=lec_id, body=doc)
        records_processed += 1
        
        if records_processed % 100 == 0 or records_processed == len(lectures):
            progress = (records_processed / len(lectures)) * 100
            logger.info(f"Проиндексировано {records_processed}/{len(lectures)} лекций ({progress:.1f}%)")
    
    elapsed_time = time.time() - start_time
    logger.info(f"Elasticsearch заполнен успешно: {records_processed} лекций за {elapsed_time:.2f} секунд")

##########################################################################
# Обновление Postgres с id из внешних систем (mongo_id, elasticsearch_id, neo_id)
##########################################################################

def update_postgres_ids(pg_conn):
    """
    После загрузки данных во внешние БД обновляет в Postgres поля:
      - groups.mongo_id формируется как 'mongo_group_<id>',
      - lecture.elasticsearch_id формируется как 'elastic_lecture_<id>',
      - В department добавляется (если отсутствует) и обновляется поле neo_id, формируется как 'neo_dept_<id>'.
    """
    logger.info("Начинаю обновление идентификаторов в PostgreSQL...")
    start_time = time.time()
    
    cur = pg_conn.cursor()
    
    logger.info("Обновляю mongo_id для групп...")
    mongo_update_start = time.time()
    cur.execute("UPDATE groups SET mongo_id = 'mongo_group_' || id;")
    mongo_update_time = time.time() - mongo_update_start
    logger.info(f"Обновление mongo_id выполнено за {mongo_update_time:.2f} секунд")
    
    logger.info("Обновляю elasticsearch_id для лекций...")
    es_update_start = time.time()
    cur.execute("UPDATE lecture SET elasticsearch_id = 'elastic_lecture_' || id;")
    es_update_time = time.time() - es_update_start
    logger.info(f"Обновление elasticsearch_id выполнено за {es_update_time:.2f} секунд")
    
    logger.info("Добавляю и обновляю neo_id для кафедр...")
    neo_update_start = time.time()
    # Проверяем существование колонки neo_id
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'department' AND column_name = 'neo_id';
    """)
    column_exists = cur.fetchone()
    
    if not column_exists:
        logger.info("Добавляю колонку neo_id в таблицу department...")
        cur.execute("ALTER TABLE department ADD COLUMN neo_id VARCHAR(100);")
    
    cur.execute("UPDATE department SET neo_id = 'neo_dept_' || id;")
    neo_update_time = time.time() - neo_update_start
    logger.info(f"Обновление neo_id выполнено за {neo_update_time:.2f} секунд")
    
    pg_conn.commit()
    cur.close()
    
    elapsed_time = time.time() - start_time
    logger.info(f"Обновление идентификаторов в PostgreSQL завершено за {elapsed_time:.2f} секунд")

##########################################################################
# Основной запуск: заполнение всех БД и обновление id в Postgres
##########################################################################

def main():
    total_start_time = time.time()
    logger.info("=== Начало процесса генерации данных ===")
    
    # Подключение к PostgreSQL
    try:
        logger.info("Подключение к PostgreSQL...")
        pg_conn = psycopg2.connect(**PG_CONN_PARAMS)
        logger.info("Подключение к PostgreSQL успешно установлено")
    except Exception as e:
        logger.error(f"Ошибка подключения к PostgreSQL: {e}")
        return

    # Создаем схему и заполняем PostgreSQL
    try:
        logger.info("=== Этап 1: Создание и заполнение PostgreSQL ===")
        pg_step_start = time.time()
        
        create_postgres_schema(pg_conn)
        populate_postgres(pg_conn)
        
        pg_step_time = time.time() - pg_step_start
        logger.info(f"Создание и заполнение PostgreSQL завершено за {pg_step_time:.2f} секунд")
    except Exception as e:
        logger.error(f"Ошибка при создании и заполнении PostgreSQL: {e}")
        pg_conn.close()
        return

    # Выводим количество записей в таблицах для контроля
    logger.info("Проверка количества созданных записей в PostgreSQL:")
    check_tables = [
        "university", "institute", "department", "groups", 
        "student", "course", "lecture", "schedule", "attendance"
    ]
    cur = pg_conn.cursor()
    total_records = 0
    
    for tbl in check_tables:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(tbl)))
        count = cur.fetchone()[0]
        total_records += count
        logger.info(f"Таблица {tbl}: {count} записей")
    
    cur.close()
    logger.info(f"Всего в PostgreSQL создано {total_records} записей")

    # Заполнение внешних БД
    try:
        logger.info("=== Этап 2: Заполнение MongoDB ===")
        populate_mongodb(pg_conn)
    except Exception as e:
        logger.error(f"Ошибка при заполнении MongoDB: {e}")
    
    try:
        logger.info("=== Этап 3: Заполнение Neo4j ===")
        populate_neo4j(pg_conn)
    except Exception as e:
        logger.error(f"Ошибка при заполнении Neo4j: {e}")
    
    try:
        logger.info("=== Этап 4: Заполнение Redis ===")
        populate_redis(pg_conn)
    except Exception as e:
        logger.error(f"Ошибка при заполнении Redis: {e}")
    
    try:
        logger.info("=== Этап 5: Заполнение Elasticsearch ===")
        populate_elasticsearch(pg_conn)
    except Exception as e:
        logger.error(f"Ошибка при заполнении Elasticsearch: {e}")

    # Обновляем идентификаторы во внешних системах в Postgres
    try:
        logger.info("=== Этап 6: Обновление идентификаторов в PostgreSQL ===")
        update_postgres_ids(pg_conn)
    except Exception as e:
        logger.error(f"Ошибка при обновлении идентификаторов: {e}")

    pg_conn.close()
    
    total_elapsed_time = time.time() - total_start_time
    minutes = int(total_elapsed_time // 60)
    seconds = total_elapsed_time % 60
    
    logger.info(f"=== Процесс генерации данных завершен ===")
    logger.info(f"Общее время выполнения: {minutes} мин {seconds:.2f} сек")
    
    # Если все прошло успешно, выводим итоговое сообщение
    if total_records > 30000:
        logger.info(f"УСПЕХ! Сгенерировано более 30000 записей ({total_records})")
    else:
        logger.warning(f"ВНИМАНИЕ! Сгенерировано менее 30000 записей ({total_records})")

if __name__ == "__main__":
    main()
