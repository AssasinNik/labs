import random
import datetime
import time
import sys
from tqdm import tqdm

import psycopg2
from psycopg2 import sql
from faker import Faker

from pymongo import MongoClient
from neo4j import GraphDatabase
import redis
from elasticsearch import Elasticsearch

# Импортируем наш визуализатор вместо стандартного логгера
from terminal_visualizer import start_operation, update_progress, complete_operation, info, show_summary

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
    operation = start_operation("Создание схемы PostgreSQL", 100)
    
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
        "users"
    ]
    
    info("Удаление существующих таблиц...")
    update_progress(operation, 10)
    
    for i, table in enumerate(tables):
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(table)))
        update_progress(operation, 10 + int(20 * (i+1) / len(tables)))
    conn.commit()

    info("Создание новых таблиц и партиций...")
    update_progress(operation, 30)
    
    schema_sql = """
    CREATE TABLE university (
        id SERIAL PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE institute (
        id SERIAL PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        id_university INT NOT NULL REFERENCES university(id),
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX idx_institute_university ON institute(id_university);

    CREATE TABLE department (
        id SERIAL PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        id_institute INT NOT NULL REFERENCES institute(id),
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX idx_department_institute ON department(id_institute);

    CREATE TABLE groups (
        id SERIAL PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        id_department INT NOT NULL REFERENCES department(id),
        mongo_id VARCHAR(100),
        formation_year INT,
        created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX idx_groups_department ON groups(id_department);

    CREATE TABLE student (
        student_number VARCHAR(100) PRIMARY KEY,
        fullname VARCHAR(200) NOT NULL,
        email VARCHAR(255),
        id_group INT NOT NULL REFERENCES groups(id),
        redis_key VARCHAR(100),
        created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX idx_student_group ON student(id_group);

    CREATE TABLE course (
        id SERIAL PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        id_department INT NOT NULL REFERENCES department(id),
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX idx_course_department ON course(id_department);

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
        
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(100) NOT NULL,
        hash_password VARCHAR(255) NOT NULL
    );
    """
    
    update_progress(operation, 50)
    cur.execute(schema_sql)
    conn.commit()
    cur.close()
    
    update_progress(operation, 100)
    complete_operation(operation)

def populate_postgres(conn):
    op_main = start_operation("Заполнение PostgreSQL", 100)
    
    cur = conn.cursor()

    num_universities = min(len(UNIVERSITIES), 3)
    institutes_per_univ = 4
    departments_per_inst = 5
    groups_per_department = 5
    students_per_group = 30
    courses_per_department = 5
    lectures_per_course = 2
    
    estimated_students = num_universities * institutes_per_univ * departments_per_inst * groups_per_department * students_per_group
    info(f"Планируется создать примерно {estimated_students} студентов")

    base_datetime = datetime.datetime(2023, 9, 4, 9, 0, 0)

    universities = []
    institutes = {}
    departments = {}
    groups = {}

    # 1. Университеты
    op_universities = start_operation("Создание университетов", num_universities)
    for i in range(num_universities):
        uni_name = UNIVERSITIES[i]
        cur.execute("INSERT INTO university(name) VALUES (%s) RETURNING id;", (uni_name,))
        uni_id = cur.fetchone()[0]
        universities.append((uni_id, uni_name))
        update_progress(op_universities, i+1)
    conn.commit()
    complete_operation(op_universities)
    update_progress(op_main, 10)

    # 2. Институты → Кафедры → Группы → Студенты
    total_institutes = 0
    total_departments = 0
    total_groups = 0
    total_students = 0
    
    student_batch = []
    STUDENT_BATCH_SIZE = 1000  # Размер пакета для студентов
    
    # Институты
    op_institutes = start_operation("Создание институтов", num_universities * institutes_per_univ)
    for uni_idx, (uni_id, uni_name) in enumerate(universities):
        institutes[uni_id] = []
        
        for j in range(institutes_per_univ):
            inst_name = INSTITUTES[j % len(INSTITUTES)]
            cur.execute("INSERT INTO institute(name, id_university) VALUES (%s, %s) RETURNING id;", (inst_name, uni_id))
            inst_id = cur.fetchone()[0]
            institutes[uni_id].append((inst_id, inst_name))
            total_institutes += 1
            update_progress(op_institutes, total_institutes)
    conn.commit()
    complete_operation(op_institutes)
    update_progress(op_main, 20)
    
    # Кафедры
    total_dept_expected = num_universities * institutes_per_univ * departments_per_inst
    op_departments = start_operation("Создание кафедр", total_dept_expected)
    for uni_id, uni_institutes in institutes.items():
        for inst_id, inst_name in uni_institutes:
            departments[inst_id] = []
            for k in range(departments_per_inst):
                dept_name = DEPARTMENTS[k % len(DEPARTMENTS)]
                cur.execute("INSERT INTO department(name, id_institute) VALUES (%s, %s) RETURNING id;", (dept_name, inst_id))
                dept_id = cur.fetchone()[0]
                departments[inst_id].append((dept_id, dept_name))
                total_departments += 1
                update_progress(op_departments, total_departments)
    conn.commit()
    complete_operation(op_departments)
    update_progress(op_main, 30)
    
    # Группы
    total_groups_expected = total_dept_expected * groups_per_department
    op_groups = start_operation("Создание групп", total_groups_expected)
    for inst_id, depts in departments.items():
        for dept_id, dept_name in depts:
            groups[dept_id] = []
            for g in range(groups_per_department):
                formation_year = random.randint(2015, 2023)
                year_suffix = str(formation_year)[-2:]
                group_name = f"БСБО-{random.randint(1, 99):02d}-{year_suffix}"
                
                cur.execute("INSERT INTO groups(name, id_department, formation_year) VALUES (%s, %s, %s) RETURNING id;",
                            (group_name, dept_id, formation_year))
                group_id = cur.fetchone()[0]
                groups[dept_id].append((group_id, group_name))
                total_groups += 1
                update_progress(op_groups, total_groups)
    conn.commit()
    complete_operation(op_groups)
    update_progress(op_main, 40)
    
    # Студенты
    op_students = start_operation("Создание студентов", estimated_students)
    for dept_id, dept_groups in groups.items():
        for group_id, group_name in dept_groups:
            for s in range(students_per_group):
                student_number = f"S{group_id}{s:04d}"
                fullname = fake.name()
                name_parts = fullname.split()
                if len(name_parts) >= 2:
                    email_name = name_parts[0].lower()
                    email_surname = name_parts[1].lower()
                    translit = {
                        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
                        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
                        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
                        'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch', 'ъ': '',
                        'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya'
                    }
                    email_name_t = ''.join(translit.get(c, c) for c in email_name.lower())
                    email_surname_t = ''.join(translit.get(c, c) for c in email_surname.lower())
                    birth_year = formation_year - random.randint(17, 22)
                    email = f"{email_surname_t}{email_name_t[0]}{birth_year}@edu.mirea.ru"
                else:
                    email = fake.email()
                    
                redis_key = f"student:{student_number}"
                student_batch.append((student_number, fullname, email, group_id, redis_key))
                total_students += 1
                update_progress(op_students, total_students)
                
                if len(student_batch) >= STUDENT_BATCH_SIZE:
                    values = ','.join(cur.mogrify("(%s, %s, %s, %s, %s)", row).decode('utf-8') for row in student_batch)
                    sql_stmt = f"INSERT INTO student (student_number, fullname, email, id_group, redis_key) VALUES {values}"
                    cur.execute(sql_stmt)
                    conn.commit()
                    student_batch = []
    
    # Вставляем оставшихся студентов
    if student_batch:
        values = ','.join(cur.mogrify("(%s, %s, %s, %s, %s)", row).decode('utf-8') for row in student_batch)
        sql_stmt = f"INSERT INTO student (student_number, fullname, email, id_group, redis_key) VALUES {values}"
        cur.execute(sql_stmt)
        conn.commit()
    
    complete_operation(op_students)
    update_progress(op_main, 60)
    
    # 3. Курсы, лекции, расписание и посещаемость
    op_courses = start_operation("Создание курсов", total_departments * courses_per_department)
    
    total_courses = 0
    total_lectures = 0
    total_schedules = 0
    total_attendances = 0
    
    attendance_batch = []
    ATTENDANCE_BATCH_SIZE = 10000  # Размер пакета для посещаемости
    
    institutes_names = {}
    cur.execute("SELECT id, name FROM institute;")
    for inst_id, inst_name in cur.fetchall():
        institutes_names[inst_id] = inst_name
    
    for inst_id, depts in departments.items():
        inst_name = institutes_names.get(inst_id, f"Институт ID: {inst_id}")
        
        for dept_idx, (dept_id, dept_name) in enumerate(depts):            
            available_courses = list(COURSES)
            random.shuffle(available_courses)
            
            for c in range(min(courses_per_department, len(available_courses))):
                course_name = available_courses[c]
                cur.execute("INSERT INTO course(name, id_department) VALUES (%s, %s) RETURNING id;", (course_name, dept_id))
                course_id = cur.fetchone()[0]
                total_courses += 1
                update_progress(op_courses, total_courses)
    
    complete_operation(op_courses)
    update_progress(op_main, 70)
    
    # Лекции
    op_lectures = start_operation("Создание лекций", total_courses * lectures_per_course)
    
    cur.execute("SELECT id, name, id_department FROM course;")
    all_courses = cur.fetchall()
    
    for course_id, course_name, dept_id in all_courses:
        available_lectures = list(LECTURE_TOPICS)
        random.shuffle(available_lectures)
        
        for l in range(min(lectures_per_course, len(available_lectures))):
            lecture_name = f"{available_lectures[l]} ({course_name})"
            tech_equipment = random.choice([True, False])
            cur.execute("INSERT INTO lecture(name, duration_hours, tech_equipment, id_course) VALUES (%s, %s, %s, %s) RETURNING id;",
                        (lecture_name, 2, tech_equipment, course_id))
            lecture_id = cur.fetchone()[0]
            total_lectures += 1
            update_progress(op_lectures, total_lectures)
    
    complete_operation(op_lectures)
    update_progress(op_main, 75)
    
    # Расписание
    op_schedule = start_operation("Создание расписаний", 1000)  # Примерное значение
    
    cur.execute("SELECT l.id, c.id_department FROM lecture l JOIN course c ON l.id_course = c.id;")
    lectures_depts = cur.fetchall()
    
    schedule_count = 0
    
    for lecture_id, dept_id in lectures_depts:
        cur.execute("SELECT id FROM groups WHERE id_department = %s;", (dept_id,))
        dept_groups = cur.fetchall()
        
        for group in dept_groups:
            group_id = group[0]
            
            for week_offset in range(0, 15, 2):
                weekday = random.randint(1, 5)
                hour = random.choice([9, 11, 14, 16])
                schedule_time = base_datetime + datetime.timedelta(weeks=week_offset, days=weekday-1)
                schedule_time = schedule_time.replace(hour=hour, minute=0, second=0)
                week_start = (schedule_time - datetime.timedelta(days=schedule_time.weekday())).date()
                
                location = f"А-{random.randint(1, 5)}{random.randint(0, 9)}{random.randint(0, 9)}"
                cur.execute("INSERT INTO schedule(id_lecture, id_group, timestamp, location) VALUES (%s, %s, %s, %s) RETURNING id;",
                            (lecture_id, group_id, schedule_time, location))
                schedule_id = cur.fetchone()[0]
                total_schedules += 1
                schedule_count += 1
                
                if schedule_count % 100 == 0:
                    update_progress(op_schedule, min(schedule_count, 1000))
    
    conn.commit()
    complete_operation(op_schedule)
    update_progress(op_main, 80)
    
    # Посещаемость
    op_attendance = start_operation("Создание записей посещаемости", 1000000)  # Примерное значение
    
    cur.execute("""
        SELECT s.id, s.id_group, s.timestamp 
        FROM schedule s
        ORDER BY s.id_group, s.timestamp;
    """)
    all_schedules = cur.fetchall()
    
    attendance_count = 0
    attendance_total = 0
    
    for schedule_id, group_id, schedule_time in all_schedules:
        cur.execute("SELECT student_number FROM student WHERE id_group = %s;", (group_id,))
        student_numbers = [row[0] for row in cur.fetchall()]
        
        week_start = (schedule_time - datetime.timedelta(days=schedule_time.weekday())).date()
        
        for stud_num in student_numbers:
            attendance_probability = random.uniform(0.7, 0.9)
            attendance_status = random.random() < attendance_probability
            
            attendance_batch.append((schedule_time, week_start, stud_num, schedule_id, attendance_status))
            total_attendances += 1
            attendance_count += 1
            
            if len(attendance_batch) >= ATTENDANCE_BATCH_SIZE:
                values = ','.join(cur.mogrify("(%s, %s, %s, %s, %s)", row).decode('utf-8') for row in attendance_batch)
                sql_stmt = f"INSERT INTO attendance (timestamp, week_start, id_student, id_schedule, status) VALUES {values}"
                cur.execute(sql_stmt)
                conn.commit()
                attendance_batch = []
                attendance_total += attendance_count
                update_progress(op_attendance, min(attendance_total, 1000000))
                attendance_count = 0
    
    # Вставляем оставшиеся записи посещаемости
    if attendance_batch:
        values = ','.join(cur.mogrify("(%s, %s, %s, %s, %s)", row).decode('utf-8') for row in attendance_batch)
        sql_stmt = f"INSERT INTO attendance (timestamp, week_start, id_student, id_schedule, status) VALUES {values}"
        cur.execute(sql_stmt)
        conn.commit()
        attendance_total += attendance_count
        update_progress(op_attendance, min(attendance_total, 1000000))
    
    complete_operation(op_attendance)
    update_progress(op_main, 90)
    
    # === (4) Специальные лекции ===
    op_special = start_operation("Добавление специальных лекций", 100)
    target_group_id = 4
    
    update_progress(op_special, 10)
    
    # получаем все лекции и их кафедры
    cur.execute("""
        SELECT l.id AS lec_id, c.id_department AS dept_id
        FROM lecture l
        JOIN course c ON l.id_course = c.id
    """)
    lecture_depts = cur.fetchall()
    update_progress(op_special, 30)

    # определяем dept_id для нашей группы
    target_dept_id = None
    for dept_id, dept_groups in groups.items():
        if any(gid == target_group_id for gid, _ in dept_groups):
            target_dept_id = dept_id
            break
    update_progress(op_special, 50)

    if target_dept_id is None:
        info(f"Группа {target_group_id} не найдена — спец-лекции не добавлены")
    else:
        # отбираем лекции из других кафедр
        other_lects = [lec for lec, d in lecture_depts if d != target_dept_id]
        special_sample = random.sample(other_lects, min(2, len(other_lects)))
        
        update_progress(op_special, 60)

        # удаляем старые schedule и attendance для этих лекций
        cur.execute(
            "DELETE FROM attendance WHERE id_schedule IN "
            "(SELECT id FROM schedule WHERE id_group = %s AND id_lecture = ANY(%s))",
            (target_group_id, special_sample)
        )
        cur.execute(
            "DELETE FROM schedule WHERE id_group = %s AND id_lecture = ANY(%s)",
            (target_group_id, special_sample)
        )
        conn.commit()
        update_progress(op_special, 70)

        # создаём новые schedule и собираем их id
        new_sched_ids = []
        for lec_id in special_sample:
            cur.execute(
                "INSERT INTO schedule(id_lecture, id_group, timestamp, location) "
                "VALUES (%s, %s, %s, %s) RETURNING id;",
                (lec_id, target_group_id, base_datetime, f"Спец-Ауд-{random.randint(1,5)}")
            )
            new_sched_ids.append(cur.fetchone()[0])
        update_progress(op_special, 80)

        # получаем всех студентов группы
        cur.execute("SELECT student_number FROM student WHERE id_group = %s;", (target_group_id,))
        student_numbers = [row[0] for row in cur.fetchall()]
        
        update_progress(op_special, 90)

        # для каждого студента вставляем 1 или 2 записи attendance
        attendance_batch = []
        for stu_num in student_numbers:
            # случайно выбираем, сколько сессий у этого студента: 1 или 2 (если спец-лекций 2)
            count_for_student = random.randint(1, len(new_sched_ids))
            chosen = random.sample(new_sched_ids, count_for_student)
            for sched_id in chosen:
                status = random.random() < 0.8
                attendance_batch.append((base_datetime, 
                                       (base_datetime - datetime.timedelta(days=base_datetime.weekday())).date(), 
                                       stu_num, sched_id, status))

        if attendance_batch:
            values = ','.join(cur.mogrify("(%s, %s, %s, %s, %s)", row).decode('utf-8') for row in attendance_batch)
            sql_stmt = f"INSERT INTO attendance (timestamp, week_start, id_student, id_schedule, status) VALUES {values}"
            cur.execute(sql_stmt)
            conn.commit()
                
        update_progress(op_special, 100)
    
    complete_operation(op_special)
    update_progress(op_main, 100)
    complete_operation(op_main)
    cur.close()

##########################################################################
# MongoDB: Заполнение коллекции «universities» вложенной структурой
##########################################################################

def populate_mongodb(pg_conn):
    """
    Извлекает из PostgreSQL данные по университетам, институтам и кафедрам,
    формирует вложенную структуру и загружает в MongoDB.
    """
    op_mongo = start_operation("Заполнение MongoDB", 100)
    
    cur = pg_conn.cursor()
    
    # Получаем иерархию: университеты -> институты -> кафедры
    info("Извлечение данных из PostgreSQL...")
    update_progress(op_mongo, 10)
    
    cur.execute("""
        SELECT u.id, u.name, i.id, i.name, d.id, d.name
        FROM university u
        JOIN institute i ON i.id_university = u.id
        JOIN department d ON d.id_institute = i.id
        ORDER BY u.id, i.id, d.id;
    """)
    rows = cur.fetchall()
    update_progress(op_mongo, 20)
    
    info(f"Получено {len(rows)} записей из PostgreSQL")
    cur.close()

    # Строим иерархию данных без групп
    info("Построение иерархических документов для MongoDB...")
    update_progress(op_mongo, 30)
    
    mongo_data = {}
    universities_count = 0
    institutes_count = 0
    departments_count = 0
    
    for u_id, u_name, i_id, i_name, d_id, d_name in rows:
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
                "departmentId": int(d_id),
                "name": d_name
            }
            departments_count += 1
    
    update_progress(op_mongo, 50)

    # Преобразуем словари в списки для финального JSON
    info("Преобразование структуры данных для записи в MongoDB...")
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
    
    update_progress(op_mongo, 70)

    # Записываем в MongoDB
    info(f"Загрузка {len(documents)} университетов в MongoDB...")
    client = MongoClient(MONGO_CONN_STRING)
    db = client["university"]
    collection = db["universities"]
    
    # Очищаем коллекцию перед загрузкой
    info("Очистка существующей коллекции в MongoDB...")
    collection.delete_many({})
    update_progress(op_mongo, 80)
    
    if documents:
        collection.insert_many(documents)
        info(f"Загружено {len(documents)} документов в MongoDB")
    
    update_progress(op_mongo, 100)
    complete_operation(op_mongo)

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
    op_neo4j = start_operation("Заполнение Neo4j", 100)
    
    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    with driver.session() as session:
        # Очищаем базу Neo4j
        info("Очистка существующих данных в Neo4j...")
        session.run("MATCH (n) DETACH DELETE n")
        update_progress(op_neo4j, 10)
        
        # 1. Создаем узлы Department
        info("Извлечение данных о кафедрах из PostgreSQL...")
        cur = pg_conn.cursor()
        cur.execute("SELECT id, name FROM department;")
        depts = cur.fetchall()
        cur.close()
        
        dept_nodes = [{"id": d[0], "name": d[1], "neo_id": f"neo_dept_{d[0]}"} for d in depts]
        session.run(
            "UNWIND $nodes AS node CREATE (d:Department {id: node.id, name: node.name, neo_id: node.neo_id})",
            {"nodes": dept_nodes}
        )
        info(f"Создано {len(dept_nodes)} узлов Department")
        update_progress(op_neo4j, 30)
        
        # 2. Создаем узлы Lecture и связи ORIGINATES_FROM
        cur = pg_conn.cursor()
        cur.execute(
            """
            SELECT l.id, l.name, c.id_department
            FROM lecture l JOIN course c ON l.id_course = c.id;
            """
        )
        lectures = cur.fetchall()
        cur.close()
        
        lecture_nodes = [{"id": lec[0], "name": lec[1]} for lec in lectures]
        session.run(
            "UNWIND $nodes AS node CREATE (l:Lecture {id: node.id, name: node.name})",
            {"nodes": lecture_nodes}
        )
        
        lecture_relations_op = start_operation("Создание связей для лекций", len(lectures))
        for i, lec in enumerate(lectures):
            lec_id, _, dept_id = lec
            session.run(
                "MATCH (l:Lecture {id: $lec_id}), (d:Department {id: $dept_id}) CREATE (l)-[:ORIGINATES_FROM]->(d)",
                {"lec_id": lec_id, "dept_id": dept_id}
            )
            update_progress(lecture_relations_op, i+1)
        complete_operation(lecture_relations_op)
        update_progress(op_neo4j, 50)
        
        # 3. Создаем узлы Group
        cur = pg_conn.cursor()
        cur.execute("SELECT id, name, mongo_id FROM groups;")
        groups = cur.fetchall()
        cur.close()
        
        group_nodes = [{"id": g[0], "name": g[1], "mongo_id": g[2]} for g in groups]
        session.run(
            "UNWIND $nodes AS node CREATE (g:Group {id: node.id, name: node.name, mongo_id: node.mongo_id})",
            {"nodes": group_nodes}
        )
        info(f"Создано {len(group_nodes)} узлов Group")
        update_progress(op_neo4j, 60)
        
        # 4. Создаем узлы Student и связи BELONGS_TO
        cur = pg_conn.cursor()
        cur.execute("SELECT student_number, fullname, redis_key, id_group FROM student;")
        students = cur.fetchall()
        cur.close()
        
        student_nodes = [{"student_number": s[0], "fullname": s[1], "redis_key": s[2], "id_group": s[3]} for s in students]
        batch_size = 1000
        
        student_batch_op = start_operation("Создание узлов Student", len(student_nodes))
        for i in range(0, len(student_nodes), batch_size):
            batch = student_nodes[i:i+batch_size]
            session.run(
                "UNWIND $nodes AS node CREATE (st:Student {student_number: node.student_number, fullname: node.fullname, redis_key: node.redis_key})",
                {"nodes": batch}
            )
            update_progress(student_batch_op, min(i+batch_size, len(student_nodes)))
        complete_operation(student_batch_op)
        
        student_relations_op = start_operation("Создание связей для студентов", len(student_nodes))
        for i, s in enumerate(student_nodes):
            session.run(
                "MATCH (st:Student {student_number: $student_number}), (g:Group {id: $group_id}) CREATE (st)-[:BELONGS_TO]->(g)",
                {"student_number": s["student_number"], "group_id": s["id_group"]}
            )
            if (i+1) % batch_size == 0 or i+1 == len(student_nodes):
                update_progress(student_relations_op, i+1)
        complete_operation(student_relations_op)
        
        # 5. Создаем отношения HAS_SCHEDULE
        cur = pg_conn.cursor()
        cur.execute("SELECT id_group, id_lecture, timestamp, location FROM schedule;")
        schedule_pairs = cur.fetchall()
        cur.close()
        
        schedule_op = start_operation("Создание отношений HAS_SCHEDULE", len(schedule_pairs))
        for i, (group_id, lecture_id, timestamp, location) in enumerate(schedule_pairs):
            ts = timestamp.replace(tzinfo=None).isoformat()
            session.run(
                "MATCH (g:Group {id: $group_id}), (l:Lecture {id: $lecture_id}) "
                + "CREATE (g)-[:HAS_SCHEDULE {date: datetime($timestamp), location: $location}]->(l)",
                {"group_id": group_id, "lecture_id": lecture_id, "timestamp": ts, "location": location}
            )
            if (i+1) % 100 == 0 or i+1 == len(schedule_pairs):
                update_progress(schedule_op, i+1)
        complete_operation(schedule_op)
    
    driver.close()
    update_progress(op_neo4j, 100)
    complete_operation(op_neo4j)

##########################################################################
# Redis: Запись данных о студентах в виде hash
##########################################################################

def populate_redis(pg_conn):
    """
    Извлекает данные по студентам из PostgreSQL и сохраняет их в Redis.
    Ключ – redis_key, значения – поля fullname, email, group_id, group_name.
    """
    op_redis = start_operation("Заполнение Redis", 100)
    
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    # Очистка Redis перед заполнением
    info("Очистка существующих данных в Redis...")
    r.flushdb()
    update_progress(op_redis, 10)
    
    # Получаем информацию о студентах включая название группы
    info("Извлечение данных о студентах из PostgreSQL...")
    cur = pg_conn.cursor()
    cur.execute("""
        SELECT s.student_number, s.fullname, s.email, s.id_group, g.name as group_name, s.redis_key 
        FROM student s
        JOIN groups g ON s.id_group = g.id
    """)
    students = cur.fetchall()
    info(f"Получено {len(students)} записей о студентах")
    cur.close()
    update_progress(op_redis, 30)

    # Заполняем Redis
    info(f"Загрузка {len(students)} записей в Redis...")
    records_processed = 0
    
    student_load_op = start_operation("Загрузка студентов в Redis", len(students))
    
    for i, stud in enumerate(students):
        student_number, fullname, email, id_group, group_name, redis_key = stud
        r.hset(redis_key, mapping={
            "fullname": fullname,
            "email": email,
            "group_id": id_group,
            "group_name": group_name,
            "redis_key": redis_key
        })
        records_processed += 1
        
        if (i+1) % 1000 == 0 or i+1 == len(students):
            update_progress(student_load_op, i+1)
            update_progress(op_redis, 30 + int(70 * (i+1) / len(students)))
    
    complete_operation(student_load_op)
    update_progress(op_redis, 100)
    complete_operation(op_redis)

##########################################################################
# Elasticsearch: Индексирование лекций
##########################################################################

def populate_elasticsearch(pg_conn):
    """
    Извлекает данные по лекциям из PostgreSQL и индексирует их в Elasticsearch.
    Каждый документ включает id, имя лекции, подробное описание и дату создания.
    """
    op_elastic = start_operation("Заполнение Elasticsearch", 100)
    
    # Подключаемся к Elasticsearch
    info("Подключение к Elasticsearch...")
    es = Elasticsearch(ES_HOSTS)
    update_progress(op_elastic, 10)
    
    # Получаем лекции с информацией о курсе
    info("Извлечение данных о лекциях из PostgreSQL...")
    cur = pg_conn.cursor()
    cur.execute("""
        SELECT l.id, l.name, c.name as course_name, l.tech_equipment, l.created_at 
        FROM lecture l
        JOIN course c ON l.id_course = c.id
    """)
    lectures = cur.fetchall()
    info(f"Получено {len(lectures)} лекций")
    cur.close()
    update_progress(op_elastic, 20)

    # Удаляем индекс, если существует, и создаем новый
    if es.indices.exists(index="lectures"):
        info("Удаление существующего индекса 'lectures'...")
        es.indices.delete(index="lectures")
    
    # Создаем индекс с маппингом
    info("Создание нового индекса 'lectures' с маппингом...")
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
    update_progress(op_elastic, 30)

    # Генерация тематических описаний для лекций на основе их названий
    info("Подготовка шаблонов описаний лекций...")
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
    update_progress(op_elastic, 40)

    # Индексируем лекции
    info(f"Индексация {len(lectures)} лекций в Elasticsearch...")
    
    lecture_index_op = start_operation("Индексация лекций", len(lectures))
    
    for i, lec in enumerate(lectures):
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
        
        if (i+1) % 50 == 0 or i+1 == len(lectures):
            update_progress(lecture_index_op, i+1)
            update_progress(op_elastic, 40 + int(60 * (i+1) / len(lectures)))
    
    complete_operation(lecture_index_op)
    update_progress(op_elastic, 100)
    complete_operation(op_elastic)

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
    op_update = start_operation("Обновление идентификаторов в PostgreSQL", 100)
    
    cur = pg_conn.cursor()
    
    info("Обновление mongo_id для групп...")
    update_progress(op_update, 20)
    cur.execute("UPDATE groups SET mongo_id = 'mongo_group_' || id;")
    
    info("Обновление elasticsearch_id для лекций...")
    update_progress(op_update, 40)
    cur.execute("UPDATE lecture SET elasticsearch_id = 'elastic_lecture_' || id;")
    
    info("Добавление и обновление neo_id для кафедр...")
    update_progress(op_update, 60)
    # Проверяем существование колонки neo_id
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'department' AND column_name = 'neo_id';
    """)
    column_exists = cur.fetchone()
    
    if not column_exists:
        info("Добавление колонки neo_id в таблицу department...")
        cur.execute("ALTER TABLE department ADD COLUMN neo_id VARCHAR(100);")
    
    cur.execute("UPDATE department SET neo_id = 'neo_dept_' || id;")
    update_progress(op_update, 80)
    
    pg_conn.commit()
    cur.close()
    
    update_progress(op_update, 100)
    complete_operation(op_update)

##########################################################################
# Основной запуск: заполнение всех БД и обновление id в Postgres
##########################################################################

def main():
    info("=== Начало процесса генерации данных ===")
    
    # Подключение к PostgreSQL
    try:
        info("Подключение к PostgreSQL...")
        pg_conn = psycopg2.connect(**PG_CONN_PARAMS)
        info("Подключение к PostgreSQL успешно установлено")
    except Exception as e:
        error(f"Ошибка подключения к PostgreSQL: {e}")
        return

    # Создаем схему и заполняем PostgreSQL
    try:
        info("=== Этап 1: Создание и заполнение PostgreSQL ===")
        create_postgres_schema(pg_conn)
        populate_postgres(pg_conn)
    except Exception as e:
        error(f"Ошибка при создании и заполнении PostgreSQL: {e}")
        pg_conn.close()
        return

    # Выводим количество записей в таблицах для контроля
    info("Проверка количества созданных записей в PostgreSQL:")
    check_tables = [
        "university", "institute", "department", "groups", 
        "student", "course", "lecture", "schedule", "attendance"
    ]
    
    op_check = start_operation("Проверка количества записей", len(check_tables))
    
    cur = pg_conn.cursor()
    total_records = 0
    
    for i, tbl in enumerate(check_tables):
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(tbl)))
        count = cur.fetchone()[0]
        total_records += count
        info(f"Таблица {tbl}: {count} записей")
        update_progress(op_check, i+1)
    
    cur.close()
    complete_operation(op_check)
    info(f"Всего в PostgreSQL создано {total_records} записей")

    # Заполнение внешних БД
    try:
        info("=== Этап 2: Заполнение MongoDB ===")
        populate_mongodb(pg_conn)
    except Exception as e:
        error(f"Ошибка при заполнении MongoDB: {e}")
    
    try:
        info("=== Этап 3: Заполнение Neo4j ===")
        populate_neo4j(pg_conn)
    except Exception as e:
        error(f"Ошибка при заполнении Neo4j: {e}")
    
    try:
        info("=== Этап 4: Заполнение Redis ===")
        populate_redis(pg_conn)
    except Exception as e:
        error(f"Ошибка при заполнении Redis: {e}")
    
    try:
        info("=== Этап 5: Заполнение Elasticsearch ===")
        populate_elasticsearch(pg_conn)
    except Exception as e:
        error(f"Ошибка при заполнении Elasticsearch: {e}")

    # Обновляем идентификаторы во внешних системах в Postgres
    try:
        info("=== Этап 6: Обновление идентификаторов в PostgreSQL ===")
        update_postgres_ids(pg_conn)
    except Exception as e:
        error(f"Ошибка при обновлении идентификаторов: {e}")

    pg_conn.close()
    
    info("=== Процесс генерации данных завершен ===")
    
    # Если все прошло успешно, выводим итоговое сообщение
    if total_records > 30000:
        info(f"УСПЕХ! Сгенерировано более 30000 записей ({total_records})")
    else:
        info(f"ВНИМАНИЕ! Сгенерировано менее 30000 записей ({total_records})")
        
    # Показываем финальную сводку
    show_summary()

if __name__ == "__main__":
    main()