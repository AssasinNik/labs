import random
import datetime

import psycopg2
from psycopg2 import sql
from faker import Faker

from pymongo import MongoClient
from neo4j import GraphDatabase
import redis
from elasticsearch import Elasticsearch

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

##########################################################################
# PostgreSQL: Создание схемы с партиционированием таблицы attendance
##########################################################################

def create_postgres_schema(conn):
    """
    Пересоздаёт схему PostgreSQL для таблиц:
    attendance, schedule, lecture, course, student, groups, department, institute, university.
    Создаются партиции для attendance на период с сентября 2023 до февраля 2024.
    """
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
    for table in tables:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(table)))
    conn.commit()

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
    print("Схема PostgreSQL создана (таблицы и партиции attendance обновлены).")

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
    cur = conn.cursor()

    # Задаем параметры генерации
    num_universities = 3
    institutes_per_univ = 4
    departments_per_inst = 5
    groups_per_department = 5
    students_per_group = 100    # Итог: 3 * 4 * 5 * 5 * 100 ≈ 30000 студентов
    courses_per_department = 10
    lectures_per_course = 3

    # Базовое время для расписания лекций
    base_datetime = datetime.datetime(2023, 9, 4, 9, 0, 0)

    # Списки для контроля вставки
    universities = []    # (id, name)
    institutes = {}      # {uni_id: [(inst_id, name), ...]}
    departments = {}     # {inst_id: [(dept_id, name), ...]}
    groups = {}          # {dept_id: [(group_id, name), ...]}

    # 1. Университеты
    for i in range(num_universities):
        uni_name = fake.company()
        cur.execute("INSERT INTO university(name) VALUES (%s) RETURNING id;", (uni_name,))
        uni_id = cur.fetchone()[0]
        universities.append((uni_id, uni_name))
    conn.commit()
    print("Университеты созданы.")

    # 2. Институты → Кафедры → Группы → Студенты
    for uni_id, uni_name in universities:
        institutes[uni_id] = []
        for j in range(institutes_per_univ):
            inst_name = f"Институт {fake.word().capitalize()}"
            cur.execute("INSERT INTO institute(name, id_university) VALUES (%s, %s) RETURNING id;", (inst_name, uni_id))
            inst_id = cur.fetchone()[0]
            institutes[uni_id].append((inst_id, inst_name))
            # Для каждого института создаем кафедры
            departments[inst_id] = []
            for k in range(departments_per_inst):
                dept_name = f"Кафедра {fake.job().split()[0]}"
                cur.execute("INSERT INTO department(name, id_institute) VALUES (%s, %s) RETURNING id;", (dept_name, inst_id))
                dept_id = cur.fetchone()[0]
                departments[inst_id].append((dept_id, dept_name))
                # Для каждой кафедры создаем группы
                groups[dept_id] = []
                for g in range(groups_per_department):
                    group_name = f"БСБО-{random.randint(10, 99)}-{random.randint(10, 99)}"
                    formation_year = random.randint(2015, 2023)
                    cur.execute("INSERT INTO groups(name, id_department, formation_year) VALUES (%s, %s, %s) RETURNING id;",
                                (group_name, dept_id, formation_year))
                    group_id = cur.fetchone()[0]
                    groups[dept_id].append((group_id, group_name))
                    # Для каждой группы создаются студенты
                    for s in range(students_per_group):
                        student_number = f"S{uni_id}{inst_id}{dept_id}{group_id}{s:04d}"
                        fullname = fake.name()
                        email = fake.email()
                        redis_key = f"student:{student_number}"
                        cur.execute("INSERT INTO student(student_number, fullname, email, id_group, redis_key) VALUES (%s, %s, %s, %s, %s);",
                                    (student_number, fullname, email, group_id, redis_key))
    conn.commit()
    print("Институты, кафедры, группы и студенты созданы.")

    # 3. Курсы, лекции, расписание и посещаемость
    for inst_id, depts in departments.items():
        for dept_id, dept_name in depts:
            for c in range(courses_per_department):
                course_name = f"Курс {fake.bs().capitalize()}"
                cur.execute("INSERT INTO course(name, id_department) VALUES (%s, %s) RETURNING id;", (course_name, dept_id))
                course_id = cur.fetchone()[0]
                # Для курса создаются лекции
                for l in range(lectures_per_course):
                    lecture_name = f"Лекция {fake.catch_phrase()}"
                    tech_equipment = random.choice([True, False])
                    cur.execute("INSERT INTO lecture(name, duration_hours, tech_equipment, id_course) VALUES (%s, %s, %s, %s) RETURNING id;",
                                (lecture_name, 2, tech_equipment, course_id))
                    lecture_id = cur.fetchone()[0]
                    # Для каждой лекции создаём расписание для всех групп данной кафедры
                    cur.execute("SELECT id FROM groups WHERE id_department = %s;", (dept_id,))
                    dept_groups = cur.fetchall()
                    for group in dept_groups:
                        group_id = group[0]
                        schedule_time = base_datetime + datetime.timedelta(weeks=random.randint(0, 15))
                        location = f"Аудитория {random.randint(100, 500)}"
                        cur.execute("INSERT INTO schedule(id_lecture, id_group, timestamp, location) VALUES (%s, %s, %s, %s) RETURNING id;",
                                    (lecture_id, group_id, schedule_time, location))
                        schedule_id = cur.fetchone()[0]
                        # Создаем записи посещаемости для студентов группы (вероятность 10%)
                        cur.execute("SELECT student_number FROM student WHERE id_group = %s;", (group_id,))
                        student_numbers = [row[0] for row in cur.fetchall()]
                        for stud_num in student_numbers:
                            if random.random() < 0.1:
                                week_start = schedule_time - datetime.timedelta(days=schedule_time.weekday())
                                cur.execute(
                                    "INSERT INTO attendance(timestamp, week_start, id_student, id_schedule, status) VALUES (%s, %s, %s, %s, %s);",
                                    (schedule_time, week_start, stud_num, schedule_id, True)
                                )
    conn.commit()
    cur.close()
    print("Курсы, лекции, расписание и посещаемость созданы.")

##########################################################################
# MongoDB: Заполнение коллекции «universities» вложенной структурой
##########################################################################

def populate_mongodb(pg_conn):
    """
    Извлекает из PostgreSQL данные по университетам, институтам и кафедрам,
    формирует вложенную структуру и загружает в MongoDB.
    """
    cur = pg_conn.cursor()
    cur.execute("""
        SELECT u.id, u.name, i.id, i.name, d.id, d.name 
        FROM university u
        JOIN institute i ON i.id_university = u.id
        JOIN department d ON d.id_institute = i.id;
    """)
    rows = cur.fetchall()
    cur.close()

    mongo_data = {}
    for u_id, u_name, i_id, i_name, d_id, d_name in rows:
        if u_id not in mongo_data:
            mongo_data[u_id] = {
                "id": u_id,
                "name": u_name,
                "institutes": {}
            }
        if i_id not in mongo_data[u_id]["institutes"]:
            mongo_data[u_id]["institutes"][i_id] = {
                "id": i_id,
                "name": i_name,
                "departments": []
            }
        mongo_data[u_id]["institutes"][i_id]["departments"].append({
            "id": d_id,
            "name": d_name
        })

    documents = []
    for uni in mongo_data.values():
        uni["institutes"] = list(uni["institutes"].values())
        documents.append(uni)

    client = MongoClient(MONGO_CONN_STRING)
    db = client["university"]
    collection = db["universities"]
    collection.delete_many({})  # очищаем коллекцию перед загрузкой
    if documents:
        collection.insert_many(documents)
    print("Данные MongoDB заполнены.")

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
           - (Group)-[:HAS_SCHEDULE]->(Lecture) (на основании уникальных пар из schedule)
           - (Student)-[:BELONGS_TO]->(Group)
    """
    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    with driver.session() as session:
        # Очищаем базу Neo4j
        session.run("MATCH (n) DETACH DELETE n")
        
        # 1. Создаем узлы Department
        cur = pg_conn.cursor()
        cur.execute("SELECT id, name FROM department;")
        depts = cur.fetchall()  # (id, name)
        cur.close()
        dept_nodes = [{"id": d[0], "name": d[1], "neo_id": f"neo_dept_{d[0]}"} for d in depts]
        session.run("UNWIND $nodes AS node CREATE (d:Department {id: node.id, name: node.name, neo_id: node.neo_id})", nodes=dept_nodes)
        
        # 2. Создаем узлы Lecture и связываем с Department
        cur = pg_conn.cursor()
        cur.execute("""
            SELECT l.id, l.name, c.id_department
            FROM lecture l JOIN course c ON l.id_course = c.id;
        """)
        lectures = cur.fetchall()  # (lecture_id, name, dept_id)
        cur.close()
        lecture_nodes = [{"id": lec[0], "name": lec[1]} for lec in lectures]
        session.run("UNWIND $nodes AS node CREATE (l:Lecture {id: node.id, name: node.name})", nodes=lecture_nodes)
        for lec in lectures:
            lec_id, _, dept_id = lec
            session.run("""
                MATCH (l:Lecture {id: $lec_id}), (d:Department {id: $dept_id})
                CREATE (l)-[:ORIGINATES_FROM]->(d)
            """, lec_id=lec_id, dept_id=dept_id)
        
        # 3. Создаем узлы Group
        cur = pg_conn.cursor()
        cur.execute("SELECT id, name, mongo_id FROM groups;")
        groups = cur.fetchall()  # (id, name, mongo_id)
        cur.close()
        group_nodes = [{"id": g[0], "name": g[1], "mongo_id": g[2]} for g in groups]
        session.run("UNWIND $nodes AS node CREATE (g:Group {id: node.id, name: node.name, mongo_id: node.mongo_id})", nodes=group_nodes)
        
        # 4. Создаем узлы Student и связываем с Group (batch-режим для 30000+ записей)
        cur = pg_conn.cursor()
        cur.execute("SELECT student_number, fullname, redis_key, id_group FROM student;")
        students = cur.fetchall()  # (student_number, fullname, redis_key, group_id)
        cur.close()
        student_nodes = [{"student_number": s[0], "fullname": s[1], "redis_key": s[2], "id_group": s[3]} for s in students]
        batch_size = 1000
        for i in range(0, len(student_nodes), batch_size):
            batch = student_nodes[i:i+batch_size]
            session.run("""
                UNWIND $nodes AS node
                CREATE (st:Student {student_number: node.student_number, fullname: node.fullname, redis_key: node.redis_key})
            """, nodes=batch)
        # Создаем отношения (Student)-[:BELONGS_TO]->(Group)
        for s in student_nodes:
            session.run("""
                MATCH (st:Student {student_number: $student_number}), (g:Group {id: $group_id})
                CREATE (st)-[:BELONGS_TO]->(g)
            """, student_number=s["student_number"], group_id=s["id_group"])
        
        # 5. Создаем отношения (Group)-[:HAS_SCHEDULE]->(Lecture) на основании расписания
        cur = pg_conn.cursor()
        cur.execute("SELECT DISTINCT id_group, id_lecture FROM schedule;")
        schedule_pairs = cur.fetchall()  # (group_id, lecture_id)
        cur.close()
        for pair in schedule_pairs:
            group_id, lecture_id = pair
            session.run("""
                MATCH (g:Group {id: $group_id}), (l:Lecture {id: $lecture_id})
                CREATE (g)-[:HAS_SCHEDULE]->(l)
            """, group_id=group_id, lecture_id=lecture_id)
    driver.close()
    print("Neo4j заполнен полностью данными из Postgres.")

##########################################################################
# Redis: Запись данных о студентах в виде hash
##########################################################################

def populate_redis(pg_conn):
    """
    Извлекает данные по студентам из PostgreSQL и сохраняет их в Redis.
    Ключ – redis_key, значения – поля fullname, email, group_id.
    """
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    cur = pg_conn.cursor()
    cur.execute("SELECT student_number, fullname, email, id_group, redis_key FROM student;")
    students = cur.fetchall()
    cur.close()

    r.flushdb()  # очистка Redis для тестовой среды

    for stud in students:
        _, fullname, email, id_group, redis_key = stud
        r.hset(redis_key, mapping={
            "fullname": fullname,
            "email": email,
            "group_id": id_group,
            "redis_key": redis_key
        })
    print("Данные Redis заполнены.")

##########################################################################
# Elasticsearch: Индексирование лекций
##########################################################################

def populate_elasticsearch(pg_conn):
    """
    Извлекает данные по лекциям из PostgreSQL и индексирует их в Elasticsearch.
    Каждый документ включает id, имя лекции, описание и дату создания.
    """
    es = Elasticsearch(ES_HOSTS)
    cur = pg_conn.cursor()
    cur.execute("SELECT id, name, created_at FROM lecture;")
    lectures = cur.fetchall()
    cur.close()

    if es.indices.exists(index="lectures"):
        es.indices.delete(index="lectures")
    es.indices.create(index="lectures", ignore=400)

    for lec in lectures:
        lec_id, name, created_at = lec
        doc = {
            "id": lec_id,
            "name": name,
            "description": fake.text(max_nb_chars=100),
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "lecture_id": lec_id
        }
        es.index(index="lectures", id=lec_id, body=doc)
    print("Данные Elasticsearch заполнены.")

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
    cur = pg_conn.cursor()
    cur.execute("UPDATE groups SET mongo_id = 'mongo_group_' || id;")
    cur.execute("UPDATE lecture SET elasticsearch_id = 'elastic_lecture_' || id;")
    cur.execute("ALTER TABLE department ADD COLUMN IF NOT EXISTS neo_id VARCHAR(100);")
    cur.execute("UPDATE department SET neo_id = 'neo_dept_' || id;")
    pg_conn.commit()
    cur.close()
    print("Postgres обновлён: mongo_id, elasticsearch_id и neo_id заполнены.")

##########################################################################
# Основной запуск: заполнение всех БД и обновление id в Postgres
##########################################################################

def main():
    try:
        pg_conn = psycopg2.connect(**PG_CONN_PARAMS)
        print("Подключение к PostgreSQL успешно установлено.")
    except Exception as e:
        print(f"Ошибка подключения к PostgreSQL: {e}")
        return

    # Создаем схему и заполняем PostgreSQL
    create_postgres_schema(pg_conn)
    populate_postgres(pg_conn)

    # Выводим количество записей в таблицах для контроля
    check_tables = [
        "university", "institute", "department", "groups", 
        "student", "course", "lecture", "schedule", "attendance"
    ]
    cur = pg_conn.cursor()
    for tbl in check_tables:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(tbl)))
        count = cur.fetchone()[0]
        print(f"В таблице {tbl} записей: {count}")
    cur.close()

    # Заполнение внешних БД
    populate_mongodb(pg_conn)
    populate_neo4j(pg_conn)
    populate_redis(pg_conn)
    populate_elasticsearch(pg_conn)

    # Обновляем идентификаторы во внешних системах в Postgres
    update_postgres_ids(pg_conn)

    pg_conn.close()
    print("Все БД успешно заполнены и синхронизированы.")

if __name__ == "__main__":
    main()
