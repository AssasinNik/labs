import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor
from pymongo import MongoClient, errors as mongo_errors
import redis
from py2neo import Graph, Node, Relationship
from elasticsearch import Elasticsearch
from faker import Faker
import random
import datetime
import logging
import sys

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Конфигурация подключения к базам данных (согласно docker-compose)
PG_CONFIG = {
    'host': 'host.docker.internal',
    'port': 5433,
    'user': 'admin',
    'password': 'secret',
    'dbname': 'mydb'
}
MONGO_CONFIG = {
    'host': 'host.docker.internal',
    'port': 27017,
    'db': 'university_db'
}
REDIS_CONFIG = {
    'host': 'host.docker.internal',
    'port': 6379,
    'db': 0
}
NEO4J_CONFIG = {
    'uri': 'bolt://host.docker.internal:7687',
    'user': None,  # без аутентификации
    'password': None
}
ES_CONFIG = {
    'hosts': ['http://host.docker.internal:9200']
}

# Списки для генерации данных
UNIVERSITIES = [
    "МГУ", "СПбГУ", "МИРЭА", "МФТИ", "НИЮВГУ", "РТУ МИРЭА", "НИУ ВШЭ", "ИТМО", "РАНХиГС", "МИИРЭ"
]
INSTITUTE_NAMES = ["Институт информационных технологий", "Институт кибернетики", "Институт физики", "Институт математики"]
DEPARTMENT_NAMES = ["Кафедра программирования", "Кафедра прикладной математики", "Кафедра информационных систем"]
GROUP_PREFIXES = ["БСБО", "ИББО", "ДЭ", "КН"]
COURSE_NAMES = ["Алгоритмы", "Базы данных", "Контейнеризация", "Машинное обучение", "Операционные системы"]

fake = Faker("ru_RU")
Faker.seed(0)
random.seed(0)

def recreate_postgres_schema():
    """Очистка базы и создание таблиц по предоставленной схеме, включая партиционирование таблицы attendance по неделям."""
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()

        # Сначала удаляем таблицы, включая возможные партиции attendance
        tables = ["lecture_materials", "attendance", "groups", "department", "institute", "university", "course", "lecture", "schedule", "student"]
        for table in tables:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
        
        # Создаём основные таблицы согласно схеме
        schema_sql = """
        -- Основные таблицы
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

        -- Курсы и лекции
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

        -- Расписание
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

        -- Посещения (партиционированная таблица по неделям)
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

        -- Триггерная функция для заполнения week_start
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

        CREATE TABLE lecture_materials (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            id_lecture INT NOT NULL REFERENCES lecture(id)
        );
        """
        cur.execute(schema_sql)

        # Теперь создадим партиции для attendance по неделям.
        # Например, создадим партиции от (начало текущей недели - 10 недель) до (начало текущей недели + 2 недели)
        now_dt = datetime.datetime.now()
        start_week = now_dt - datetime.timedelta(days=now_dt.weekday())
        partition_start = start_week - datetime.timedelta(weeks=10)
        partition_end = start_week + datetime.timedelta(weeks=2)

        current = partition_start.date()
        end_date = partition_end.date()
        while current < end_date:
            next_week = current + datetime.timedelta(days=7)
            partition_name = f"attendance_{current.strftime('%Y%m%d')}"
            create_partition_sql = f"""
            CREATE TABLE {partition_name} PARTITION OF attendance
                FOR VALUES FROM ('{current}') TO ('{next_week}');
            """
            cur.execute(create_partition_sql)
            logger.info("Создана партиция %s для диапазона [%s, %s)", partition_name, current, next_week)
            current = next_week

        cur.close()
        conn.close()
        logger.info("PostgreSQL: Схема пересоздана.")
    except Exception as e:
        logger.error("Ошибка при пересоздании схемы PostgreSQL: %s", e)
        sys.exit(1)

def populate_postgres_data():
    """Генерация и вставка данных в PostgreSQL."""
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor()
        
        # 1. Университеты
        uni_ids = {}
        for uni in UNIVERSITIES:
            cur.execute("INSERT INTO university (name) VALUES (%s) RETURNING id;", (uni,))
            uni_id = cur.fetchone()[0]
            uni_ids[uni] = uni_id

        # 2. Институты для каждого университета (по 1-3 на универ)
        institute_ids = {}
        for uni, uni_id in uni_ids.items():
            n_institutes = random.randint(1, 3)
            for _ in range(n_institutes):
                inst_name = random.choice(INSTITUTE_NAMES) + " " + fake.word().capitalize()
                cur.execute("INSERT INTO institute (name, id_university) VALUES (%s, %s) RETURNING id;", (inst_name, uni_id))
                inst_id = cur.fetchone()[0]
                institute_ids[inst_id] = {'university': uni_id, 'name': inst_name}

        # 3. Кафедры для каждого института (1-3 кафедры)
        department_ids = {}
        for inst_id in institute_ids.keys():
            n_depts = random.randint(1, 3)
            for _ in range(n_depts):
                dept_name = random.choice(DEPARTMENT_NAMES) + " " + fake.word().capitalize()
                cur.execute("INSERT INTO department (name, id_institute) VALUES (%s, %s) RETURNING id;", (dept_name, inst_id))
                dept_id = cur.fetchone()[0]
                department_ids[dept_id] = {'institute': inst_id, 'name': dept_name}

        # 4. Группы для каждой кафедры (1-4 группы)
        group_ids = {}
        for dept_id in department_ids.keys():
            n_groups = random.randint(1, 4)
            for _ in range(n_groups):
                group_name = random.choice(GROUP_PREFIXES) + "-" + fake.bothify(text="??##")
                formation_year = random.choice(range(2015, 2023))
                cur.execute("INSERT INTO groups (name, id_department, formation_year) VALUES (%s, %s, %s) RETURNING id;", 
                            (group_name, dept_id, formation_year))
                group_id = cur.fetchone()[0]
                group_ids[group_id] = {'department': dept_id, 'name': group_name}

        # 5. Студенты (30 000 записей)
        student_records = []
        student_keys = []
        group_id_list = list(group_ids.keys())
        for i in range(30000):
            student_number = f"STU{i+1:05d}"
            fullname = fake.name()
            email = fake.email()
            id_group = random.choice(group_id_list)
            redis_key = None  # будет обновлён ниже
            student_records.append((student_number, fullname, email, id_group, redis_key))
            student_keys.append(student_number)
        execute_batch(cur, 
                      "INSERT INTO student (student_number, fullname, email, id_group, redis_key) VALUES (%s, %s, %s, %s, %s);", 
                      student_records, page_size=1000)
        
        # 6. Курсы для кафедр (1-3 курса на кафедру)
        course_ids = {}
        for dept_id in department_ids.keys():
            n_courses = random.randint(1, 3)
            for _ in range(n_courses):
                course_name = random.choice(COURSE_NAMES) + " " + fake.word().capitalize()
                cur.execute("INSERT INTO course (name, id_department) VALUES (%s, %s) RETURNING id;", (course_name, dept_id))
                course_id = cur.fetchone()[0]
                course_ids[course_id] = {'department': dept_id, 'name': course_name}
        
        # 7. Лекции для курсов (1-4 лекции на курс)
        lecture_ids = {}
        for course_id in course_ids.keys():
            n_lectures = random.randint(1, 4)
            for _ in range(n_lectures):
                lecture_name = "Лекция по " + fake.word().capitalize()
                tech_equipment = random.choice([True, False])
                cur.execute("INSERT INTO lecture (name, tech_equipment, id_course) VALUES (%s, %s, %s) RETURNING id;", 
                            (lecture_name, tech_equipment, course_id))
                lecture_id = cur.fetchone()[0]
                lecture_ids[lecture_id] = {'course': course_id, 'name': lecture_name}

        # 8. Расписание для лекций (каждая лекция - несколько записей, связываем с группами)
        schedule_ids = []
        for lecture_id in lecture_ids.keys():
            id_group = random.choice(group_id_list)
            for day_offset in range(0, 5):
                ts = datetime.datetime.now() + datetime.timedelta(days=day_offset * 7)
                location = "Аудитория " + str(random.randint(1, 300))
                cur.execute("INSERT INTO schedule (id_lecture, id_group, timestamp, location) VALUES (%s, %s, %s, %s) RETURNING id;",
                            (lecture_id, id_group, ts, location))
                sched_id = cur.fetchone()[0]
                schedule_ids.append(sched_id)

        # 9. Посещения: для каждой записи расписания создаём посещения для нескольких студентов
        for sched_id in schedule_ids:
            students_for_sched = random.sample(student_keys, 5)
            for stu in students_for_sched:
                ts = datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 30))
                # week_start вычисляется триггером, но можно задать и явно:
                week_start = ts - datetime.timedelta(days=ts.weekday())
                status = random.choice([True, False])
                cur.execute("INSERT INTO attendance (timestamp, week_start, id_student, id_schedule, status) VALUES (%s, %s, %s, %s, %s);",
                            (ts, week_start.date(), stu, sched_id, status))

        # 10. Материалы лекций (1-2 записи на лекцию)
        for lecture_id in lecture_ids.keys():
            n_materials = random.randint(1, 2)
            for _ in range(n_materials):
                material_name = "Материал " + fake.word().capitalize()
                description = fake.text(max_nb_chars=200)
                cur.execute("INSERT INTO lecture_materials (name, description, id_lecture) VALUES (%s, %s, %s);",
                            (material_name, description, lecture_id))

        # Обновление ключей в student и lecture
        cur.execute("UPDATE student SET redis_key = 'student:' || student_number;")
        cur.execute("UPDATE lecture SET elasticsearch_id = 'lecture:' || id;")
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("PostgreSQL: Данные успешно заполнены.")
    except Exception as e:
        logger.error("Ошибка при заполнении данных в PostgreSQL: %s", e)
        sys.exit(1)

def populate_mongodb():
    """
    Извлекаем из Postgres данные по университетам, институтам и кафедрам,
    формируя вложенную структуру для MongoDB с батчевой вставкой.
    """
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT id, name FROM university;")
        universities = cur.fetchall()

        for uni in universities:
            cur.execute("SELECT id, name FROM institute WHERE id_university = %s;", (uni['id'],))
            institutes = cur.fetchall()
            uni['institutes'] = []
            for inst in institutes:
                cur.execute("SELECT d.id, d.name FROM department d JOIN institute i ON d.id_institute = i.id WHERE i.id = %s;", (inst['id'],))
                departments = cur.fetchall()
                inst['departments'] = [{'id': dept['id'], 'name': dept['name']} for dept in departments]
                uni['institutes'].append({
                    'id': inst['id'],
                    'name': inst['name'],
                    'departments': inst['departments']
                })
        cur.close()
        conn.close()
    except Exception as e:
        logger.error("Ошибка при выборке данных для MongoDB: %s", e)
        sys.exit(1)
    
    try:
        client = MongoClient(MONGO_CONFIG['host'], MONGO_CONFIG['port'])
        db = client[MONGO_CONFIG['db']]
        db.universities.drop()
        if universities:
            # Оптимизированная батчевая вставка
            db.universities.insert_many(universities, ordered=False)
        logger.info("MongoDB: Данные из PostgreSQL скопированы в коллекцию universities.")
    except mongo_errors.PyMongoError as e:
        logger.error("Ошибка при вставке данных в MongoDB: %s", e)
        sys.exit(1)

def populate_redis(batch_size=1000):
    """
    Извлекаем все данные студентов из Postgres с постраничной выборкой и записываем их в Redis.
    Используем server-side курсор и fetchmany для обработки больших объёмов.
    """
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        query = """
            SELECT s.student_number, s.fullname, s.email, s.redis_key, g.id AS group_id, g.name AS group_name
            FROM student s
            JOIN groups g ON s.id_group = g.id
        """
        cur.execute(query)
        r = redis.Redis(host=REDIS_CONFIG['host'], port=REDIS_CONFIG['port'], db=REDIS_CONFIG['db'])
        r.flushdb()
        total = 0
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            for stu in rows:
                key = stu['redis_key']
                student_data = {
                    "fullname": stu['fullname'],
                    "email": stu['email'],
                    "group_id": stu['group_id'],
                    "group_name": stu['group_name'],
                    "redis_key": key
                }
                r.hmset(key, student_data)
                total += 1
        cur.close()
        conn.close()
        logger.info("Redis: %d записей студентов перенесены в Redis.", total)
    except Exception as e:
        logger.error("Ошибка при переносе данных в Redis: %s", e)
        sys.exit(1)

def populate_neo4j(batch_size=1000):
    """
    Извлекаем данные для графа из Postgres с постраничной выборкой.
    Формируем граф: Student -[BELONGS_TO]-> Group -[HAS_SCHEDULE]-> Lecture -[ORIGINATES_FROM]-> Department.
    """
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Извлекаем группы
        cur.execute("SELECT id, name FROM groups;")
        groups = {row['id']: row for row in cur.fetchall()}
        
        # Создаем словарь групп для Neo4j
        neo_groups = {}
        graph = Graph(NEO4J_CONFIG['uri'], auth=None)
        graph.delete_all()
        for grp in groups.values():
            node = Node("Group", group_id=grp['id'], name=grp['name'])
            graph.merge(node, "Group", "group_id")
            neo_groups[grp['id']] = node

        # Обрабатываем студентов постранично
        cur.execute("SELECT student_number, fullname, redis_key, id_group FROM student;")
        total_students = 0
        while True:
            students = cur.fetchmany(batch_size)
            if not students:
                break
            for stu in students:
                stu_node = Node("Student", student_number=stu['student_number'], name=stu['fullname'])
                graph.merge(stu_node, "Student", "student_number")
                grp_node = neo_groups.get(stu['id_group'])
                if grp_node:
                    rel = Relationship(stu_node, "BELONGS_TO", grp_node)
                    graph.merge(rel)
                total_students += 1

        # Извлекаем расписания для связи групп с лекциями
        cur.execute("SELECT DISTINCT id_group, id_lecture FROM schedule;")
        schedule_links = []
        while True:
            links = cur.fetchmany(batch_size)
            if not links:
                break
            schedule_links.extend(links)

        # Извлекаем лекции с данными кафедры
        cur.execute("""
            SELECT l.id AS lecture_id, l.name AS lecture_name, d.id AS department_id, d.name AS department_name
            FROM lecture l
            JOIN course c ON l.id_course = c.id
            JOIN department d ON c.id_department = d.id;
        """)
        lectures = {}
        for row in cur.fetchall():
            lectures[row['lecture_id']] = {
                "lecture_name": row['lecture_name'],
                "department_id": row['department_id'],
                "department_name": row['department_name']
            }
        cur.close()
        conn.close()

        # Создаем узлы лекций и отделов, устанавливая связи
        neo_lectures = {}
        neo_departments = {}
        for link in schedule_links:
            grp_id = link['id_group']
            lec_id = link['id_lecture']
            if lec_id not in lectures:
                continue
            lec_info = lectures[lec_id]
            if lec_id not in neo_lectures:
                lec_node = Node("Lecture", lecture_id=lec_id, name=lec_info['lecture_name'])
                graph.merge(lec_node, "Lecture", "lecture_id")
                neo_lectures[lec_id] = lec_node
            else:
                lec_node = neo_lectures[lec_id]
            dep_id = lec_info['department_id']
            if dep_id not in neo_departments:
                dep_node = Node("Department", department_id=dep_id, name=lec_info['department_name'])
                graph.merge(dep_node, "Department", "department_id")
                neo_departments[dep_id] = dep_node
            else:
                dep_node = neo_departments[dep_id]
            rel_dep = Relationship(lec_node, "ORIGINATES_FROM", dep_node)
            graph.merge(rel_dep)
            grp_node = neo_groups.get(grp_id)
            if grp_node:
                rel_sched = Relationship(grp_node, "HAS_SCHEDULE", lec_node)
                graph.merge(rel_sched)
        logger.info("Neo4j: Данные успешно перенесены в графовую базу.")
    except Exception as e:
        logger.error("Ошибка при переносе данных в Neo4j: %s", e)
        sys.exit(1)

def populate_elasticsearch(batch_size=1000):
    """
    Извлекаем данные лекций с постраничной выборкой и индексируем их в Elasticsearch.
    Для каждой лекции используем первое описание из lecture_materials (если имеется).
    """
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT l.id, l.name, l.elasticsearch_id, l.created_at,
                   (SELECT lm.description FROM lecture_materials lm WHERE lm.id_lecture = l.id LIMIT 1) AS description
            FROM lecture l;
        """)
        es = Elasticsearch(ES_CONFIG['hosts'])
        index_name = "lectures"
        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)
        es.indices.create(index=index_name)
        total = 0
        while True:
            lectures = cur.fetchmany(batch_size)
            if not lectures:
                break
            for lec in lectures:
                doc = {
                    "id": lec["id"],
                    "name": lec["name"],
                    "description": lec["description"] if lec["description"] else "",
                    "created_at": lec["created_at"].strftime("%Y-%m-%d %H:%M:%S"),
                    "lecture_id": lec["id"]
                }
                es.index(index=index_name, id=lec["id"], document=doc)
                total += 1
        cur.close()
        conn.close()
        logger.info("Elasticsearch: %d записей лекций перенесены в индекс '%s'.", total, index_name)
    except (psycopg2.Error) as e:
        logger.error("Ошибка при переносе данных в Elasticsearch: %s", e)
        sys.exit(1)

def main():
    logger.info("Начало работы скрипта.")
    recreate_postgres_schema()
    
    logger.info("Заполнение PostgreSQL данными...")
    populate_postgres_data()
    
    logger.info("Перенос данных в MongoDB из PostgreSQL...")
    populate_mongodb()
    
    logger.info("Перенос данных в Redis из PostgreSQL с пагинацией...")
    populate_redis()
    
    logger.info("Перенос данных в Neo4j из PostgreSQL с пагинацией...")
    populate_neo4j()
    
    logger.info("Перенос данных в Elasticsearch из PostgreSQL с пагинацией...")
    populate_elasticsearch()
    
    logger.info("Все базы данных успешно обновлены и заполнены данными.")

if __name__ == "__main__":
    main()
