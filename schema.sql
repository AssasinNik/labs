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

-- Посещения
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

-- Триггерная функция остается без изменений
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

-- Пример партиции
CREATE TABLE attendance_2023_09 PARTITION OF attendance
    FOR VALUES FROM ('2023-09-01') TO ('2023-10-01');
 
CREATE TABLE lecture_materials (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
 description TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
 id_lecture INT NOT NULL REFERENCES lecture(id)
)