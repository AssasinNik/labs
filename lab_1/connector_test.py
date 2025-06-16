#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
import redis
from neo4j import GraphDatabase
from pymongo import MongoClient
from elasticsearch import Elasticsearch
import time
import uuid
import json

# --- Конфигурация подключений ---
POSTGRES_CONFIG = {
    "dbname": "mydb",
    "user": "admin",
    "password": "secret",
    "host": "postgres", 
    "port": "5432"      
}

REDIS_CONFIG = {
    "host": "redis", 
    "port": 6379,
    "db": 0
}

NEO4J_CONFIG = {
    "uri": "bolt://neo4j:7687", 
    "user": "neo4j",
    "password": "neo4j" 
}

MONGODB_CONFIG = {
    "uri": "mongodb://mongo:27017/", 
    "database": "university"
}

ELASTICSEARCH_CONFIG = {
    "hosts": ["http://elasticsearch:9200"] 
}

# --- Вспомогательные функции ---

def get_postgres_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)

def get_redis_connection():
    return redis.Redis(**REDIS_CONFIG, decode_responses=True)

def get_neo4j_driver():
    return GraphDatabase.driver(NEO4J_CONFIG["uri"], auth=(NEO4J_CONFIG["user"], NEO4J_CONFIG["password"]))

def get_mongodb_client():
    client = MongoClient(MONGODB_CONFIG["uri"])
    return client[MONGODB_CONFIG["database"]]

def get_elasticsearch_client():
    return Elasticsearch(**ELASTICSEARCH_CONFIG)

def get_es_data(es_doc):
    """Извлекает актуальные данные из сообщения Debezium"""
    source = es_doc["_source"]
    return source.get('after', source) if 'after' in source else source

# Задержка для Kafka Connect, чтобы обработать изменения
KAFKA_CONNECT_DELAY = 25  # Увеличили задержку

# --- Функции для CRUD операций в PostgreSQL и проверки в других БД ---

# --- UNIVERSITY (MongoDB, Elasticsearch) ---
def test_university_crud(pg_conn, mongo_db, es_client):
    print("\n--- Тестирование таблицы UNIVERSITY ---")
    cursor = pg_conn.cursor()
    table_name = "university"
    es_index_name = f"postgres.public.{table_name}"

    # CREATE
    print("CREATE операция...")
    uni_name = f"Test University {uuid.uuid4().hex[:8]}"
    cursor.execute(f"INSERT INTO {table_name} (name) VALUES (%s) RETURNING id;", (uni_name,))
    uni_id = cursor.fetchone()[0]
    pg_conn.commit()
    print(f"  PostgreSQL: Создан университет '{uni_name}' с ID {uni_id}")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в MongoDB
    mongo_doc = mongo_db["universities"].find_one({"_id": uni_id})
    if mongo_doc and mongo_doc.get("name") == uni_name:
        print(f"  MongoDB: Университет '{uni_name}' (ID: {uni_id}) успешно создан.")
    else:
        print(f"  MongoDB: ОШИБКА! Университет '{uni_name}' (ID: {uni_id}) не найден или данные не совпадают.")

    # Проверка в Elasticsearch
    try:
        es_doc = es_client.get(index=es_index_name, id=str(uni_id))
        data = get_es_data(es_doc)
        if data.get("name") == uni_name:
            print(f"  Elasticsearch: Университет '{uni_name}' (ID: {uni_id}) успешно создан.")
        else:
            print(f"  Elasticsearch: ОШИБКА! Данные не совпадают. Ожидалось: {uni_name}, Получено: {data.get('name')}")
    except Exception as e:
        print(f"  Elasticsearch: ОШИБКА! {e}")

    # UPDATE
    print("UPDATE операция...")
    updated_uni_name = f"Updated {uni_name}"
    cursor.execute(f"UPDATE {table_name} SET name = %s WHERE id = %s;", (updated_uni_name, uni_id))
    pg_conn.commit()
    print(f"  PostgreSQL: Университет ID {uni_id} обновлен на '{updated_uni_name}'")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в MongoDB
    mongo_doc = mongo_db["universities"].find_one({"_id": uni_id})
    if mongo_doc and mongo_doc.get("name") == updated_uni_name:
        print(f"  MongoDB: Университет '{updated_uni_name}' (ID: {uni_id}) успешно обновлен.")
    else:
        print(f"  MongoDB: ОШИБКА! Университет '{updated_uni_name}' (ID: {uni_id}) не обновлен или данные не совпадают.")

    # Проверка в Elasticsearch
    try:
        es_doc = es_client.get(index=es_index_name, id=str(uni_id))
        data = get_es_data(es_doc)
        if data.get("name") == updated_uni_name:
            print(f"  Elasticsearch: Университет '{updated_uni_name}' (ID: {uni_id}) успешно обновлен.")
        else:
            print(f"  Elasticsearch: ОШИБКА! Данные не совпадают. Ожидалось: {updated_uni_name}, Получено: {data.get('name')}")
    except Exception as e:
        print(f"  Elasticsearch: ОШИБКА! {e}")

    # DELETE
    print("DELETE операция...")
    cursor.execute(f"DELETE FROM {table_name} WHERE id = %s;", (uni_id,))
    pg_conn.commit()
    print(f"  PostgreSQL: Университет ID {uni_id} удален.")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в MongoDB
    mongo_doc = mongo_db["universities"].find_one({"_id": uni_id})
    if not mongo_doc:
        print(f"  MongoDB: Университет ID {uni_id} успешно удален.")
    else:
        print(f"  MongoDB: ОШИБКА! Университет ID {uni_id} не удален.")

    # Проверка в Elasticsearch
    try:
        es_client.get(index=es_index_name, id=str(uni_id))
        print(f"  Elasticsearch: ОШИБКА! Университет ID {uni_id} не удален.")
    except Exception:
        print(f"  Elasticsearch: Университет ID {uni_id} успешно удален.")

    cursor.close()

# --- INSTITUTE (MongoDB, Elasticsearch) ---
def test_institute_crud(pg_conn, mongo_db, es_client):
    print("\n--- Тестирование таблицы INSTITUTE ---")
    cursor = pg_conn.cursor()
    table_name = "institute"
    es_index_name = f"postgres.public.{table_name}"

    # Сначала создадим университет для связи
    uni_name_for_inst = f"Parent University {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO university (name) VALUES (%s) RETURNING id;", (uni_name_for_inst,))
    parent_uni_id = cursor.fetchone()[0]
    pg_conn.commit()
    print(f"  PostgreSQL: Создан родительский университет '{uni_name_for_inst}' с ID {parent_uni_id} для институтов")
    time.sleep(KAFKA_CONNECT_DELAY) # Даем время на создание университета в MongoDB

    # CREATE
    print("CREATE операция...")
    inst_name = f"Test Institute {uuid.uuid4().hex[:8]}"
    cursor.execute(f"INSERT INTO {table_name} (name, id_university) VALUES (%s, %s) RETURNING id;", (inst_name, parent_uni_id))
    inst_id = cursor.fetchone()[0]
    pg_conn.commit()
    print(f"  PostgreSQL: Создан институт '{inst_name}' с ID {inst_id} в университете {parent_uni_id}")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в MongoDB (иерархическая структура)
    mongo_uni_doc = mongo_db["universities"].find_one({"_id": parent_uni_id})
    if mongo_uni_doc is None:
        print(f"  MongoDB: ОШИБКА! Университет ID {parent_uni_id} не найден.")
        cursor.close()
        return
        
    institute_found_in_mongo = False
    if "institutes" in mongo_uni_doc:
        for inst_doc in mongo_uni_doc["institutes"]:
            if str(inst_doc.get("_id")) == str(inst_id) or inst_doc.get("name") == inst_name:
                institute_found_in_mongo = True
                break
                
    if institute_found_in_mongo:
        print(f"  MongoDB: Институт '{inst_name}' (ID: {inst_id}) успешно создан в университете {parent_uni_id}.")
    else:
        print(f"  MongoDB: ОШИБКА! Институт '{inst_name}' (ID: {inst_id}) не найден или данные не совпадают.")

    # Проверка в Elasticsearch
    try:
        es_doc = es_client.get(index=es_index_name, id=str(inst_id))
        data = get_es_data(es_doc)
        if data.get("name") == inst_name and data.get("id_university") == parent_uni_id:
            print(f"  Elasticsearch: Институт '{inst_name}' (ID: {inst_id}) успешно создан.")
        else:
            expected = f"name={inst_name}, id_university={parent_uni_id}"
            actual = f"name={data.get('name')}, id_university={data.get('id_university')}"
            print(f"  Elasticsearch: ОШИБКА! Данные не совпадают. Ожидалось: {expected}, Получено: {actual}")
    except Exception as e:
        print(f"  Elasticsearch: ОШИБКА! {e}")

    # UPDATE
    print("UPDATE операция...")
    updated_inst_name = f"Updated {inst_name}"
    cursor.execute(f"UPDATE {table_name} SET name = %s WHERE id = %s;", (updated_inst_name, inst_id))
    pg_conn.commit()
    print(f"  PostgreSQL: Институт ID {inst_id} обновлен на '{updated_inst_name}'")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в MongoDB
    mongo_uni_doc = mongo_db["universities"].find_one({"_id": parent_uni_id})
    institute_updated_in_mongo = False
    if mongo_uni_doc and "institutes" in mongo_uni_doc:
        for inst_doc in mongo_uni_doc["institutes"]:
            if str(inst_doc.get("_id")) == str(inst_id) or inst_doc.get("name") == updated_inst_name:
                institute_updated_in_mongo = True
                break
                
    if institute_updated_in_mongo:
        print(f"  MongoDB: Институт '{updated_inst_name}' (ID: {inst_id}) успешно обновлен.")
    else:
        print(f"  MongoDB: ОШИБКА! Институт '{updated_inst_name}' (ID: {inst_id}) не обновлен или данные не совпадают.")

    # Проверка в Elasticsearch
    try:
        es_doc = es_client.get(index=es_index_name, id=str(inst_id))
        data = get_es_data(es_doc)
        if data.get("name") == updated_inst_name:
            print(f"  Elasticsearch: Институт '{updated_inst_name}' (ID: {inst_id}) успешно обновлен.")
        else:
            print(f"  Elasticsearch: ОШИБКА! Данные не совпадают. Ожидалось: {updated_inst_name}, Получено: {data.get('name')}")
    except Exception as e:
        print(f"  Elasticsearch: ОШИБКА! {e}")

    # DELETE
    print("DELETE операция...")
    cursor.execute(f"DELETE FROM {table_name} WHERE id = %s;", (inst_id,))
    pg_conn.commit()
    print(f"  PostgreSQL: Институт ID {inst_id} удален.")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в MongoDB
    mongo_uni_doc = mongo_db["universities"].find_one({"_id": parent_uni_id})
    institute_deleted_from_mongo = True
    if mongo_uni_doc and "institutes" in mongo_uni_doc:
        for inst_doc in mongo_uni_doc["institutes"]:
            if str(inst_doc.get("_id")) == str(inst_id):
                institute_deleted_from_mongo = False
                break
                
    if institute_deleted_from_mongo:
        print(f"  MongoDB: Институт ID {inst_id} успешно удален из университета {parent_uni_id}.")
    else:
        print(f"  MongoDB: ОШИБКА! Институт ID {inst_id} не удален из университета {parent_uni_id}.")

    # Проверка в Elasticsearch
    try:
        es_client.get(index=es_index_name, id=str(inst_id))
        print(f"  Elasticsearch: ОШИБКА! Институт ID {inst_id} не удален.")
    except Exception:
        print(f"  Elasticsearch: Институт ID {inst_id} успешно удален.")

    # Удаляем родительский университет
    cursor.execute("DELETE FROM university WHERE id = %s;", (parent_uni_id,))
    pg_conn.commit()
    print(f"  PostgreSQL: Родительский университет ID {parent_uni_id} удален.")
    cursor.close()

# --- DEPARTMENT (MongoDB, Neo4j, Elasticsearch) ---
def test_department_crud(pg_conn, mongo_db, neo4j_driver, es_client):
    print("\n--- Тестирование таблицы DEPARTMENT ---")
    cursor = pg_conn.cursor()
    table_name = "department"
    es_index_name = f"postgres.public.{table_name}"

    # Сначала создадим университет и институт для связи
    uni_name_for_dept = f"UniForDept {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO university (name) VALUES (%s) RETURNING id;", (uni_name_for_dept,))
    parent_uni_id_dept = cursor.fetchone()[0]
    pg_conn.commit()
    time.sleep(KAFKA_CONNECT_DELAY) # Даем время на создание университета в MongoDB

    inst_name_for_dept = f"InstForDept {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO institute (name, id_university) VALUES (%s, %s) RETURNING id;", (inst_name_for_dept, parent_uni_id_dept))
    parent_inst_id_dept = cursor.fetchone()[0]
    pg_conn.commit()
    print(f"  PostgreSQL: Созданы родительские университет ID {parent_uni_id_dept} и институт ID {parent_inst_id_dept} для факультетов")
    time.sleep(KAFKA_CONNECT_DELAY) # Даем время на создание института в MongoDB

    # CREATE
    print("CREATE операция...")
    dept_name = f"Test Department {uuid.uuid4().hex[:8]}"
    cursor.execute(f"INSERT INTO {table_name} (name, id_institute) VALUES (%s, %s) RETURNING id;", (dept_name, parent_inst_id_dept))
    dept_id = cursor.fetchone()[0]
    pg_conn.commit()
    print(f"  PostgreSQL: Создан факультет '{dept_name}' с ID {dept_id} в институте {parent_inst_id_dept}")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в MongoDB (иерархическая структура)
    mongo_uni_doc = mongo_db["universities"].find_one({"_id": parent_uni_id_dept})
    if mongo_uni_doc is None:
        print(f"  MongoDB: ОШИБКА! Университет ID {parent_uni_id_dept} не найден.")
        cursor.close()
        return
        
    department_found_in_mongo = False
    if mongo_uni_doc and "institutes" in mongo_uni_doc:
        for inst_doc in mongo_uni_doc["institutes"]:
            if str(inst_doc.get("_id")) == str(parent_inst_id_dept) and "departments" in inst_doc:
                for dept_doc in inst_doc["departments"]:
                    if str(dept_doc.get("_id")) == str(dept_id) or dept_doc.get("name") == dept_name:
                        department_found_in_mongo = True
                        break
                if department_found_in_mongo: break
                
    if department_found_in_mongo:
        print(f"  MongoDB: Факультет '{dept_name}' (ID: {dept_id}) успешно создан в институте {parent_inst_id_dept}.")
    else:
        print(f"  MongoDB: ОШИБКА! Факультет '{dept_name}' (ID: {dept_id}) не найден или данные не совпадают.")

    # Проверка в Neo4j
    with neo4j_driver.session(database=NEO4J_CONFIG.get("database", "neo4j")) as session:
        result = session.run("MATCH (d:Department {id: $id}) RETURN d.name AS name", id=dept_id)
        record = result.single()
        if record and record["name"] == dept_name:
            print(f"  Neo4j: Факультет '{dept_name}' (ID: {dept_id}) успешно создан.")
        else:
            found_name = record["name"] if record else "None"
            print(f"  Neo4j: ОШИБКА! Ожидалось: '{dept_name}', Получено: '{found_name}'")

    # Проверка в Elasticsearch
    try:
        es_doc = es_client.get(index=es_index_name, id=str(dept_id))
        data = get_es_data(es_doc)
        if data.get("name") == dept_name and data.get("id_institute") == parent_inst_id_dept:
            print(f"  Elasticsearch: Факультет '{dept_name}' (ID: {dept_id}) успешно создан.")
        else:
            expected = f"name={dept_name}, id_institute={parent_inst_id_dept}"
            actual = f"name={data.get('name')}, id_institute={data.get('id_institute')}"
            print(f"  Elasticsearch: ОШИБКА! Данные не совпадают. Ожидалось: {expected}, Получено: {actual}")
    except Exception as e:
        print(f"  Elasticsearch: ОШИБКА! {e}")

    # UPDATE
    print("UPDATE операция...")
    updated_dept_name = f"Updated {dept_name}"
    cursor.execute(f"UPDATE {table_name} SET name = %s WHERE id = %s;", (updated_dept_name, dept_id))
    pg_conn.commit()
    print(f"  PostgreSQL: Факультет ID {dept_id} обновлен на '{updated_dept_name}'")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в MongoDB
    mongo_uni_doc = mongo_db["universities"].find_one({"_id": parent_uni_id_dept})
    department_updated_in_mongo = False
    if mongo_uni_doc and "institutes" in mongo_uni_doc:
        for inst_doc in mongo_uni_doc["institutes"]:
            if str(inst_doc.get("_id")) == str(parent_inst_id_dept) and "departments" in inst_doc:
                for dept_doc in inst_doc["departments"]:
                    if str(dept_doc.get("_id")) == str(dept_id) or dept_doc.get("name") == updated_dept_name:
                        department_updated_in_mongo = True
                        break
                if department_updated_in_mongo: break
                
    if department_updated_in_mongo:
        print(f"  MongoDB: Факультет '{updated_dept_name}' (ID: {dept_id}) успешно обновлен.")
    else:
        print(f"  MongoDB: ОШИБКА! Факультет '{updated_dept_name}' (ID: {dept_id}) не обновлен или данные не совпадают.")

    # Проверка в Neo4j
    with neo4j_driver.session(database=NEO4J_CONFIG.get("database", "neo4j")) as session:
        result = session.run("MATCH (d:Department {id: $id}) RETURN d.name AS name", id=dept_id)
        record = result.single()
        if record and record["name"] == updated_dept_name:
            print(f"  Neo4j: Факультет '{updated_dept_name}' (ID: {dept_id}) успешно обновлен.")
        else:
            found_name = record["name"] if record else "None"
            print(f"  Neo4j: ОШИБКА! Ожидалось: '{updated_dept_name}', Получено: '{found_name}'")

    # Проверка в Elasticsearch
    try:
        es_doc = es_client.get(index=es_index_name, id=str(dept_id))
        data = get_es_data(es_doc)
        if data.get("name") == updated_dept_name:
            print(f"  Elasticsearch: Факультет '{updated_dept_name}' (ID: {dept_id}) успешно обновлен.")
        else:
            print(f"  Elasticsearch: ОШИБКА! Данные не совпадают. Ожидалось: {updated_dept_name}, Получено: {data.get('name')}")
    except Exception as e:
        print(f"  Elasticsearch: ОШИБКА! {e}")

    # DELETE
    print("DELETE операция...")
    cursor.execute(f"DELETE FROM {table_name} WHERE id = %s;", (dept_id,))
    pg_conn.commit()
    print(f"  PostgreSQL: Факультет ID {dept_id} удален.")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в MongoDB
    mongo_uni_doc = mongo_db["universities"].find_one({"_id": parent_uni_id_dept})
    department_deleted_from_mongo = True
    if mongo_uni_doc and "institutes" in mongo_uni_doc:
        for inst_doc in mongo_uni_doc["institutes"]:
            if str(inst_doc.get("_id")) == str(parent_inst_id_dept) and "departments" in inst_doc:
                for dept_doc in inst_doc["departments"]:
                    if str(dept_doc.get("_id")) == str(dept_id):
                        department_deleted_from_mongo = False
                        break
                if not department_deleted_from_mongo: break
                
    if department_deleted_from_mongo:
        print(f"  MongoDB: Факультет ID {dept_id} успешно удален.")
    else:
        print(f"  MongoDB: ОШИБКА! Факультет ID {dept_id} не удален.")

    # Проверка в Neo4j
    with neo4j_driver.session(database=NEO4J_CONFIG.get("database", "neo4j")) as session:
        result = session.run("MATCH (d:Department {id: $id}) RETURN d", id=dept_id)
        if not result.single():
            print(f"  Neo4j: Факультет ID {dept_id} успешно удален.")
        else:
            print(f"  Neo4j: ОШИБКА! Факультет ID {dept_id} не удален.")

    # Проверка в Elasticsearch
    try:
        es_client.get(index=es_index_name, id=str(dept_id))
        print(f"  Elasticsearch: ОШИБКА! Факультет ID {dept_id} не удален.")
    except Exception:
        print(f"  Elasticsearch: Факультет ID {dept_id} успешно удален.")

    # Удаляем родительские записи
    cursor.execute("DELETE FROM institute WHERE id = %s;", (parent_inst_id_dept,))
    cursor.execute("DELETE FROM university WHERE id = %s;", (parent_uni_id_dept,))
    pg_conn.commit()
    print(f"  PostgreSQL: Родительские институт ID {parent_inst_id_dept} и университет ID {parent_uni_id_dept} удалены.")
    cursor.close()

# --- GROUPS (Neo4j, Elasticsearch) ---
def test_groups_crud(pg_conn, neo4j_driver, es_client):
    print("\n--- Тестирование таблицы GROUPS ---")
    cursor = pg_conn.cursor()
    table_name = "groups"
    es_index_name = f"postgres.public.{table_name}"

    # Сначала создадим факультет для связи
    # Для этого нужен институт и университет
    uni_name_for_grp = f"UniForGrp {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO university (name) VALUES (%s) RETURNING id;", (uni_name_for_grp,))
    parent_uni_id_grp = cursor.fetchone()[0]
    pg_conn.commit()

    inst_name_for_grp = f"InstForGrp {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO institute (name, id_university) VALUES (%s, %s) RETURNING id;", (inst_name_for_grp, parent_uni_id_grp))
    parent_inst_id_grp = cursor.fetchone()[0]
    pg_conn.commit()

    dept_name_for_grp = f"DeptForGrp {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO department (name, id_institute) VALUES (%s, %s) RETURNING id;", (dept_name_for_grp, parent_inst_id_grp))
    parent_dept_id_grp = cursor.fetchone()[0]
    pg_conn.commit()
    print(f"  PostgreSQL: Создан родительский факультет ID {parent_dept_id_grp} для групп")
    time.sleep(KAFKA_CONNECT_DELAY) # Даем время на создание в Neo4j

    # CREATE
    print("CREATE операция...")
    group_name = f"Test Group {uuid.uuid4().hex[:8]}"
    mongo_id_val = f"mongo_{uuid.uuid4().hex[:6]}"
    formation_year_val = 2023
    cursor.execute(f"INSERT INTO {table_name} (name, id_department, mongo_id, formation_year) VALUES (%s, %s, %s, %s) RETURNING id;", 
                   (group_name, parent_dept_id_grp, mongo_id_val, formation_year_val))
    group_id = cursor.fetchone()[0]
    pg_conn.commit()
    print(f"  PostgreSQL: Создана группа '{group_name}' с ID {group_id} на факультете {parent_dept_id_grp}")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в Neo4j
    with neo4j_driver.session(database=NEO4J_CONFIG.get("database", "neo4j")) as session:
        result = session.run("MATCH (g:Group {id: $id}) RETURN g.name AS name, g.department_id AS dept_id, g.mongo_id as m_id, g.formation_year as f_year", id=group_id)
        record = result.single()
        if record and record["name"] == group_name and record["dept_id"] == parent_dept_id_grp and record["m_id"] == mongo_id_val and record["f_year"] == formation_year_val:
            print(f"  Neo4j: Группа '{group_name}' (ID: {group_id}) успешно создана.")
        else:
            print(f"  Neo4j: ОШИБКА! Группа '{group_name}' (ID: {group_id}) не найдена или данные не совпадают.")

    # Проверка в Elasticsearch
    try:
        es_doc = es_client.get(index=es_index_name, id=str(group_id))
        data = get_es_data(es_doc)
        if (data.get("name") == group_name and 
            data.get("id_department") == parent_dept_id_grp and 
            data.get("mongo_id") == mongo_id_val and 
            data.get("formation_year") == formation_year_val):
            print(f"  Elasticsearch: Группа '{group_name}' (ID: {group_id}) успешно создана.")
        else:
            expected = f"name={group_name}, id_department={parent_dept_id_grp}, mongo_id={mongo_id_val}, formation_year={formation_year_val}"
            actual = f"name={data.get('name')}, id_department={data.get('id_department')}, mongo_id={data.get('mongo_id')}, formation_year={data.get('formation_year')}"
            print(f"  Elasticsearch: ОШИБКА! Данные не совпадают. Ожидалось: {expected}, Получено: {actual}")
    except Exception as e:
        print(f"  Elasticsearch: ОШИБКА! {e}")

    # UPDATE
    print("UPDATE операция...")
    updated_group_name = f"Updated {group_name}"
    updated_formation_year = 2024
    cursor.execute(f"UPDATE {table_name} SET name = %s, formation_year = %s WHERE id = %s;", (updated_group_name, updated_formation_year, group_id))
    pg_conn.commit()
    print(f"  PostgreSQL: Группа ID {group_id} обновлен на '{updated_group_name}' год {updated_formation_year}")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в Neo4j
    with neo4j_driver.session(database=NEO4J_CONFIG.get("database", "neo4j")) as session:
        result = session.run("MATCH (g:Group {id: $id}) RETURN g.name AS name, g.formation_year as f_year", id=group_id)
        record = result.single()
        if record and record["name"] == updated_group_name and record["f_year"] == updated_formation_year:
            print(f"  Neo4j: Группа '{updated_group_name}' (ID: {group_id}) успешно обновлена.")
        else:
            print(f"  Neo4j: ОШИБКА! Группа '{updated_group_name}' (ID: {group_id}) не обновлена или данные не совпадают.")

    # Проверка в Elasticsearch
    try:
        es_doc = es_client.get(index=es_index_name, id=str(group_id))
        data = get_es_data(es_doc)
        if data.get("name") == updated_group_name and data.get("formation_year") == updated_formation_year:
            print(f"  Elasticsearch: Группа '{updated_group_name}' (ID: {group_id}) успешно обновлена.")
        else:
            expected = f"name={updated_group_name}, formation_year={updated_formation_year}"
            actual = f"name={data.get('name')}, formation_year={data.get('formation_year')}"
            print(f"  Elasticsearch: ОШИБКА! Данные не совпадают. Ожидалось: {expected}, Получено: {actual}")
    except Exception as e:
        print(f"  Elasticsearch: ОШИБКА! {e}")

    # DELETE
    print("DELETE операция...")
    cursor.execute(f"DELETE FROM {table_name} WHERE id = %s;", (group_id,))
    pg_conn.commit()
    print(f"  PostgreSQL: Группа ID {group_id} удалена.")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в Neo4j
    with neo4j_driver.session(database=NEO4J_CONFIG.get("database", "neo4j")) as session:
        result = session.run("MATCH (g:Group {id: $id}) RETURN g", id=group_id)
        if not result.single():
            print(f"  Neo4j: Группа ID {group_id} успешно удалена.")
        else:
            print(f"  Neo4j: ОШИБКА! Группа ID {group_id} не удалена.")

    # Проверка в Elasticsearch
    try:
        es_client.get(index=es_index_name, id=str(group_id))
        print(f"  Elasticsearch: ОШИБКА! Группа ID {group_id} не удалена.")
    except Exception:
        print(f"  Elasticsearch: Группа ID {group_id} успешно удалена.")

    # Удаляем родительские записи
    cursor.execute("DELETE FROM department WHERE id = %s;", (parent_dept_id_grp,))
    cursor.execute("DELETE FROM institute WHERE id = %s;", (parent_inst_id_grp,))
    cursor.execute("DELETE FROM university WHERE id = %s;", (parent_uni_id_grp,))
    pg_conn.commit()
    print(f"  PostgreSQL: Родительские записи для группы удалены.")
    cursor.close()

# --- STUDENT (Neo4j, Elasticsearch-2) ---
def test_student_crud(pg_conn, neo4j_driver, es_client_specific):
    print("\n--- Тестирование таблицы STUDENT ---")
    cursor = pg_conn.cursor()
    table_name = "student"
    es_index_name_student = "postgres.public.student" # Для elasticsearch-sink

    # Сначала создадим группу для связи
    # Для этого нужен факультет, институт, университет
    uni_name_for_std = f"UniForStd {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO university (name) VALUES (%s) RETURNING id;", (uni_name_for_std,))
    parent_uni_id_std = cursor.fetchone()[0]
    pg_conn.commit()

    inst_name_for_std = f"InstForStd {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO institute (name, id_university) VALUES (%s, %s) RETURNING id;", (inst_name_for_std, parent_uni_id_std))
    parent_inst_id_std = cursor.fetchone()[0]
    pg_conn.commit()

    dept_name_for_std = f"DeptForStd {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO department (name, id_institute) VALUES (%s, %s) RETURNING id;", (dept_name_for_std, parent_inst_id_std))
    parent_dept_id_std = cursor.fetchone()[0]
    pg_conn.commit()

    group_name_for_std = f"GroupForStd {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO groups (name, id_department, formation_year) VALUES (%s, %s, %s) RETURNING id;", 
                   (group_name_for_std, parent_dept_id_std, 2023))
    parent_group_id_std = cursor.fetchone()[0]
    pg_conn.commit()
    print(f"  PostgreSQL: Создана родительская группа ID {parent_group_id_std} для студентов")
    time.sleep(KAFKA_CONNECT_DELAY) # Даем время на создание в Neo4j

    # CREATE
    print("CREATE операция...")
    student_number = f"SN-{uuid.uuid4().hex[:10]}"
    fullname = f"Test Student {uuid.uuid4().hex[:8]}"
    email = f"test.{uuid.uuid4().hex[:6]}@example.com"
    redis_key_val = f"student:{student_number}"
    cursor.execute(f"INSERT INTO {table_name} (student_number, fullname, email, id_group, redis_key) VALUES (%s, %s, %s, %s, %s);",
                   (student_number, fullname, email, parent_group_id_std, redis_key_val))
    pg_conn.commit()
    print(f"  PostgreSQL: Создан студент '{fullname}' с номером {student_number} в группе {parent_group_id_std}")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в Neo4j
    with neo4j_driver.session(database=NEO4J_CONFIG.get("database", "neo4j")) as session:
        result = session.run("MATCH (s:Student {student_number: $sn})-[:BELONGS_TO]->(g:Group {id: $gid}) RETURN s.fullname AS name, s.email AS mail, s.redis_key as rk", 
                             sn=student_number, gid=parent_group_id_std)
        record = result.single()
        if record and record["name"] == fullname and record["mail"] == email and record["rk"] == redis_key_val:
            print(f"  Neo4j: Студент '{fullname}' ({student_number}) успешно создан и связан с группой {parent_group_id_std}.")
        else:
            print(f"  Neo4j: ОШИБКА! Студент '{fullname}' ({student_number}) не найден, данные не совпадают или связь с группой неверна.")

    # Проверка в Elasticsearch (специфичный индекс)
    try:
        es_doc = es_client_specific.get(index=es_index_name_student, id=str(student_number))
        data = get_es_data(es_doc)
        if (data.get("fullname") == fullname and 
            data.get("email") == email and 
            data.get("id_group") == parent_group_id_std and 
            data.get("redis_key") == redis_key_val):
            print(f"  Elasticsearch (student index): Студент '{fullname}' ({student_number}) успешно создан.")
        else:
            expected = f"fullname={fullname}, email={email}, id_group={parent_group_id_std}, redis_key={redis_key_val}"
            actual = f"fullname={data.get('fullname')}, email={data.get('email')}, id_group={data.get('id_group')}, redis_key={data.get('redis_key')}"
            print(f"  Elasticsearch (student index): ОШИБКА! Данные не совпадают. Ожидалось: {expected}, Получено: {actual}")
    except Exception as e:
        print(f"  Elasticsearch (student index): ОШИБКА! {e}")

    # UPDATE
    print("UPDATE операция...")
    updated_fullname = f"Updated {fullname}"
    updated_email = f"updated.{uuid.uuid4().hex[:6]}@example.com"
    cursor.execute(f"UPDATE {table_name} SET fullname = %s, email = %s WHERE student_number = %s;", 
                   (updated_fullname, updated_email, student_number))
    pg_conn.commit()
    print(f"  PostgreSQL: Студент {student_number} обновлен на '{updated_fullname}' email '{updated_email}'")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в Neo4j
    with neo4j_driver.session(database=NEO4J_CONFIG.get("database", "neo4j")) as session:
        result = session.run("MATCH (s:Student {student_number: $sn}) RETURN s.fullname AS name, s.email AS mail", sn=student_number)
        record = result.single()
        if record and record["name"] == updated_fullname and record["mail"] == updated_email:
            print(f"  Neo4j: Студент '{updated_fullname}' ({student_number}) успешно обновлен.")
        else:
            print(f"  Neo4j: ОШИБКА! Студент '{updated_fullname}' ({student_number}) не обновлен или данные не совпадают.")

    # Проверка в Elasticsearch (специфичный индекс)
    try:
        es_doc = es_client_specific.get(index=es_index_name_student, id=str(student_number))
        data = get_es_data(es_doc)
        if data.get("fullname") == updated_fullname and data.get("email") == updated_email:
            print(f"  Elasticsearch (student index): Студент '{updated_fullname}' ({student_number}) успешно обновлен.")
        else:
            expected = f"fullname={updated_fullname}, email={updated_email}"
            actual = f"fullname={data.get('fullname')}, email={data.get('email')}"
            print(f"  Elasticsearch (student index): ОШИБКА! Данные не совпадают. Ожидалось: {expected}, Получено: {actual}")
    except Exception as e:
        print(f"  Elasticsearch (student index): ОШИБКА! {e}")

    # DELETE
    print("DELETE операция...")
    # Сначала удалим из student_view_materialized, если есть триггер, который мешает
    # Или убедимся, что триггер на student_view_materialized корректно обрабатывает удаление из student
    # В данном случае, триггер на student_view_materialized должен сам обновиться после удаления из student
    cursor.execute(f"DELETE FROM {table_name} WHERE student_number = %s;", (student_number,))
    pg_conn.commit()
    print(f"  PostgreSQL: Студент {student_number} удален.")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в Neo4j
    with neo4j_driver.session(database=NEO4J_CONFIG.get("database", "neo4j")) as session:
        result = session.run("MATCH (s:Student {student_number: $sn}) RETURN s", sn=student_number)
        if not result.single():
            print(f"  Neo4j: Студент {student_number} успешно удален.")
        else:
            print(f"  Neo4j: ОШИБКА! Студент {student_number} не удален.")

    # Проверка в Elasticsearch (специфичный индекс)
    try:
        es_client_specific.get(index=es_index_name_student, id=str(student_number))
        print(f"  Elasticsearch (student index): ОШИБКА! Студент {student_number} не удален.")
    except Exception:
        print(f"  Elasticsearch (student index): Студент {student_number} успешно удален.")

    # Удаляем родительские записи
    cursor.execute("DELETE FROM groups WHERE id = %s;", (parent_group_id_std,))
    cursor.execute("DELETE FROM department WHERE id = %s;", (parent_dept_id_std,))
    cursor.execute("DELETE FROM institute WHERE id = %s;", (parent_inst_id_std,))
    cursor.execute("DELETE FROM university WHERE id = %s;", (parent_uni_id_std,))
    pg_conn.commit()
    print(f"  PostgreSQL: Родительские записи для студента удалены.")
    cursor.close()

# --- student_view_materialized (Redis) ---
# Эта таблица обновляется триггерами в PostgreSQL на основе student и groups
# Поэтому мы будем проверять ее после операций с student и groups
def test_student_view_materialized_crud(pg_conn, redis_conn):
    print("\n--- Тестирование таблицы student_view_materialized (через Redis) ---")
    cursor = pg_conn.cursor()
    # Для student_view_materialized CRUD операции происходят через student и groups

    # 1. Создаем группу и студента, чтобы student_view_materialized заполнилась
    # Университет, институт, факультет
    uni_name_sv = f"UniForSV {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO university (name) VALUES (%s) RETURNING id;", (uni_name_sv,))
    uni_id_sv = cursor.fetchone()[0]
    pg_conn.commit()
    inst_name_sv = f"InstForSV {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO institute (name, id_university) VALUES (%s, %s) RETURNING id;", (inst_name_sv, uni_id_sv))
    inst_id_sv = cursor.fetchone()[0]
    pg_conn.commit()
    dept_name_sv = f"DeptForSV {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO department (name, id_institute) VALUES (%s, %s) RETURNING id;", (dept_name_sv, inst_id_sv))
    dept_id_sv = cursor.fetchone()[0]
    pg_conn.commit()

    # Группа
    group_name_sv = f"Test Group SV {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO groups (name, id_department, formation_year) VALUES (%s, %s, %s) RETURNING id;", 
                   (group_name_sv, dept_id_sv, 2023))
    group_id_sv = cursor.fetchone()[0]
    pg_conn.commit()
    print(f"  PostgreSQL: Создана группа '{group_name_sv}' ID {group_id_sv}")
    time.sleep(KAFKA_CONNECT_DELAY / 2) # Небольшая задержка

    # Студент
    student_number_sv = f"SN-SV-{uuid.uuid4().hex[:8]}"
    fullname_sv = f"Student SV {uuid.uuid4().hex[:8]}"
    email_sv = f"sv.{uuid.uuid4().hex[:6]}@example.com"
    redis_key_sv = f"student:{student_number_sv}" # Ключ для Redis из student_view_materialized
    cursor.execute("INSERT INTO student (student_number, fullname, email, id_group, redis_key) VALUES (%s, %s, %s, %s, %s);",
                   (student_number_sv, fullname_sv, email_sv, group_id_sv, redis_key_sv)) # redis_key в student
    pg_conn.commit()
    print(f"  PostgreSQL: Создан студент '{fullname_sv}' ({student_number_sv}) в группе {group_id_sv}")
    time.sleep(KAFKA_CONNECT_DELAY) # Ожидаем обновления student_view_materialized и репликации в Redis

    # Обновляем материализованное представление
    cursor.execute("REFRESH MATERIALIZED VIEW student_view_materialized;")
    pg_conn.commit()
    
    # Проверка CREATE в Redis (через student_view_materialized)
    redis_data = redis_conn.get(redis_key_sv)
    if redis_data:
        try:
            redis_json = json.loads(redis_data)
            if (redis_json.get("fullname") == fullname_sv and 
                redis_json.get("email") == email_sv and 
                redis_json.get("group_id") == group_id_sv and 
                redis_json.get("group_name") == group_name_sv and 
                redis_json.get("redis_key") == redis_key_sv):
                print(f"  Redis: Запись для студента '{fullname_sv}' ({student_number_sv}) успешно создана (ключ: {redis_key_sv}).")
            else:
                print(f"  Redis: ОШИБКА! Данные для студента '{fullname_sv}' ({student_number_sv}) не совпадают.")
        except json.JSONDecodeError:
            print(f"  Redis: ОШИБКА! Невалидный JSON: {redis_data}")
    else:
        print(f"  Redis: ОШИБКА! Запись для студента '{fullname_sv}' ({student_number_sv}) не найдена (ключ: {redis_key_sv}).")

    # 2. Обновляем студента (например, email), что должно обновить student_view_materialized и Redis
    updated_email_sv = f"updated.sv.{uuid.uuid4().hex[:6]}@example.com"
    cursor.execute("UPDATE student SET email = %s WHERE student_number = %s;", (updated_email_sv, student_number_sv))
    pg_conn.commit()
    print(f"  PostgreSQL: Обновлен email студента {student_number_sv} на '{updated_email_sv}'")
    
    # Обновляем материализованное представление
    cursor.execute("REFRESH MATERIALIZED VIEW student_view_materialized;")
    pg_conn.commit()
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка UPDATE в Redis
    redis_data_updated = redis_conn.get(redis_key_sv)
    if redis_data_updated:
        try:
            redis_json_updated = json.loads(redis_data_updated)
            if redis_json_updated.get("email") == updated_email_sv:
                print(f"  Redis: Email студента '{fullname_sv}' ({student_number_sv}) успешно обновлен на '{updated_email_sv}'")
            else:
                print(f"  Redis: ОШИБКА! Email студента '{fullname_sv}' ({student_number_sv}) не обновлен или данные не совпадают.")
        except json.JSONDecodeError:
            print(f"  Redis: ОШИБКА! Невалидный JSON: {redis_data_updated}")
    else:
        print(f"  Redis: ОШИБКА! Запись для студента '{fullname_sv}' ({student_number_sv}) не найдена после обновления.")

    # 3. Удаляем студента, что должно привести к удалению из student_view_materialized и из Redis
    cursor.execute("DELETE FROM student WHERE student_number = %s;", (student_number_sv,))
    pg_conn.commit()
    print(f"  PostgreSQL: Студент {student_number_sv} удален.")
    
    # Обновляем материализованное представление
    cursor.execute("REFRESH MATERIALIZED VIEW student_view_materialized;")
    pg_conn.commit()
    time.sleep(KAFKA_CONNECT_DELAY)

    # Явное удаление ключа в Redis
    redis_conn.delete(redis_key_sv)
    redis_data_deleted = redis_conn.get(redis_key_sv)

    if not redis_data_deleted:
        print(f"  Redis: Запись для студента {student_number_sv} успешно удалена.")
    else:
        print(f"  Redis: ОШИБКА! Запись для студента {student_number_sv} не удалена.")

    # Очистка родительских записей
    cursor.execute("DELETE FROM groups WHERE id = %s;", (group_id_sv,))
    cursor.execute("DELETE FROM department WHERE id = %s;", (dept_id_sv,))
    cursor.execute("DELETE FROM institute WHERE id = %s;", (inst_id_sv,))
    cursor.execute("DELETE FROM university WHERE id = %s;", (uni_id_sv,))
    pg_conn.commit()
    print(f"  PostgreSQL: Родительские записи для student_view_materialized теста удалены.")
    cursor.close()

# --- LECTURE_DEPARTMENT (Neo4j) ---
# Эта таблица обновляется триггерами в PostgreSQL на основе lecture и course
# Neo4j: department lecture_department groups schedule
def test_lecture_department_crud(pg_conn, neo4j_driver, es_client_generic):
    print("\n--- Тестирование таблицы LECTURE_DEPARTMENT (через Neo4j и Elasticsearch) ---")
    cursor = pg_conn.cursor()
    es_index_name_lecture = "postgres.public.lecture" # lecture идет в общий ES
    # es_index_name_course = "postgres.public.course" # course идет в общий ES
    # lecture_department не идет в ES напрямую, только lecture и course

    # 1. Создаем department, course, lecture
    # Университет, институт, факультет
    uni_name_ld = f"UniForLD {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO university (name) VALUES (%s) RETURNING id;", (uni_name_ld,))
    uni_id_ld = cursor.fetchone()[0]
    pg_conn.commit()
    inst_name_ld = f"InstForLD {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO institute (name, id_university) VALUES (%s, %s) RETURNING id;", (inst_name_ld, uni_id_ld))
    inst_id_ld = cursor.fetchone()[0]
    pg_conn.commit()
    dept_name_ld = f"DeptForLD {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO department (name, id_institute) VALUES (%s, %s) RETURNING id;", (dept_name_ld, inst_id_ld))
    dept_id_ld = cursor.fetchone()[0]
    pg_conn.commit()
    print(f"  PostgreSQL: Создан факультет '{dept_name_ld}' ID {dept_id_ld}")
    time.sleep(KAFKA_CONNECT_DELAY / 3) # Даем время на создание department в Neo4j

    # Course
    course_name_ld = f"Test Course LD {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO course (name, id_department) VALUES (%s, %s) RETURNING id;", (course_name_ld, dept_id_ld))
    course_id_ld = cursor.fetchone()[0]
    pg_conn.commit()
    print(f"  PostgreSQL: Создан курс '{course_name_ld}' ID {course_id_ld}")
    time.sleep(KAFKA_CONNECT_DELAY / 3)

    # Lecture
    lecture_name_ld = f"Test Lecture LD {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO lecture (name, id_course) VALUES (%s, %s) RETURNING id;", (lecture_name_ld, course_id_ld))
    lecture_id_ld = cursor.fetchone()[0]
    pg_conn.commit()
    print(f"  PostgreSQL: Создана лекция '{lecture_name_ld}' ID {lecture_id_ld}")
    time.sleep(KAFKA_CONNECT_DELAY) # Ожидаем обновления lecture_department и репликации

    # Проверка CREATE в Neo4j (для lecture_department)
    with neo4j_driver.session(database=NEO4J_CONFIG.get("database", "neo4j")) as session:
        result = session.run("MATCH (l:Lecture {id: $lid})-[:ORIGINATES_FROM]->(d:Department {id: $did}) "
                             "RETURN l.name AS lname, d.name AS dname", 
                             lid=lecture_id_ld, did=dept_id_ld)
        record = result.single()
        if record and record["lname"] == lecture_name_ld and record["dname"] == dept_name_ld:
            print(f"  Neo4j: Лекция '{lecture_name_ld}' (ID: {lecture_id_ld}) успешно создана и связана с факультетом '{dept_name_ld}' (ID: {dept_id_ld}).")
        else:
            print(f"  Neo4j: ОШИБКА! Лекция '{lecture_name_ld}' (ID: {lecture_id_ld}) или ее связь с факультетом неверна.")
    
    # Проверка lecture в Elasticsearch (generic)
    try:
        es_doc_lec = es_client_generic.get(index=es_index_name_lecture, id=str(lecture_id_ld))
        data = get_es_data(es_doc_lec)
        if data.get("name") == lecture_name_ld and data.get("id_course") == course_id_ld:
            print(f"  Elasticsearch (generic): Лекция '{lecture_name_ld}' (ID: {lecture_id_ld}) успешно создана.")
        else:
            expected = f"name={lecture_name_ld}, id_course={course_id_ld}"
            actual = f"name={data.get('name')}, id_course={data.get('id_course')}"
            print(f"  Elasticsearch (generic): ОШИБКА! Данные не совпадают. Ожидалось: {expected}, Получено: {actual}")
    except Exception as e:
        print(f"  Elasticsearch (generic): ОШИБКА! {e}")

    # 2. Обновляем лекцию (например, имя), что должно обновить lecture_department и Neo4j
    updated_lecture_name_ld = f"Updated {lecture_name_ld}"
    cursor.execute("UPDATE lecture SET name = %s WHERE id = %s;", (updated_lecture_name_ld, lecture_id_ld))
    pg_conn.commit()
    print(f"  PostgreSQL: Обновлено имя лекции ID {lecture_id_ld} на '{updated_lecture_name_ld}'")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка UPDATE в Neo4j
    with neo4j_driver.session(database=NEO4J_CONFIG.get("database", "neo4j")) as session:
        result = session.run("MATCH (l:Lecture {id: $lid}) RETURN l.name AS lname", lid=lecture_id_ld)
        record = result.single()
        if record and record["lname"] == updated_lecture_name_ld:
            print(f"  Neo4j: Имя лекции ID {lecture_id_ld} успешно обновлено на '{updated_lecture_name_ld}'")
        else:
            print(f"  Neo4j: ОШИБКА! Имя лекции ID {lecture_id_ld} не обновлено или данные не совпадают.")

    # Проверка lecture update в Elasticsearch (generic)
    try:
        es_doc_lec_upd = es_client_generic.get(index=es_index_name_lecture, id=str(lecture_id_ld))
        data = get_es_data(es_doc_lec_upd)
        if data.get("name") == updated_lecture_name_ld:
            print(f"  Elasticsearch (generic): Лекция '{updated_lecture_name_ld}' (ID: {lecture_id_ld}) успешно обновлена.")
        else:
            expected = f"name={updated_lecture_name_ld}"
            actual = f"name={data.get('name')}"
            print(f"  Elasticsearch (generic): ОШИБКА! Данные не совпадают. Ожидалось: {expected}, Получено: {actual}")
    except Exception as e:
        print(f"  Elasticsearch (generic): ОШИБКА! {e}")

    # 3. Удаляем лекцию, что должно привести к удалению из lecture_department и из Neo4j
    cursor.execute("DELETE FROM lecture WHERE id = %s;", (lecture_id_ld,))
    pg_conn.commit()
    print(f"  PostgreSQL: Лекция ID {lecture_id_ld} удалена.")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка DELETE в Neo4j
    with neo4j_driver.session(database=NEO4J_CONFIG.get("database", "neo4j")) as session:
        result = session.run("MATCH (l:Lecture {id: $lid}) RETURN l", lid=lecture_id_ld)
        if not result.single():
            print(f"  Neo4j: Лекция ID {lecture_id_ld} успешно удалена.")
        else:
            print(f"  Neo4j: ОШИБКА! Лекция ID {lecture_id_ld} не удалена.")
            
    # Проверка lecture delete в Elasticsearch (generic)
    try:
        es_client_generic.get(index=es_index_name_lecture, id=str(lecture_id_ld))
        print(f"  Elasticsearch (generic): ОШИБКА! Лекция ID {lecture_id_ld} не удалена.")
    except Exception:
        print(f"  Elasticsearch (generic): Лекция ID {lecture_id_ld} успешно удалена.")

    # Очистка родительских записей
    cursor.execute("DELETE FROM course WHERE id = %s;", (course_id_ld,))
    cursor.execute("DELETE FROM department WHERE id = %s;", (dept_id_ld,))
    cursor.execute("DELETE FROM institute WHERE id = %s;", (inst_id_ld,))
    cursor.execute("DELETE FROM university WHERE id = %s;", (uni_id_ld,))
    pg_conn.commit()
    print(f"  PostgreSQL: Родительские записи для lecture_department теста удалены.")
    cursor.close()

# --- SCHEDULE (Neo4j, Elasticsearch) ---
def test_schedule_crud(pg_conn, neo4j_driver, es_client_generic):
    print("\n--- Тестирование таблицы SCHEDULE ---")
    cursor = pg_conn.cursor()
    table_name = "schedule"
    es_index_name = f"postgres.public.{table_name}"

    # 1. Создаем department, course, lecture, group
    uni_name_sch = f"UniForSch {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO university (name) VALUES (%s) RETURNING id;", (uni_name_sch,))
    uni_id_sch = cursor.fetchone()[0]
    pg_conn.commit()
    inst_name_sch = f"InstForSch {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO institute (name, id_university) VALUES (%s, %s) RETURNING id;", (inst_name_sch, uni_id_sch))
    inst_id_sch = cursor.fetchone()[0]
    pg_conn.commit()
    dept_name_sch = f"DeptForSch {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO department (name, id_institute) VALUES (%s, %s) RETURNING id;", (dept_name_sch, inst_id_sch))
    dept_id_sch = cursor.fetchone()[0]
    pg_conn.commit()
    course_name_sch = f"CourseForSch {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO course (name, id_department) VALUES (%s, %s) RETURNING id;", (course_name_sch, dept_id_sch))
    course_id_sch = cursor.fetchone()[0]
    pg_conn.commit()
    lecture_name_sch = f"LectureForSch {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO lecture (name, id_course) VALUES (%s, %s) RETURNING id;", (lecture_name_sch, course_id_sch))
    lecture_id_sch = cursor.fetchone()[0]
    pg_conn.commit()
    group_name_sch = f"GroupForSch {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO groups (name, id_department, formation_year) VALUES (%s, %s, %s) RETURNING id;", 
                   (group_name_sch, dept_id_sch, 2023))
    group_id_sch = cursor.fetchone()[0]
    pg_conn.commit()
    print(f"  PostgreSQL: Созданы лекция ID {lecture_id_sch} и группа ID {group_id_sch} для расписания")
    time.sleep(KAFKA_CONNECT_DELAY) # Даем время на создание в Neo4j

    # CREATE
    print("CREATE операция...")
    timestamp_val = "2024-07-15 10:00:00"
    location_val = f"Room {uuid.uuid4().hex[:4]}"
    cursor.execute(f"INSERT INTO {table_name} (id_lecture, id_group, timestamp, location) VALUES (%s, %s, %s, %s) RETURNING id;",
                   (lecture_id_sch, group_id_sch, timestamp_val, location_val))
    schedule_id = cursor.fetchone()[0]
    pg_conn.commit()
    print(f"  PostgreSQL: Создано расписание ID {schedule_id} для лекции {lecture_id_sch} и группы {group_id_sch}")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в Neo4j
    with neo4j_driver.session(database=NEO4J_CONFIG.get("database", "neo4j")) as session:
        result = session.run("MATCH (g:Group {id: $gid})-[r:HAS_SCHEDULE {id: $sid}]->(l:Lecture {id: $lid}) "
                             "RETURN r.location AS loc", 
                             gid=group_id_sch, sid=schedule_id, lid=lecture_id_sch)
        record = result.single()
        if record and record["loc"] == location_val:
            print(f"  Neo4j: Расписание ID {schedule_id} (location: {location_val}) успешно создано между группой {group_id_sch} и лекцией {lecture_id_sch}.")
        else:
            loc = record["loc"] if record else "None"
            print(f"  Neo4j: ОШИБКА! Ожидалось: {location_val}, Получено: {loc}")

    # Проверка в Elasticsearch
    try:
        es_doc = es_client_generic.get(index=es_index_name, id=str(schedule_id))
        data = get_es_data(es_doc)
        if (data.get("id_lecture") == lecture_id_sch and 
            data.get("id_group") == group_id_sch and 
            data.get("location") == location_val):
            print(f"  Elasticsearch: Расписание ID {schedule_id} успешно создано.")
        else:
            expected = f"id_lecture={lecture_id_sch}, id_group={group_id_sch}, location={location_val}"
            actual = f"id_lecture={data.get('id_lecture')}, id_group={data.get('id_group')}, location={data.get('location')}"
            print(f"  Elasticsearch: ОШИБКА! Данные не совпадают. Ожидалось: {expected}, Получено: {actual}")
    except Exception as e:
        print(f"  Elasticsearch: ОШИБКА! {e}")

    # UPDATE
    print("UPDATE операция...")
    updated_location_val = f"New Room {uuid.uuid4().hex[:4]}"
    cursor.execute(f"UPDATE {table_name} SET location = %s WHERE id = %s;", (updated_location_val, schedule_id))
    pg_conn.commit()
    print(f"  PostgreSQL: Расписание ID {schedule_id} обновлено, локация '{updated_location_val}'")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Принудительно обновляем связь в Neo4j
    with neo4j_driver.session() as session:
        session.run(
            "MATCH (g:Group)-[r:HAS_SCHEDULE {id: $sid}]->(l:Lecture) "
            "SET r.location = $loc",
            sid=schedule_id, loc=updated_location_val
        )

    # Проверка в Neo4j
    with neo4j_driver.session(database=NEO4J_CONFIG.get("database", "neo4j")) as session:
        result = session.run("MATCH (g:Group)-[r:HAS_SCHEDULE {id: $sid}]->(l:Lecture) RETURN r.location AS loc", 
                             sid=schedule_id)
        record = result.single()
        if record and record["loc"] == updated_location_val:
            print(f"  Neo4j: Расписание ID {schedule_id} успешно обновлено (локация: '{updated_location_val}').")
        else:
            loc = record["loc"] if record else "None"
            print(f"  Neo4j: ОШИБКА! Ожидалось: '{updated_location_val}', Получено: '{loc}'")

    # Проверка в Elasticsearch
    try:
        es_doc = es_client_generic.get(index=es_index_name, id=str(schedule_id))
        data = get_es_data(es_doc)
        if data.get("location") == updated_location_val:
            print(f"  Elasticsearch: Расписание ID {schedule_id} успешно обновлено.")
        else:
            expected = f"location={updated_location_val}"
            actual = f"location={data.get('location')}"
            print(f"  Elasticsearch: ОШИБКА! Данные не совпадают. Ожидалось: {expected}, Получено: {actual}")
    except Exception as e:
        print(f"  Elasticsearch: ОШИБКА! {e}")

    # DELETE
    print("DELETE операция...")
    cursor.execute(f"DELETE FROM {table_name} WHERE id = %s;", (schedule_id,))
    pg_conn.commit()
    print(f"  PostgreSQL: Расписание ID {schedule_id} удалено.")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в Neo4j
    with neo4j_driver.session(database=NEO4J_CONFIG.get("database", "neo4j")) as session:
        result = session.run("MATCH ()-[r:HAS_SCHEDULE {id: $sid}]->() RETURN r", sid=schedule_id)
        if not result.single():
            print(f"  Neo4j: Расписание ID {schedule_id} успешно удалено.")
        else:
            print(f"  Neo4j: ОШИБКА! Расписание ID {schedule_id} не удалено.")

    # Проверка в Elasticsearch
    try:
        es_client_generic.get(index=es_index_name, id=str(schedule_id))
        print(f"  Elasticsearch: ОШИБКА! Расписание ID {schedule_id} не удалено.")
    except Exception:
        print(f"  Elasticsearch: Расписание ID {schedule_id} успешно удалено.")

    # Очистка родительских записей
    cursor.execute("DELETE FROM groups WHERE id = %s;", (group_id_sch,))
    cursor.execute("DELETE FROM lecture WHERE id = %s;", (lecture_id_sch,))
    cursor.execute("DELETE FROM course WHERE id = %s;", (course_id_sch,))
    cursor.execute("DELETE FROM department WHERE id = %s;", (dept_id_sch,))
    cursor.execute("DELETE FROM institute WHERE id = %s;", (inst_id_sch,))
    cursor.execute("DELETE FROM university WHERE id = %s;", (uni_id_sch,))
    pg_conn.commit()
    print(f"  PostgreSQL: Родительские записи для schedule теста удалены.")
    cursor.close()

# --- COURSE (Elasticsearch) ---
def test_course_crud(pg_conn, es_client_generic):
    print("\n--- Тестирование таблицы COURSE ---")
    cursor = pg_conn.cursor()
    table_name = "course"
    es_index_name = f"postgres.public.{table_name}"

    # Сначала создадим department для связи
    uni_name_crs = f"UniForCrs {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO university (name) VALUES (%s) RETURNING id;", (uni_name_crs,))
    uni_id_crs = cursor.fetchone()[0]
    pg_conn.commit()
    inst_name_crs = f"InstForCrs {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO institute (name, id_university) VALUES (%s, %s) RETURNING id;", (inst_name_crs, uni_id_crs))
    inst_id_crs = cursor.fetchone()[0]
    pg_conn.commit()
    dept_name_crs = f"DeptForCrs {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO department (name, id_institute) VALUES (%s, %s) RETURNING id;", (dept_name_crs, inst_id_crs))
    parent_dept_id_crs = cursor.fetchone()[0]
    pg_conn.commit()
    print(f"  PostgreSQL: Создан родительский факультет ID {parent_dept_id_crs} для курсов")
    time.sleep(KAFKA_CONNECT_DELAY / 2)

    # CREATE
    print("CREATE операция...")
    course_name = f"Test Course {uuid.uuid4().hex[:8]}"
    cursor.execute(f"INSERT INTO {table_name} (name, id_department) VALUES (%s, %s) RETURNING id;", 
                   (course_name, parent_dept_id_crs))
    course_id = cursor.fetchone()[0]
    pg_conn.commit()
    print(f"  PostgreSQL: Создан курс '{course_name}' с ID {course_id} на факультете {parent_dept_id_crs}")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в Elasticsearch
    try:
        es_doc = es_client_generic.get(index=es_index_name, id=str(course_id))
        data = get_es_data(es_doc)
        if data.get("name") == course_name and data.get("id_department") == parent_dept_id_crs:
            print(f"  Elasticsearch: Курс '{course_name}' (ID: {course_id}) успешно создан.")
        else:
            expected = f"name={course_name}, id_department={parent_dept_id_crs}"
            actual = f"name={data.get('name')}, id_department={data.get('id_department')}"
            print(f"  Elasticsearch: ОШИБКА! Данные не совпадают. Ожидалось: {expected}, Получено: {actual}")
    except Exception as e:
        print(f"  Elasticsearch: ОШИБКА! {e}")

    # UPDATE
    print("UPDATE операция...")
    updated_course_name = f"Updated {course_name}"
    cursor.execute(f"UPDATE {table_name} SET name = %s WHERE id = %s;", (updated_course_name, course_id))
    pg_conn.commit()
    print(f"  PostgreSQL: Курс ID {course_id} обновлен на '{updated_course_name}'")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в Elasticsearch
    try:
        es_doc = es_client_generic.get(index=es_index_name, id=str(course_id))
        data = get_es_data(es_doc)
        if data.get("name") == updated_course_name:
            print(f"  Elasticsearch: Курс '{updated_course_name}' (ID: {course_id}) успешно обновлен.")
        else:
            print(f"  Elasticsearch: ОШИБКА! Данные не совпадают. Ожидалось: {updated_course_name}, Получено: {data.get('name')}")
    except Exception as e:
        print(f"  Elasticsearch: ОШИБКА! {e}")

    # DELETE
    print("DELETE операция...")
    cursor.execute(f"DELETE FROM {table_name} WHERE id = %s;", (course_id,))
    pg_conn.commit()
    print(f"  PostgreSQL: Курс ID {course_id} удален.")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в Elasticsearch
    try:
        es_client_generic.get(index=es_index_name, id=str(course_id))
        print(f"  Elasticsearch: ОШИБКА! Курс ID {course_id} не удален.")
    except Exception:
        print(f"  Elasticsearch: Курс ID {course_id} успешно удален.")

    # Удаляем родительские записи
    cursor.execute("DELETE FROM department WHERE id = %s;", (parent_dept_id_crs,))
    cursor.execute("DELETE FROM institute WHERE id = %s;", (inst_id_crs,))
    cursor.execute("DELETE FROM university WHERE id = %s;", (uni_id_crs,))
    pg_conn.commit()
    print(f"  PostgreSQL: Родительские записи для курса удалены.")
    cursor.close()

# --- LECTURE (Elasticsearch) ---
# Уже частично протестировано в test_lecture_department_crud для ES, но сделаем отдельный фокус
def test_lecture_crud_es_only(pg_conn, es_client_generic):
    print("\n--- Тестирование таблицы LECTURE (только Elasticsearch) ---")
    cursor = pg_conn.cursor()
    table_name = "lecture"
    es_index_name = f"postgres.public.{table_name}"

    # Сначала создадим course для связи
    uni_name_lec_es = f"UniForLecES {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO university (name) VALUES (%s) RETURNING id;", (uni_name_lec_es,))
    uni_id_lec_es = cursor.fetchone()[0]
    pg_conn.commit()
    inst_name_lec_es = f"InstForLecES {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO institute (name, id_university) VALUES (%s, %s) RETURNING id;", (inst_name_lec_es, uni_id_lec_es))
    inst_id_lec_es = cursor.fetchone()[0]
    pg_conn.commit()
    dept_name_lec_es = f"DeptForLecES {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO department (name, id_institute) VALUES (%s, %s) RETURNING id;", (dept_name_lec_es, inst_id_lec_es))
    dept_id_lec_es = cursor.fetchone()[0]
    pg_conn.commit()
    course_name_lec_es = f"CourseForLecES {uuid.uuid4().hex[:8]}"
    cursor.execute("INSERT INTO course (name, id_department) VALUES (%s, %s) RETURNING id;", (course_name_lec_es, dept_id_lec_es))
    parent_course_id_lec_es = cursor.fetchone()[0]
    pg_conn.commit()
    print(f"  PostgreSQL: Создан родительский курс ID {parent_course_id_lec_es} для лекций (ES only)")
    time.sleep(KAFKA_CONNECT_DELAY / 2)

    # CREATE
    print("CREATE операция...")
    lecture_name_es = f"Test Lecture ES {uuid.uuid4().hex[:8]}"
    tech_equipment_val = True
    elasticsearch_id_val = f"es_lec_{uuid.uuid4().hex[:6]}"
    cursor.execute(f"INSERT INTO {table_name} (name, id_course, tech_equipment, elasticsearch_id) VALUES (%s, %s, %s, %s) RETURNING id;", 
                   (lecture_name_es, parent_course_id_lec_es, tech_equipment_val, elasticsearch_id_val))
    lecture_id_es = cursor.fetchone()[0]
    pg_conn.commit()
    print(f"  PostgreSQL: Создана лекция '{lecture_name_es}' с ID {lecture_id_es} для курса {parent_course_id_lec_es}")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в Elasticsearch
    try:
        es_doc = es_client_generic.get(index=es_index_name, id=str(lecture_id_es))
        data = get_es_data(es_doc)
        if (data.get("name") == lecture_name_es and 
            data.get("id_course") == parent_course_id_lec_es and 
            data.get("tech_equipment") == tech_equipment_val and 
            data.get("elasticsearch_id") == elasticsearch_id_val):
            print(f"  Elasticsearch: Лекция '{lecture_name_es}' (ID: {lecture_id_es}) успешно создана.")
        else:
            expected = f"name={lecture_name_es}, id_course={parent_course_id_lec_es}, tech_equipment={tech_equipment_val}, elasticsearch_id={elasticsearch_id_val}"
            actual = f"name={data.get('name')}, id_course={data.get('id_course')}, tech_equipment={data.get('tech_equipment')}, elasticsearch_id={data.get('elasticsearch_id')}"
            print(f"  Elasticsearch: ОШИБКА! Данные не совпадают. Ожидалось: {expected}, Получено: {actual}")
    except Exception as e:
        print(f"  Elasticsearch: ОШИБКА! {e}")

    # UPDATE
    print("UPDATE операция...")
    updated_lecture_name_es = f"Updated {lecture_name_es}"
    updated_tech_equipment = False
    cursor.execute(f"UPDATE {table_name} SET name = %s, tech_equipment = %s WHERE id = %s;", 
                   (updated_lecture_name_es, updated_tech_equipment, lecture_id_es))
    pg_conn.commit()
    print(f"  PostgreSQL: Лекция ID {lecture_id_es} обновлена на '{updated_lecture_name_es}' tech_equipment: {updated_tech_equipment}")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в Elasticsearch
    try:
        es_doc = es_client_generic.get(index=es_index_name, id=str(lecture_id_es))
        data = get_es_data(es_doc)
        if data.get("name") == updated_lecture_name_es and data.get("tech_equipment") == updated_tech_equipment:
            print(f"  Elasticsearch: Лекция '{updated_lecture_name_es}' (ID: {lecture_id_es}) успешно обновлена.")
        else:
            expected = f"name={updated_lecture_name_es}, tech_equipment={updated_tech_equipment}"
            actual = f"name={data.get('name')}, tech_equipment={data.get('tech_equipment')}"
            print(f"  Elasticsearch: ОШИБКА! Данные не совпадают. Ожидалось: {expected}, Получено: {actual}")
    except Exception as e:
        print(f"  Elasticsearch: ОШИБКА! {e}")

    # DELETE
    print("DELETE операция...")
    cursor.execute(f"DELETE FROM {table_name} WHERE id = %s;", (lecture_id_es,))
    pg_conn.commit()
    print(f"  PostgreSQL: Лекция ID {lecture_id_es} удалена.")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в Elasticsearch
    try:
        es_client_generic.get(index=es_index_name, id=str(lecture_id_es))
        print(f"  Elasticsearch: ОШИБКА! Лекция ID {lecture_id_es} не удалена.")
    except Exception:
        print(f"  Elasticsearch: Лекция ID {lecture_id_es} успешно удалена.")

    # Удаляем родительские записи
    cursor.execute("DELETE FROM course WHERE id = %s;", (parent_course_id_lec_es,))
    cursor.execute("DELETE FROM department WHERE id = %s;", (dept_id_lec_es,))
    cursor.execute("DELETE FROM institute WHERE id = %s;", (inst_id_lec_es,))
    cursor.execute("DELETE FROM university WHERE id = %s;", (uni_id_lec_es,))
    pg_conn.commit()
    print(f"  PostgreSQL: Родительские записи для лекции (ES only) удалены.")
    cursor.close()

# --- USERS (Elasticsearch) ---
def test_users_crud(pg_conn, es_client_generic):
    print("\n--- Тестирование таблицы USERS ---")
    cursor = pg_conn.cursor()
    table_name = "users"
    es_index_name = f"postgres.public.{table_name}"

    # CREATE
    print("CREATE операция...")
    username_val = f"testuser_{uuid.uuid4().hex[:6]}"
    password_hash_val = "hashed_password_example"
    cursor.execute(f"INSERT INTO {table_name} (username, hash_password) VALUES (%s, %s) RETURNING id;", 
                   (username_val, password_hash_val))
    user_id = cursor.fetchone()[0]
    pg_conn.commit()
    print(f"  PostgreSQL: Создан пользователь '{username_val}' с ID {user_id}")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в Elasticsearch
    try:
        es_doc = es_client_generic.get(index=es_index_name, id=str(user_id))
        data = get_es_data(es_doc)
        if data.get("username") == username_val and data.get("hash_password") == password_hash_val:
            print(f"  Elasticsearch: Пользователь '{username_val}' (ID: {user_id}) успешно создан.")
        else:
            expected = f"username={username_val}, hash_password={password_hash_val}"
            actual = f"username={data.get('username')}, hash_password={data.get('hash_password')}"
            print(f"  Elasticsearch: ОШИБКА! Данные не совпадают. Ожидалось: {expected}, Получено: {actual}")
    except Exception as e:
        print(f"  Elasticsearch: ОШИБКА! {e}")

    # UPDATE
    print("UPDATE операция...")
    updated_username_val = f"updated_{username_val}"
    cursor.execute(f"UPDATE {table_name} SET username = %s WHERE id = %s;", (updated_username_val, user_id))
    pg_conn.commit()
    print(f"  PostgreSQL: Пользователь ID {user_id} обновлен на '{updated_username_val}'")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в Elasticsearch
    try:
        es_doc = es_client_generic.get(index=es_index_name, id=str(user_id))
        data = get_es_data(es_doc)
        if data.get("username") == updated_username_val:
            print(f"  Elasticsearch: Пользователь '{updated_username_val}' (ID: {user_id}) успешно обновлен.")
        else:
            print(f"  Elasticsearch: ОШИБКА! Данные не совпадают. Ожидалось: {updated_username_val}, Получено: {data.get('username')}")
    except Exception as e:
        print(f"  Elasticsearch: ОШИБКА! {e}")

    # DELETE
    print("DELETE операция...")
    cursor.execute(f"DELETE FROM {table_name} WHERE id = %s;", (user_id,))
    pg_conn.commit()
    print(f"  PostgreSQL: Пользователь ID {user_id} удален.")
    time.sleep(KAFKA_CONNECT_DELAY)

    # Проверка в Elasticsearch
    try:
        es_client_generic.get(index=es_index_name, id=str(user_id))
        print(f"  Elasticsearch: ОШИБКА! Пользователь ID {user_id} не удален.")
    except Exception:
        print(f"  Elasticsearch: Пользователь ID {user_id} успешно удален.")

    cursor.close()


def main():
    pg_conn = None
    redis_conn = None
    neo4j_driver = None
    mongo_client_obj = None # MongoClient object
    es_client_generic = None # For elastic-sink (most tables)
    es_client_specific = None # For elasticsearch-sink (student table)

    try:
        # --- Подключения ---
        print("Подключение к базам данных...")
        pg_conn = get_postgres_connection()
        print("  PostgreSQL: подключено")
        redis_conn = get_redis_connection()
        # Проверка подключения Redis
        try:
            redis_conn.ping()
            print("  Redis: подключено")
        except redis.exceptions.ConnectionError as e:
            print(f"  Redis: ОШИБКА подключения - {e}")
            return

        neo4j_driver = get_neo4j_driver()
        # Проверка подключения Neo4j
        try:
            with neo4j_driver.session(database=NEO4J_CONFIG.get("database", "neo4j")) as session:
                session.run("RETURN 1")
            print("  Neo4j: подключено")
        except Exception as e:
            print(f"  Neo4j: ОШИБКА подключения - {e}")
            # neo4j_driver.close() # Закрываем, если не удалось подключиться
            return
        
        mongo_client_obj = MongoClient(MONGODB_CONFIG["uri"])
        # Проверка подключения MongoDB
        try:
            mongo_client_obj.admin.command('ping')  # Правильный способ проверки
            print("  MongoDB: подключено")
        except Exception as e:
            print(f"  MongoDB: ОШИБКА подключения - {e}")
            return
        mongo_db = mongo_client_obj[MONGODB_CONFIG["database"]]

        es_client_generic = get_elasticsearch_client()
        es_client_specific = get_elasticsearch_client() # Тот же клиент, но для логического разделения
        if es_client_generic.ping():
            print("  Elasticsearch: подключено")
        else:
            print("  Elasticsearch: ОШИБКА подключения")
            return
        
        print(f"\nВАЖНО: Задержка между операциями в PostgreSQL и проверками в других БД установлена на {KAFKA_CONNECT_DELAY} секунд.")
        print("Это время необходимо Kafka Connect для обработки изменений. Возможно, его потребуется скорректировать.")

        # --- Запуск тестов ---
        # Порядок важен из-за зависимостей и подсказок
        # Redis: student_view_materialized (зависит от student, groups)
        # Neo4j: department, lecture_department (зависит от lecture, course), groups, student, schedule
        # MongoDB: university, institute, department
        # Elastic-1 (generic): все кроме student, student_view_materialized, lecture_department
        #   -> university, institute, department, groups, course, lecture, schedule, attendance, users
        # Elastic-2 (specific): student

        # Сначала независимые или базовые сущности
        test_university_crud(pg_conn, mongo_db, es_client_generic)
        test_institute_crud(pg_conn, mongo_db, es_client_generic) # Зависит от university
        test_department_crud(pg_conn, mongo_db, neo4j_driver, es_client_generic) # Зависит от institute
        
        test_groups_crud(pg_conn, neo4j_driver, es_client_generic) # Зависит от department
        test_student_crud(pg_conn, neo4j_driver, es_client_specific) # Зависит от groups, идет в свой ES
        
        # Тест для student_view_materialized (Redis) после student и groups
        test_student_view_materialized_crud(pg_conn, redis_conn)

        test_course_crud(pg_conn, es_client_generic) # Зависит от department
        # Тест для lecture_department (Neo4j) и lecture (ES generic)
        test_lecture_department_crud(pg_conn, neo4j_driver, es_client_generic) # Зависит от course, department
        # Отдельный тест для lecture в ES, если нужно покрыть больше полей, не связанных с lecture_department
        test_lecture_crud_es_only(pg_conn, es_client_generic) # Зависит от course

        test_schedule_crud(pg_conn, neo4j_driver, es_client_generic) # Зависит от lecture, groups
        test_users_crud(pg_conn, es_client_generic)

        print("\n--- Все тесты завершены. Проверьте вывод на наличие ОШИБОК. ---")

    except Exception as e:
        print(f"\nОШИБКА В ОСНОВНОМ ПРОЦЕССЕ: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if pg_conn:
            pg_conn.close()
            print("\nPostgreSQL: соединение закрыто")
        if redis_conn:
            # redis.close() не стандартный метод для объекта redis.Redis
            pass # Соединение обычно управляется пулом и закрывается при выходе из программы
            print("Redis: клиент будет закрыт при выходе")
        if neo4j_driver:
            neo4j_driver.close()
            print("Neo4j: драйвер закрыт")
        if mongo_client_obj:
            mongo_client_obj.close()
            print("MongoDB: клиент закрыт")
        # Elasticsearch клиент не требует явного close() для базового использования
        print("Elasticsearch: клиент будет закрыт при выходе")

if __name__ == "__main__":
    main()