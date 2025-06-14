from faker import Faker
fake = Faker("ru_RU")

import psycopg2
import redis
from neo4j import GraphDatabase
from pymongo import MongoClient
from elasticsearch import Elasticsearch
import time

PG_CONN_PARAMS = {
    "host": "postgres",
    "port": 5432,
    "user": "admin",
    "password": "secret",
    "dbname": "mydb",
}

MONGO_CONN_STRING = "mongodb://mongo:27017/"
NEO4J_URI = "bolt://neo4j:7687"
NEO4J_AUTH = None  # if NEO4J_AUTH=none
REDIS_HOST = "redis"
REDIS_PORT = 6379
ES_HOSTS = ["http://elasticsearch:9200"]

def connect_pg():
    try:
        conn = psycopg2.connect(**PG_CONN_PARAMS)
        print("Connected to PostgreSQL successfully!")
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def connect_redis():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        print("Connected to Redis successfully!")
        return r
    except Exception as e:
        print(f"Error connecting to Redis: {e}")
        return None

def connect_neo4j():
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
        driver.verify_connectivity()
        print("Connected to Neo4j successfully!")
        return driver
    except Exception as e:
        print(f"Error connecting to Neo4j: {e}")
        return None

def connect_mongo():
    try:
        client = MongoClient(MONGO_CONN_STRING)
        client.admin.command('ping')
        print("Connected to MongoDB successfully!")
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

def connect_es():
    try:
        es = Elasticsearch(ES_HOSTS)
        es.ping()
        print("Connected to Elasticsearch successfully!")
        return es
    except Exception as e:
        print(f"Error connecting to Elasticsearch: {e}")
        return None

if __name__ == "__main__":
    pg_conn = connect_pg()
    redis_client = connect_redis()
    neo4j_driver = connect_neo4j()
    mongo_client = connect_mongo()
    es_client = connect_es()

    if pg_conn: pg_conn.close()
    if neo4j_driver: neo4j_driver.close()
    if mongo_client: mongo_client.close()

def generate_unique_student_data():
    student_number = fake.unique.uuid4()
    fullname = fake.name()
    email = fake.email()
    redis_key = f"student:{student_number}"
    return student_number, fullname, email, redis_key

def create_student_pg(pg_conn, id_group):
    student_number, fullname, email, redis_key = generate_unique_student_data()
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO student (student_number, fullname, email, id_group, redis_key) VALUES (%s, %s, %s, %s, %s)",
                (student_number, fullname, email, id_group, redis_key)
            )
            pg_conn.commit()
        print(f"Inserted student {fullname} ({student_number}) into PostgreSQL.")
        return student_number, fullname, email, id_group, redis_key
    except Exception as e:
        print(f"Error inserting student into PostgreSQL: {e}")
        pg_conn.rollback()
        return None

def verify_redis_student(redis_client, redis_key, expected_fullname, expected_email, expected_group_id, expected_group_name):
    try:
        data = redis_client.json().get(redis_key)
        if data:
            if (
                data.get("fullname") == expected_fullname and
                data.get("email") == expected_email and
                data.get("group_id") == expected_group_id and
                data.get("group_name") == expected_group_name
            ):
                print(f"Redis verification successful for {redis_key}.")
                return True
            else:
                print(f"Redis verification failed for {redis_key}. Expected: {{'fullname': '{expected_fullname}', 'email': '{expected_email}', 'group_id': {expected_group_id}, 'group_name': '{expected_group_name}'}}, Got: {data}")
                return False
        else:
            print(f"Redis verification failed for {redis_key}. Key not found.")
            return False
    except Exception as e:
        print(f"Error verifying Redis: {e}")
        return False

def get_existing_group_id(pg_conn):
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT id, name FROM groups LIMIT 1")
            result = cur.fetchone()
            if result:
                return result[0], result[1]
            else:
                print("No groups found in PostgreSQL. Please populate data first.")
                return None, None
    except Exception as e:
        print(f"Error getting group ID from PostgreSQL: {e}")
        return None, None

if __name__ == "__main__":
    pg_conn = connect_pg()
    redis_client = connect_redis()
    neo4j_driver = connect_neo4j()
    mongo_client = connect_mongo()
    es_client = connect_es()

    if pg_conn and redis_client:
        group_id, group_name = get_existing_group_id(pg_conn)
        if group_id:
            print("\n--- Testing Redis Connector (Create) ---")
            student_data = create_student_pg(pg_conn, group_id)
            if student_data:
                student_number, fullname, email, id_group, redis_key = student_data
                time.sleep(5)  # Give Kafka Connect time to process
                verify_redis_student(redis_client, redis_key, fullname, email, id_group, group_name)

    if pg_conn: pg_conn.close()
    if neo4j_driver: neo4j_driver.close()
    if mongo_client: mongo_client.close()

def update_student_pg(pg_conn, student_number, new_fullname, new_email):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "UPDATE student SET fullname = %s, email = %s WHERE student_number = %s",
                (new_fullname, new_email, student_number)
            )
            pg_conn.commit()
        print(f"Updated student {student_number} in PostgreSQL.")
        return True
    except Exception as e:
        print(f"Error updating student in PostgreSQL: {e}")
        pg_conn.rollback()
        return False

def delete_student_pg(pg_conn, student_number):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM student WHERE student_number = %s",
                (student_number,)
            )
            pg_conn.commit()
        print(f"Deleted student {student_number} from PostgreSQL.")
        return True
    except Exception as e:
        print(f"Error deleting student from PostgreSQL: {e}")
        pg_conn.rollback()
        return False

def verify_redis_student_deleted(redis_client, redis_key):
    try:
        data = redis_client.json().get(redis_key)
        if data is None:
            print(f"Redis verification successful: {redis_key} deleted.")
            return True
        else:
            print(f"Redis verification failed: {redis_key} still exists.")
            return False
    except Exception as e:
        print(f"Error verifying Redis deletion: {e}")
        return False

if __name__ == "__main__":
    pg_conn = connect_pg()
    redis_client = connect_redis()
    neo4j_driver = connect_neo4j()
    mongo_client = connect_mongo()
    es_client = connect_es()

    if pg_conn and redis_client:
        group_id, group_name = get_existing_group_id(pg_conn)
        if group_id:
            print("\n--- Testing Redis Connector (Create) ---")
            student_data = create_student_pg(pg_conn, group_id)
            if student_data:
                student_number, fullname, email, id_group, redis_key = student_data
                time.sleep(5)  # Give Kafka Connect time to process
                verify_redis_student(redis_client, redis_key, fullname, email, id_group, group_name)

                print("\n--- Testing Redis Connector (Update) ---")
                new_fullname = fake.name()
                new_email = fake.email()
                if update_student_pg(pg_conn, student_number, new_fullname, new_email):
                    time.sleep(5)
                    verify_redis_student(redis_client, redis_key, new_fullname, new_email, id_group, group_name)

                print("\n--- Testing Redis Connector (Delete) ---")
                if delete_student_pg(pg_conn, student_number):
                    time.sleep(5)
                    verify_redis_student_deleted(redis_client, redis_key)

    if pg_conn: pg_conn.close()
    if neo4j_driver: neo4j_driver.close()
    if mongo_client: mongo_client.close()

def create_department_pg(pg_conn, name, id_institute):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO department (name, id_institute) VALUES (%s, %s) RETURNING id",
                (name, id_institute)
            )
            department_id = cur.fetchone()[0]
            pg_conn.commit()
        print(f"Inserted department {name} ({department_id}) into PostgreSQL.")
        return department_id
    except Exception as e:
        print(f"Error inserting department into PostgreSQL: {e}")
        pg_conn.rollback()
        return None

def update_department_pg(pg_conn, department_id, new_name):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "UPDATE department SET name = %s WHERE id = %s",
                (new_name, department_id)
            )
            pg_conn.commit()
        print(f"Updated department {department_id} in PostgreSQL.")
        return True
    except Exception as e:
        print(f"Error updating department in PostgreSQL: {e}")
        pg_conn.rollback()
        return False

def delete_department_pg(pg_conn, department_id):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM department WHERE id = %s",
                (department_id,)
            )
            pg_conn.commit()
        print(f"Deleted department {department_id} from PostgreSQL.")
        return True
    except Exception as e:
        print(f"Error deleting department from PostgreSQL: {e}")
        pg_conn.rollback()
        return False

def verify_neo4j_department(neo4j_driver, department_id, expected_name):
    try:
        with neo4j_driver.session() as session:
            result = session.run(
                "MATCH (d:Department {id: $department_id}) RETURN d.name AS name",
                department_id=department_id
            )
            record = result.single()
            if record:
                if record["name"] == expected_name:
                    print(f"Neo4j verification successful for Department {department_id}.")
                    return True
                else:
                    print(f"Neo4j verification failed for Department {department_id}. Expected: {expected_name}, Got: {record['name']}")
                    return False
            else:
                print(f"Neo4j verification failed for Department {department_id}. Node not found.")
                return False
    except Exception as e:
        print(f"Error verifying Neo4j Department: {e}")
        return False

def verify_neo4j_department_deleted(neo4j_driver, department_id):
    try:
        with neo4j_driver.session() as session:
            result = session.run(
                "MATCH (d:Department {id: $department_id}) RETURN d",
                department_id=department_id
            )
            if not result.single():
                print(f"Neo4j verification successful: Department {department_id} deleted.")
                return True
            else:
                print(f"Neo4j verification failed: Department {department_id} still exists.")
                return False
    except Exception as e:
        print(f"Error verifying Neo4j Department deletion: {e}")
        return False

def get_existing_institute_id(pg_conn):
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT id FROM institute LIMIT 1")
            result = cur.fetchone()
            if result:
                return result[0]
            else:
                print("No institutes found in PostgreSQL. Please populate data first.")
                return None
    except Exception as e:
        print(f"Error getting institute ID from PostgreSQL: {e}")
        return None

if __name__ == "__main__":
    pg_conn = connect_pg()
    redis_client = connect_redis()
    neo4j_driver = connect_neo4j()
    mongo_client = connect_mongo()
    es_client = connect_es()

    if pg_conn and redis_client:
        group_id, group_name = get_existing_group_id(pg_conn)
        if group_id:
            print("\n--- Testing Redis Connector (Create) ---")
            student_data = create_student_pg(pg_conn, group_id)
            if student_data:
                student_number, fullname, email, id_group, redis_key = student_data
                time.sleep(5)  # Give Kafka Connect time to process
                verify_redis_student(redis_client, redis_key, fullname, email, id_group, group_name)

                print("\n--- Testing Redis Connector (Update) ---")
                new_fullname = fake.name()
                new_email = fake.email()
                if update_student_pg(pg_conn, student_number, new_fullname, new_email):
                    time.sleep(5)
                    verify_redis_student(redis_client, redis_key, new_fullname, new_email, id_group, group_name)

                print("\n--- Testing Redis Connector (Delete) ---")
                if delete_student_pg(pg_conn, student_number):
                    time.sleep(5)
                    verify_redis_student_deleted(redis_client, redis_key)

    if pg_conn and neo4j_driver:
        institute_id = get_existing_institute_id(pg_conn)
        if institute_id:
            print("\n--- Testing Neo4j Connector (Department - Create) ---")
            department_name = fake.word() + " Department"
            department_id = create_department_pg(pg_conn, department_name, institute_id)
            if department_id:
                time.sleep(5)
                verify_neo4j_department(neo4j_driver, department_id, department_name)

                print("\n--- Testing Neo4j Connector (Department - Update) ---")
                new_department_name = fake.word() + " Updated Department"
                if update_department_pg(pg_conn, department_id, new_department_name):
                    time.sleep(5)
                    verify_neo4j_department(neo4j_driver, department_id, new_department_name)

                print("\n--- Testing Neo4j Connector (Department - Delete) ---")
                if delete_department_pg(pg_conn, department_id):
                    time.sleep(5)
                    verify_neo4j_department_deleted(neo4j_driver, department_id)

    if pg_conn: pg_conn.close()
    if neo4j_driver: neo4j_driver.close()
    if mongo_client: mongo_client.close()

def create_university_pg(pg_conn, name):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO university (name) VALUES (%s) RETURNING id",
                (name,)
            )
            university_id = cur.fetchone()[0]
            pg_conn.commit()
        print(f"Inserted university {name} ({university_id}) into PostgreSQL.")
        return university_id
    except Exception as e:
        print(f"Error inserting university into PostgreSQL: {e}")
        pg_conn.rollback()
        return None

def update_university_pg(pg_conn, university_id, new_name):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "UPDATE university SET name = %s WHERE id = %s",
                (new_name, university_id)
            )
            pg_conn.commit()
        print(f"Updated university {university_id} in PostgreSQL.")
        return True
    except Exception as e:
        print(f"Error updating university in PostgreSQL: {e}")
        pg_conn.rollback()
        return False

def delete_university_pg(pg_conn, university_id):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM university WHERE id = %s",
                (university_id,)
            )
            pg_conn.commit()
        print(f"Deleted university {university_id} from PostgreSQL.")
        return True
    except Exception as e:
        print(f"Error deleting university from PostgreSQL: {e}")
        pg_conn.rollback()
        return False

def verify_mongo_university(mongo_client, university_id, expected_name):
    try:
        db = mongo_client.university
        collection = db.universities
        doc = collection.find_one({"id": university_id})
        if doc:
            if doc.get("name") == expected_name:
                print(f"MongoDB verification successful for University {university_id}.")
                return True
            else:
                print(f"MongoDB verification failed for University {university_id}. Expected: {expected_name}, Got: {doc.get('name')}")
                return False
        else:
            print(f"MongoDB verification failed for University {university_id}. Document not found.")
            return False
    except Exception as e:
        print(f"Error verifying MongoDB University: {e}")
        return False

def verify_mongo_university_deleted(mongo_client, university_id):
    try:
        db = mongo_client.university
        collection = db.universities
        doc = collection.find_one({"id": university_id})
        if doc is None:
            print(f"MongoDB verification successful: University {university_id} deleted.")
            return True
        else:
            print(f"MongoDB verification failed: University {university_id} still exists.")
            return False
    except Exception as e:
        print(f"Error verifying MongoDB University deletion: {e}")
        return False

def create_institute_pg(pg_conn, name, id_university):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO institute (name, id_university) VALUES (%s, %s) RETURNING id",
                (name, id_university)
            )
            institute_id = cur.fetchone()[0]
            pg_conn.commit()
        print(f"Inserted institute {name} ({institute_id}) into PostgreSQL.")
        return institute_id
    except Exception as e:
        print(f"Error inserting institute into PostgreSQL: {e}")
        pg_conn.rollback()
        return None

def update_institute_pg(pg_conn, institute_id, new_name):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "UPDATE institute SET name = %s WHERE id = %s",
                (new_name, institute_id)
            )
            pg_conn.commit()
        print(f"Updated institute {institute_id} in PostgreSQL.")
        return True
    except Exception as e:
        print(f"Error updating institute in PostgreSQL: {e}")
        pg_conn.rollback()
        return False

def delete_institute_pg(pg_conn, institute_id):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM institute WHERE id = %s",
                (institute_id,)
            )
            pg_conn.commit()
        print(f"Deleted institute {institute_id} from PostgreSQL.")
        return True
    except Exception as e:
        print(f"Error deleting institute from PostgreSQL: {e}")
        pg_conn.rollback()
        return False

def verify_mongo_institute(mongo_client, university_id, institute_id, expected_name):
    try:
        db = mongo_client.university
        collection = db.universities
        doc = collection.find_one({"id": university_id, "institutes.id": institute_id}, {"institutes.$": 1})
        if doc and "institutes" in doc and len(doc["institutes"]) > 0:
            institute_doc = doc["institutes"][0]
            if institute_doc.get("name") == expected_name:
                print(f"MongoDB verification successful for Institute {institute_id} under University {university_id}.")
                return True
            else:
                print(f"MongoDB verification failed for Institute {institute_id}. Expected: {expected_name}, Got: {institute_doc.get('name')}")
                return False
        else:
            print(f"MongoDB verification failed for Institute {institute_id} under University {university_id}. Document not found.")
            return False
    except Exception as e:
        print(f"Error verifying MongoDB Institute: {e}")
        return False

def verify_mongo_institute_deleted(mongo_client, university_id, institute_id):
    try:
        db = mongo_client.university
        collection = db.universities
        doc = collection.find_one({"id": university_id, "institutes.id": institute_id})
        if doc is None or "institutes" not in doc or not any(inst["id"] == institute_id for inst in doc["institutes"]):
            print(f"MongoDB verification successful: Institute {institute_id} deleted from University {university_id}.")
            return True
        else:
            print(f"MongoDB verification failed: Institute {institute_id} still exists under University {university_id}.")
            return False
    except Exception as e:
        print(f"Error verifying MongoDB Institute deletion: {e}")
        return False

def verify_mongo_department(mongo_client, university_id, institute_id, department_id, expected_name):
    try:
        db = mongo_client.university
        collection = db.universities
        doc = collection.find_one({"id": university_id, "institutes.id": institute_id, "institutes.departments.id": department_id}, {"institutes.$": 1})
        if doc and "institutes" in doc and len(doc["institutes"]) > 0:
            institute_doc = doc["institutes"][0]
            if "departments" in institute_doc and len(institute_doc["departments"]) > 0:
                department_doc = next((d for d in institute_doc["departments"] if d["id"] == department_id), None)
                if department_doc:
                    if department_doc.get("name") == expected_name:
                        print(f"MongoDB verification successful for Department {department_id} under Institute {institute_id} and University {university_id}.")
                        return True
                    else:
                        print(f"MongoDB verification failed for Department {department_id}. Expected: {expected_name}, Got: {department_doc.get('name')}")
                        return False
        print(f"MongoDB verification failed for Department {department_id} under Institute {institute_id} and University {university_id}. Document not found.")
        return False
    except Exception as e:
        print(f"Error verifying MongoDB Department: {e}")
        return False

def verify_mongo_department_deleted(mongo_client, university_id, institute_id, department_id):
    try:
        db = mongo_client.university
        collection = db.universities
        doc = collection.find_one({"id": university_id, "institutes.id": institute_id, "institutes.departments.id": department_id})
        if doc is None or "institutes" not in doc or not any(inst["id"] == institute_id for inst in doc["institutes"]) or not any(dept["id"] == department_id for inst in doc["institutes"] for dept in inst["departments"]):
            print(f"MongoDB verification successful: Department {department_id} deleted from Institute {institute_id} and University {university_id}.")
            return True
        else:
            print(f"MongoDB verification failed: Department {department_id} still exists under Institute {institute_id} and University {university_id}.")
            return False
    except Exception as e:
        print(f"Error verifying MongoDB Department deletion: {e}")
        return False

if __name__ == "__main__":
    pg_conn = connect_pg()
    redis_client = connect_redis()
    neo4j_driver = connect_neo4j()
    mongo_client = connect_mongo()
    es_client = connect_es()

    if pg_conn and redis_client:
        group_id, group_name = get_existing_group_id(pg_conn)
        if group_id:
            print("\n--- Testing Redis Connector (Create) ---")
            student_data = create_student_pg(pg_conn, group_id)
            if student_data:
                student_number, fullname, email, id_group, redis_key = student_data
                time.sleep(5)  # Give Kafka Connect time to process
                verify_redis_student(redis_client, redis_key, fullname, email, id_group, group_name)

                print("\n--- Testing Redis Connector (Update) ---")
                new_fullname = fake.name()
                new_email = fake.email()
                if update_student_pg(pg_conn, student_number, new_fullname, new_email):
                    time.sleep(5)
                    verify_redis_student(redis_client, redis_key, new_fullname, new_email, id_group, group_name)

                print("\n--- Testing Redis Connector (Delete) ---")
                if delete_student_pg(pg_conn, student_number):
                    time.sleep(5)
                    verify_redis_student_deleted(redis_client, redis_key)

    if pg_conn and neo4j_driver:
        institute_id = get_existing_institute_id(pg_conn)
        if institute_id:
            print("\n--- Testing Neo4j Connector (Department - Create) ---")
            department_name = fake.word() + " Department"
            department_id = create_department_pg(pg_conn, department_name, institute_id)
            if department_id:
                time.sleep(5)
                verify_neo4j_department(neo4j_driver, department_id, department_name)

                print("\n--- Testing Neo4j Connector (Department - Update) ---")
                new_department_name = fake.word() + " Updated Department"
                if update_department_pg(pg_conn, department_id, new_department_name):
                    time.sleep(5)
                    verify_neo4j_department(neo4j_driver, department_id, new_department_name)

                print("\n--- Testing Neo4j Connector (Department - Delete) ---")
                if delete_department_pg(pg_conn, department_id):
                    time.sleep(5)
                    verify_neo4j_department_deleted(neo4j_driver, department_id)

    if pg_conn and mongo_client:
        print("\n--- Testing MongoDB Connector (University - Create) ---")
        university_name = fake.university()
        university_id = create_university_pg(pg_conn, university_name)
        if university_id:
            time.sleep(5)
            verify_mongo_university(mongo_client, university_id, university_name)

            print("\n--- Testing MongoDB Connector (University - Update) ---")
            new_university_name = fake.university() + " Updated"
            if update_university_pg(pg_conn, university_id, new_university_name):
                time.sleep(5)
                verify_mongo_university(mongo_client, university_id, new_university_name)

            print("\n--- Testing MongoDB Connector (Institute - Create) ---")
            institute_name = fake.word() + " Institute"
            institute_id = create_institute_pg(pg_conn, institute_name, university_id)
            if institute_id:
                time.sleep(5)
                verify_mongo_institute(mongo_client, university_id, institute_id, institute_name)

                print("\n--- Testing MongoDB Connector (Institute - Update) ---")
                new_institute_name = fake.word() + " Updated Institute"
                if update_institute_pg(pg_conn, institute_id, new_institute_name):
                    time.sleep(5)
                    verify_mongo_institute(mongo_client, university_id, institute_id, new_institute_name)

                print("\n--- Testing MongoDB Connector (Department - Create) ---")
                department_name = fake.word() + " Department"
                department_id = create_department_pg(pg_conn, department_name, institute_id)
                if department_id:
                    time.sleep(5)
                    verify_mongo_department(mongo_client, university_id, institute_id, department_id, department_name)

                    print("\n--- Testing MongoDB Connector (Department - Update) ---")
                    new_department_name = fake.word() + " Updated Department"
                    if update_department_pg(pg_conn, department_id, new_department_name):
                        time.sleep(5)
                        verify_mongo_department(mongo_client, university_id, institute_id, department_id, new_department_name)

                    print("\n--- Testing MongoDB Connector (Department - Delete) ---")
                    if delete_department_pg(pg_conn, department_id):
                        time.sleep(5)
                        verify_mongo_department_deleted(mongo_client, university_id, institute_id, department_id)

                print("\n--- Testing MongoDB Connector (Institute - Delete) ---")
                if delete_institute_pg(pg_conn, institute_id):
                    time.sleep(5)
                    verify_mongo_institute_deleted(mongo_client, university_id, institute_id)

            print("\n--- Testing MongoDB Connector (University - Delete) ---")
            if delete_university_pg(pg_conn, university_id):
                time.sleep(5)
                verify_mongo_university_deleted(mongo_client, university_id)

    if pg_conn: pg_conn.close()
    if neo4j_driver: neo4j_driver.close()
    if mongo_client: mongo_client.close()

def create_course_pg(pg_conn, name, id_department):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO course (name, id_department) VALUES (%s, %s) RETURNING id",
                (name, id_department)
            )
            course_id = cur.fetchone()[0]
            pg_conn.commit()
        print(f"Inserted course {name} ({course_id}) into PostgreSQL.")
        return course_id
    except Exception as e:
        print(f"Error inserting course into PostgreSQL: {e}")
        pg_conn.rollback()
        return None

def update_course_pg(pg_conn, course_id, new_name):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "UPDATE course SET name = %s WHERE id = %s",
                (new_name, course_id)
            )
            pg_conn.commit()
        print(f"Updated course {course_id} in PostgreSQL.")
        return True
    except Exception as e:
        print(f"Error updating course in PostgreSQL: {e}")
        pg_conn.rollback()
        return False

def delete_course_pg(pg_conn, course_id):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM course WHERE id = %s",
                (course_id,)
            )
            pg_conn.commit()
        print(f"Deleted course {course_id} from PostgreSQL.")
        return True
    except Exception as e:
        print(f"Error deleting course from PostgreSQL: {e}")
        pg_conn.rollback()
        return False

def verify_es_document(es_client, index_name, doc_id, expected_data):
    try:
        doc = es_client.get(index=index_name, id=doc_id)
        if doc["_source"]:
            # Simple check for now, can be extended for deep comparison
            match = True
            for key, value in expected_data.items():
                if doc["_source"].get(key) != value:
                    match = False
                    break
            if match:
                print(f"Elasticsearch verification successful for {index_name} document {doc_id}.")
                return True
            else:
                print(f"Elasticsearch verification failed for {index_name} document {doc_id}. Expected: {expected_data}, Got: {doc['_source']}")
                return False
        else:
            print(f"Elasticsearch verification failed for {index_name} document {doc_id}. Document not found.")
            return False
    except Exception as e:
        print(f"Error verifying Elasticsearch document: {e}")
        return False

def verify_es_document_deleted(es_client, index_name, doc_id):
    try:
        if not es_client.exists(index=index_name, id=doc_id):
            print(f"Elasticsearch verification successful: {index_name} document {doc_id} deleted.")
            return True
        else:
            print(f"Elasticsearch verification failed: {index_name} document {doc_id} still exists.")
            return False
    except Exception as e:
        print(f"Error verifying Elasticsearch document deletion: {e}")
        return False

if __name__ == "__main__":
    pg_conn = connect_pg()
    redis_client = connect_redis()
    neo4j_driver = connect_neo4j()
    mongo_client = connect_mongo()
    es_client = connect_es()

    if pg_conn and redis_client:
        group_id, group_name = get_existing_group_id(pg_conn)
        if group_id:
            print("\n--- Testing Redis Connector (Create) ---")
            student_data = create_student_pg(pg_conn, group_id)
            if student_data:
                student_number, fullname, email, id_group, redis_key = student_data
                time.sleep(5)  # Give Kafka Connect time to process
                verify_redis_student(redis_client, redis_key, fullname, email, id_group, group_name)

                print("\n--- Testing Redis Connector (Update) ---")
                new_fullname = fake.name()
                new_email = fake.email()
                if update_student_pg(pg_conn, student_number, new_fullname, new_email):
                    time.sleep(5)
                    verify_redis_student(redis_client, redis_key, new_fullname, new_email, id_group, group_name)

                print("\n--- Testing Redis Connector (Delete) ---")
                if delete_student_pg(pg_conn, student_number):
                    time.sleep(5)
                    verify_redis_student_deleted(redis_client, redis_key)

    if pg_conn and neo4j_driver:
        institute_id = get_existing_institute_id(pg_conn)
        if institute_id:
            print("\n--- Testing Neo4j Connector (Department - Create) ---")
            department_name = fake.word() + " Department"
            department_id = create_department_pg(pg_conn, department_name, institute_id)
            if department_id:
                time.sleep(5)
                verify_neo4j_department(neo4j_driver, department_id, department_name)

                print("\n--- Testing Neo4j Connector (Department - Update) ---")
                new_department_name = fake.word() + " Updated Department"
                if update_department_pg(pg_conn, department_id, new_department_name):
                    time.sleep(5)
                    verify_neo4j_department(neo4j_driver, department_id, new_department_name)

                print("\n--- Testing Neo4j Connector (Department - Delete) ---")
                if delete_department_pg(pg_conn, department_id):
                    time.sleep(5)
                    verify_neo4j_department_deleted(neo4j_driver, department_id)

    if pg_conn and mongo_client:
        print("\n--- Testing MongoDB Connector (University - Create) ---")
        university_name = fake.university()
        university_id = create_university_pg(pg_conn, university_name)
        if university_id:
            time.sleep(5)
            verify_mongo_university(mongo_client, university_id, university_name)

            print("\n--- Testing MongoDB Connector (University - Update) ---")
            new_university_name = fake.university() + " Updated"
            if update_university_pg(pg_conn, university_id, new_university_name):
                time.sleep(5)
                verify_mongo_university(mongo_client, university_id, new_university_name)

            print("\n--- Testing MongoDB Connector (Institute - Create) ---")
            institute_name = fake.word() + " Institute"
            institute_id = create_institute_pg(pg_conn, institute_name, university_id)
            if institute_id:
                time.sleep(5)
                verify_mongo_institute(mongo_client, university_id, institute_id, institute_name)

                print("\n--- Testing MongoDB Connector (Institute - Update) ---")
                new_institute_name = fake.word() + " Updated Institute"
                if update_institute_pg(pg_conn, institute_id, new_institute_name):
                    time.sleep(5)
                    verify_mongo_institute(mongo_client, university_id, institute_id, new_institute_name)

                print("\n--- Testing MongoDB Connector (Department - Create) ---")
                department_name = fake.word() + " Department"
                department_id = create_department_pg(pg_conn, department_name, institute_id)
                if department_id:
                    time.sleep(5)
                    verify_mongo_department(mongo_client, university_id, institute_id, department_id, department_name)

                    print("\n--- Testing MongoDB Connector (Department - Update) ---")
                    new_department_name = fake.word() + " Updated Department"
                    if update_department_pg(pg_conn, department_id, new_department_name):
                        time.sleep(5)
                        verify_mongo_department(mongo_client, university_id, institute_id, department_id, new_department_name)

                    print("\n--- Testing MongoDB Connector (Department - Delete) ---")
                    if delete_department_pg(pg_conn, department_id):
                        time.sleep(5)
                        verify_mongo_department_deleted(mongo_client, university_id, institute_id, department_id)

                print("\n--- Testing MongoDB Connector (Institute - Delete) ---")
                if delete_institute_pg(pg_conn, institute_id):
                    time.sleep(5)
                    verify_mongo_institute_deleted(mongo_client, university_id, institute_id)

            print("\n--- Testing MongoDB Connector (University - Delete) ---")
            if delete_university_pg(pg_conn, university_id):
                time.sleep(5)
                verify_mongo_university_deleted(mongo_client, university_id)

    if pg_conn and es_client:
        department_id = get_existing_institute_id(pg_conn) # Reusing this to get a department_id
        if department_id:
            print("\n--- Testing Elasticsearch Connector (Course - Create) ---")
            course_name = fake.word() + " Course"
            course_id = create_course_pg(pg_conn, course_name, department_id)
            if course_id:
                time.sleep(5)
                verify_es_document(es_client, "postgres.public.course", course_id, {"name": course_name, "id_department": department_id})

                print("\n--- Testing Elasticsearch Connector (Course - Update) ---")
                new_course_name = fake.word() + " Updated Course"
                if update_course_pg(pg_conn, course_id, new_course_name):
                    time.sleep(5)
                    verify_es_document(es_client, "postgres.public.course", course_id, {"name": new_course_name, "id_department": department_id})

                print("\n--- Testing Elasticsearch Connector (Course - Delete) ---")
                if delete_course_pg(pg_conn, course_id):
                    time.sleep(5)
                    verify_es_document_deleted(es_client, "postgres.public.course", course_id)

    if pg_conn: pg_conn.close()
    if neo4j_driver: neo4j_driver.close()
    if mongo_client: mongo_client.close()

def verify_es_student(es_client, student_number, expected_fullname, expected_email, expected_id_group):
    try:
        doc = es_client.get(index="postgres.public.student", id=student_number)
        if doc["_source"]:
            if (
                doc["_source"].get("fullname") == expected_fullname and
                doc["_source"].get("email") == expected_email and
                doc["_source"].get("id_group") == expected_id_group
            ):
                print(f"Elasticsearch (student) verification successful for {student_number}.")
                return True
            else:
                print(f"Elasticsearch (student) verification failed for {student_number}. Expected: {expected_fullname}, {expected_email}, {expected_id_group}, Got: {doc['_source']}")
                return False
        else:
            print(f"Elasticsearch (student) verification failed for {student_number}. Document not found.")
            return False
    except Exception as e:
        print(f"Error verifying Elasticsearch (student) document: {e}")
        return False

def verify_es_student_deleted(es_client, student_number):
    try:
        if not es_client.exists(index="postgres.public.student", id=student_number):
            print(f"Elasticsearch (student) verification successful: {student_number} deleted.")
            return True
        else:
            print(f"Elasticsearch (student) verification failed: {student_number} still exists.")
            return False
    except Exception as e:
        print(f"Error verifying Elasticsearch (student) deletion: {e}")
        return False

if __name__ == "__main__":
    pg_conn = connect_pg()
    redis_client = connect_redis()
    neo4j_driver = connect_neo4j()
    mongo_client = connect_mongo()
    es_client = connect_es()

    if pg_conn and redis_client:
        group_id, group_name = get_existing_group_id(pg_conn)
        if group_id:
            print("\n--- Testing Redis Connector (Create) ---")
            student_data = create_student_pg(pg_conn, group_id)
            if student_data:
                student_number, fullname, email, id_group, redis_key = student_data
                time.sleep(5)  # Give Kafka Connect time to process
                verify_redis_student(redis_client, redis_key, fullname, email, id_group, group_name)

                print("\n--- Testing Redis Connector (Update) ---")
                new_fullname = fake.name()
                new_email = fake.email()
                if update_student_pg(pg_conn, student_number, new_fullname, new_email):
                    time.sleep(5)
                    verify_redis_student(redis_client, redis_key, new_fullname, new_email, id_group, group_name)

                print("\n--- Testing Redis Connector (Delete) ---")
                if delete_student_pg(pg_conn, student_number):
                    time.sleep(5)
                    verify_redis_student_deleted(redis_client, redis_key)

    if pg_conn and neo4j_driver:
        institute_id = get_existing_institute_id(pg_conn)
        if institute_id:
            print("\n--- Testing Neo4j Connector (Department - Create) ---")
            department_name = fake.word() + " Department"
            department_id = create_department_pg(pg_conn, department_name, institute_id)
            if department_id:
                time.sleep(5)
                verify_neo4j_department(neo4j_driver, department_id, department_name)

                print("\n--- Testing Neo4j Connector (Department - Update) ---")
                new_department_name = fake.word() + " Updated Department"
                if update_department_pg(pg_conn, department_id, new_department_name):
                    time.sleep(5)
                    verify_neo4j_department(neo4j_driver, department_id, new_department_name)

                print("\n--- Testing Neo4j Connector (Department - Delete) ---")
                if delete_department_pg(pg_conn, department_id):
                    time.sleep(5)
                    verify_neo4j_department_deleted(neo4j_driver, department_id)

    if pg_conn and mongo_client:
        print("\n--- Testing MongoDB Connector (University - Create) ---")
        university_name = fake.university()
        university_id = create_university_pg(pg_conn, university_name)
        if university_id:
            time.sleep(5)
            verify_mongo_university(mongo_client, university_id, university_name)

            print("\n--- Testing MongoDB Connector (University - Update) ---")
            new_university_name = fake.university() + " Updated"
            if update_university_pg(pg_conn, university_id, new_university_name):
                time.sleep(5)
                verify_mongo_university(mongo_client, university_id, new_university_name)

            print("\n--- Testing MongoDB Connector (Institute - Create) ---")
            institute_name = fake.word() + " Institute"
            institute_id = create_institute_pg(pg_conn, institute_name, university_id)
            if institute_id:
                time.sleep(5)
                verify_mongo_institute(mongo_client, university_id, institute_id, institute_name)

                print("\n--- Testing MongoDB Connector (Institute - Update) ---")
                new_institute_name = fake.word() + " Updated Institute"
                if update_institute_pg(pg_conn, institute_id, new_institute_name):
                    time.sleep(5)
                    verify_mongo_institute(mongo_client, university_id, institute_id, new_institute_name)

                print("\n--- Testing MongoDB Connector (Department - Create) ---")
                department_name = fake.word() + " Department"
                department_id = create_department_pg(pg_conn, department_name, institute_id)
                if department_id:
                    time.sleep(5)
                    verify_mongo_department(mongo_client, university_id, institute_id, department_id, department_name)

                    print("\n--- Testing MongoDB Connector (Department - Update) ---")
                    new_department_name = fake.word() + " Updated Department"
                    if update_department_pg(pg_conn, department_id, new_department_name):
                        time.sleep(5)
                        verify_mongo_department(mongo_client, university_id, institute_id, department_id, new_department_name)

                    print("\n--- Testing MongoDB Connector (Department - Delete) ---")
                    if delete_department_pg(pg_conn, department_id):
                        time.sleep(5)
                        verify_mongo_department_deleted(mongo_client, university_id, institute_id, department_id)

                print("\n--- Testing MongoDB Connector (Institute - Delete) ---")
                if delete_institute_pg(pg_conn, institute_id):
                    time.sleep(5)
                    verify_mongo_institute_deleted(mongo_client, university_id, institute_id)

            print("\n--- Testing MongoDB Connector (University - Delete) ---")
            if delete_university_pg(pg_conn, university_id):
                time.sleep(5)
                verify_mongo_university_deleted(mongo_client, university_id)

    if pg_conn and es_client:
        department_id = get_existing_institute_id(pg_conn) # Reusing this to get a department_id
        if department_id:
            print("\n--- Testing Elasticsearch Connector (Course - Create) ---")
            course_name = fake.word() + " Course"
            course_id = create_course_pg(pg_conn, course_name, department_id)
            if course_id:
                time.sleep(5)
                verify_es_document(es_client, "postgres.public.course", course_id, {"name": course_name, "id_department": department_id})

                print("\n--- Testing Elasticsearch Connector (Course - Update) ---")
                new_course_name = fake.word() + " Updated Course"
                if update_course_pg(pg_conn, course_id, new_course_name):
                    time.sleep(5)
                    verify_es_document(es_client, "postgres.public.course", course_id, {"name": new_course_name, "id_department": department_id})

                print("\n--- Testing Elasticsearch Connector (Course - Delete) ---")
                if delete_course_pg(pg_conn, course_id):
                    time.sleep(5)
                    verify_es_document_deleted(es_client, "postgres.public.course", course_id)

            print("\n--- Testing Elasticsearch Connector (Student - Create) ---")
            group_id, _ = get_existing_group_id(pg_conn)
            if group_id:
                student_number, fullname, email, id_group, _ = create_student_pg(pg_conn, group_id)
                if student_number:
                    time.sleep(5)
                    verify_es_student(es_client, student_number, fullname, email, id_group)

                    print("\n--- Testing Elasticsearch Connector (Student - Update) ---")
                    new_fullname = fake.name()
                    new_email = fake.email()
                    if update_student_pg(pg_conn, student_number, new_fullname, new_email):
                        time.sleep(5)
                        verify_es_student(es_client, student_number, new_fullname, new_email, id_group)

                    print("\n--- Testing Elasticsearch Connector (Student - Delete) ---")
                    if delete_student_pg(pg_conn, student_number):
                        time.sleep(5)
                        verify_es_student_deleted(es_client, student_number)

    if pg_conn: pg_conn.close()
    if neo4j_driver: neo4j_driver.close()
    if mongo_client: mongo_client.close()

def create_lecture_pg(pg_conn, name, id_course):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO lecture (name, id_course) VALUES (%s, %s) RETURNING id",
                (name, id_course)
            )
            lecture_id = cur.fetchone()[0]
            pg_conn.commit()
        print(f"Inserted lecture {name} ({lecture_id}) into PostgreSQL.")
        return lecture_id
    except Exception as e:
        print(f"Error inserting lecture into PostgreSQL: {e}")
        pg_conn.rollback()
        return None

def update_lecture_pg(pg_conn, lecture_id, new_name):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "UPDATE lecture SET name = %s WHERE id = %s",
                (new_name, lecture_id)
            )
            pg_conn.commit()
        print(f"Updated lecture {lecture_id} in PostgreSQL.")
        return True
    except Exception as e:
        print(f"Error updating lecture in PostgreSQL: {e}")
        pg_conn.rollback()
        return False

def delete_lecture_pg(pg_conn, lecture_id):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM lecture WHERE id = %s",
                (lecture_id,)
            )
            pg_conn.commit()
        print(f"Deleted lecture {lecture_id} from PostgreSQL.")
        return True
    except Exception as e:
        print(f"Error deleting lecture from PostgreSQL: {e}")
        pg_conn.rollback()
        return False

def verify_neo4j_lecture_department(neo4j_driver, lecture_id, expected_lecture_name, expected_id_course, expected_id_department, expected_department_name):
    try:
        with neo4j_driver.session() as session:
            result = session.run(
                "MATCH (l:Lecture {id: $lecture_id}) RETURN l.name AS lecture_name, l.id_course AS id_course, l.id_department AS id_department, l.department_name AS department_name",
                lecture_id=lecture_id
            )
            record = result.single()
            if record:
                if (
                    record["lecture_name"] == expected_lecture_name and
                    record["id_course"] == expected_id_course and
                    record["id_department"] == expected_id_department and
                    record["department_name"] == expected_department_name
                ):
                    print(f"Neo4j verification successful for Lecture Department {lecture_id}.")
                    return True
                else:
                    print(f"Neo4j verification failed for Lecture Department {lecture_id}. Expected: {expected_lecture_name}, {expected_id_course}, {expected_id_department}, {expected_department_name}, Got: {record}")
                    return False
            else:
                print(f"Neo4j verification failed for Lecture Department {lecture_id}. Node not found.")
                return False
    except Exception as e:
        print(f"Error verifying Neo4j Lecture Department: {e}")
        return False

def verify_neo4j_lecture_department_deleted(neo4j_driver, lecture_id):
    try:
        with neo4j_driver.session() as session:
            result = session.run(
                "MATCH (l:Lecture {id: $lecture_id}) RETURN l",
                lecture_id=lecture_id
            )
            if not result.single():
                print(f"Neo4j verification successful: Lecture Department {lecture_id} deleted.")
                return True
            else:
                print(f"Neo4j verification failed: Lecture Department {lecture_id} still exists.")
                return False
    except Exception as e:
        print(f"Error verifying Neo4j Lecture Department deletion: {e}")
        return False

def create_group_pg(pg_conn, name, id_department):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO groups (name, id_department) VALUES (%s, %s) RETURNING id",
                (name, id_department)
            )
            group_id = cur.fetchone()[0]
            pg_conn.commit()
        print(f"Inserted group {name} ({group_id}) into PostgreSQL.")
        return group_id
    except Exception as e:
        print(f"Error inserting group into PostgreSQL: {e}")
        pg_conn.rollback()
        return None

def update_group_pg(pg_conn, group_id, new_name):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "UPDATE groups SET name = %s WHERE id = %s",
                (new_name, group_id)
            )
            pg_conn.commit()
        print(f"Updated group {group_id} in PostgreSQL.")
        return True
    except Exception as e:
        print(f"Error updating group in PostgreSQL: {e}")
        pg_conn.rollback()
        return False

def delete_group_pg(pg_conn, group_id):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM groups WHERE id = %s",
                (group_id,)
            )
            pg_conn.commit()
        print(f"Deleted group {group_id} from PostgreSQL.")
        return True
    except Exception as e:
        print(f"Error deleting group from PostgreSQL: {e}")
        pg_conn.rollback()
        return False

def verify_neo4j_group(neo4j_driver, group_id, expected_name, expected_department_id):
    try:
        with neo4j_driver.session() as session:
            result = session.run(
                "MATCH (g:Group {id: $group_id}) RETURN g.name AS name, g.department_id AS department_id",
                group_id=group_id
            )
            record = result.single()
            if record:
                if record["name"] == expected_name and record["department_id"] == expected_department_id:
                    print(f"Neo4j verification successful for Group {group_id}.")
                    return True
                else:
                    print(f"Neo4j verification failed for Group {group_id}. Expected: {expected_name}, {expected_department_id}, Got: {record}")
                    return False
            else:
                print(f"Neo4j verification failed for Group {group_id}. Node not found.")
                return False
    except Exception as e:
        print(f"Error verifying Neo4j Group: {e}")
        return False

def verify_neo4j_group_deleted(neo4j_driver, group_id):
    try:
        with neo4j_driver.session() as session:
            result = session.run(
                "MATCH (g:Group {id: $group_id}) RETURN g",
                group_id=group_id
            )
            if not result.single():
                print(f"Neo4j verification successful: Group {group_id} deleted.")
                return True
            else:
                print(f"Neo4j verification failed: Group {group_id} still exists.")
                return False
    except Exception as e:
        print(f"Error verifying Neo4j Group deletion: {e}")
        return False

def create_schedule_pg(pg_conn, id_lecture, id_group, location):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO schedule (id_lecture, id_group, timestamp, location) VALUES (%s, %s, NOW(), %s) RETURNING id",
                (id_lecture, id_group, location)
            )
            schedule_id = cur.fetchone()[0]
            pg_conn.commit()
        print(f"Inserted schedule {schedule_id} into PostgreSQL.")
        return schedule_id
    except Exception as e:
        print(f"Error inserting schedule into PostgreSQL: {e}")
        pg_conn.rollback()
        return None

def update_schedule_pg(pg_conn, schedule_id, new_location):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "UPDATE schedule SET location = %s WHERE id = %s",
                (new_location, schedule_id)
            )
            pg_conn.commit()
        print(f"Updated schedule {schedule_id} in PostgreSQL.")
        return True
    except Exception as e:
        print(f"Error updating schedule in PostgreSQL: {e}")
        pg_conn.rollback()
        return False

def delete_schedule_pg(pg_conn, schedule_id):
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM schedule WHERE id = %s",
                (schedule_id,)
            )
            pg_conn.commit()
        print(f"Deleted schedule {schedule_id} from PostgreSQL.")
        return True
    except Exception as e:
        print(f"Error deleting schedule from PostgreSQL: {e}")
        pg_conn.rollback()
        return False

def verify_neo4j_schedule(neo4j_driver, schedule_id, expected_location):
    try:
        with neo4j_driver.session() as session:
            result = session.run(
                "MATCH ()-[r:HAS_SCHEDULE {id: $schedule_id}]->() RETURN r.location AS location",
                schedule_id=schedule_id
            )
            record = result.single()
            if record:
                if record["location"] == expected_location:
                    print(f"Neo4j verification successful for Schedule {schedule_id}.")
                    return True
                else:
                    print(f"Neo4j verification failed for Schedule {schedule_id}. Expected: {expected_location}, Got: {record['location']}")
                    return False
            else:
                print(f"Neo4j verification failed for Schedule {schedule_id}. Relationship not found.")
                return False
    except Exception as e:
        print(f"Error verifying Neo4j Schedule: {e}")
        return False

def verify_neo4j_schedule_deleted(neo4j_driver, schedule_id):
    try:
        with neo4j_driver.session() as session:
            result = session.run(
                "MATCH ()-[r:HAS_SCHEDULE {id: $schedule_id}]->() RETURN r",
                schedule_id=schedule_id
            )
            if not result.single():
                print(f"Neo4j verification successful: Schedule {schedule_id} deleted.")
                return True
            else:
                print(f"Neo4j verification failed: Schedule {schedule_id} still exists.")
                return False
    except Exception as e:
        print(f"Error verifying Neo4j Schedule deletion: {e}")
        return False

def get_existing_course_id(pg_conn):
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT id, id_department FROM course LIMIT 1")
            result = cur.fetchone()
            if result:
                return result[0], result[1]
            else:
                print("No courses found in PostgreSQL. Please populate data first.")
                return None, None
    except Exception as e:
        print(f"Error getting course ID from PostgreSQL: {e}")
        return None, None

if __name__ == "__main__":
    pg_conn = connect_pg()
    redis_client = connect_redis()
    neo4j_driver = connect_neo4j()
    mongo_client = connect_mongo()
    es_client = connect_es()

    if pg_conn and redis_client:
        group_id, group_name = get_existing_group_id(pg_conn)
        if group_id:
            print("\n--- Testing Redis Connector (Create) ---")
            student_data = create_student_pg(pg_conn, group_id)
            if student_data:
                student_number, fullname, email, id_group, redis_key = student_data
                time.sleep(5)  # Give Kafka Connect time to process
                verify_redis_student(redis_client, redis_key, fullname, email, id_group, group_name)

                print("\n--- Testing Redis Connector (Update) ---")
                new_fullname = fake.name()
                new_email = fake.email()
                if update_student_pg(pg_conn, student_number, new_fullname, new_email):
                    time.sleep(5)
                    verify_redis_student(redis_client, redis_key, new_fullname, new_email, id_group, group_name)

                print("\n--- Testing Redis Connector (Delete) ---")
                if delete_student_pg(pg_conn, student_number):
                    time.sleep(5)
                    verify_redis_student_deleted(redis_client, redis_key)

    if pg_conn and neo4j_driver:
        institute_id = get_existing_institute_id(pg_conn)
        if institute_id:
            print("\n--- Testing Neo4j Connector (Department - Create) ---")
            department_name = fake.word() + " Department"
            department_id = create_department_pg(pg_conn, department_name, institute_id)
            if department_id:
                time.sleep(5)
                verify_neo4j_department(neo4j_driver, department_id, department_name)

                print("\n--- Testing Neo4j Connector (Department - Update) ---")
                new_department_name = fake.word() + " Updated Department"
                if update_department_pg(pg_conn, department_id, new_department_name):
                    time.sleep(5)
                    verify_neo4j_department(neo4j_driver, department_id, new_department_name)

                print("\n--- Testing Neo4j Connector (Department - Delete) ---")
                if delete_department_pg(pg_conn, department_id):
                    time.sleep(5)
                    verify_neo4j_department_deleted(neo4j_driver, department_id)

            print("\n--- Testing Neo4j Connector (Group - Create) ---")
            group_name = fake.word() + " Group"
            group_id = create_group_pg(pg_conn, group_name, department_id)
            if group_id:
                time.sleep(5)
                verify_neo4j_group(neo4j_driver, group_id, group_name, department_id)

                print("\n--- Testing Neo4j Connector (Group - Update) ---")
                new_group_name = fake.word() + " Updated Group"
                if update_group_pg(pg_conn, group_id, new_group_name):
                    time.sleep(5)
                    verify_neo4j_group(neo4j_driver, group_id, new_group_name, department_id)

                print("\n--- Testing Neo4j Connector (Group - Delete) ---")
                if delete_group_pg(pg_conn, group_id):
                    time.sleep(5)
                    verify_neo4j_group_deleted(neo4j_driver, group_id)

            print("\n--- Testing Neo4j Connector (Lecture Department - Create/Update/Delete) ---")
            course_id, department_id_for_course = get_existing_course_id(pg_conn)
            if course_id:
                lecture_name = fake.word() + " Lecture"
                lecture_id = create_lecture_pg(pg_conn, lecture_name, course_id)
                if lecture_id:
                    time.sleep(5)
                    # Need to get department name for verification
                    with pg_conn.cursor() as cur:
                        cur.execute("SELECT name FROM department WHERE id = %s", (department_id_for_course,))
                        department_name_for_course = cur.fetchone()[0]
                    verify_neo4j_lecture_department(neo4j_driver, lecture_id, lecture_name, course_id, department_id_for_course, department_name_for_course)

                    print("\n--- Testing Neo4j Connector (Lecture Department - Update Lecture) ---")
                    new_lecture_name = fake.word() + " Updated Lecture"
                    if update_lecture_pg(pg_conn, lecture_id, new_lecture_name):
                        time.sleep(5)
                        verify_neo4j_lecture_department(neo4j_driver, lecture_id, new_lecture_name, course_id, department_id_for_course, department_name_for_course)

                    print("\n--- Testing Neo4j Connector (Lecture Department - Delete Lecture) ---")
                    if delete_lecture_pg(pg_conn, lecture_id):
                        time.sleep(5)
                        verify_neo4j_lecture_department_deleted(neo4j_driver, lecture_id)

            print("\n--- Testing Neo4j Connector (Schedule - Create) ---")
            lecture_id_for_schedule, _ = get_existing_course_id(pg_conn) # Reusing to get a lecture_id
            group_id_for_schedule, _ = get_existing_group_id(pg_conn)
            if lecture_id_for_schedule and group_id_for_schedule:
                location = fake.address()
                schedule_id = create_schedule_pg(pg_conn, lecture_id_for_schedule, group_id_for_schedule, location)
                if schedule_id:
                    time.sleep(5)
                    verify_neo4j_schedule(neo4j_driver, schedule_id, location)

                    print("\n--- Testing Neo4j Connector (Schedule - Update) ---")
                    new_location = fake.address()
                    if update_schedule_pg(pg_conn, schedule_id, new_location):
                        time.sleep(5)
                        verify_neo4j_schedule(neo4j_driver, schedule_id, new_location)

                    print("\n--- Testing Neo4j Connector (Schedule - Delete) ---")
                    if delete_schedule_pg(pg_conn, schedule_id):
                        time.sleep(5)
                        verify_neo4j_schedule_deleted(neo4j_driver, schedule_id)

    if pg_conn and mongo_client:
        print("\n--- Testing MongoDB Connector (University - Create) ---")
        university_name = fake.university()
        university_id = create_university_pg(pg_conn, university_name)
        if university_id:
            time.sleep(5)
            verify_mongo_university(mongo_client, university_id, university_name)

            print("\n--- Testing MongoDB Connector (University - Update) ---")
            new_university_name = fake.university() + " Updated"
            if update_university_pg(pg_conn, university_id, new_university_name):
                time.sleep(5)
                verify_mongo_university(mongo_client, university_id, new_university_name)

            print("\n--- Testing MongoDB Connector (Institute - Create) ---")
            institute_name = fake.word() + " Institute"
            institute_id = create_institute_pg(pg_conn, institute_name, university_id)
            if institute_id:
                time.sleep(5)
                verify_mongo_institute(mongo_client, university_id, institute_id, institute_name)

                print("\n--- Testing MongoDB Connector (Institute - Update) ---")
                new_institute_name = fake.word() + " Updated Institute"
                if update_institute_pg(pg_conn, institute_id, new_institute_name):
                    time.sleep(5)
                    verify_mongo_institute(mongo_client, university_id, institute_id, new_institute_name)

                print("\n--- Testing MongoDB Connector (Department - Create) ---")
                department_name = fake.word() + " Department"
                department_id = create_department_pg(pg_conn, department_name, institute_id)
                if department_id:
                    time.sleep(5)
                    verify_mongo_department(mongo_client, university_id, institute_id, department_id, department_name)

                    print("\n--- Testing MongoDB Connector (Department - Update) ---")
                    new_department_name = fake.word() + " Updated Department"
                    if update_department_pg(pg_conn, department_id, new_department_name):
                        time.sleep(5)
                        verify_mongo_department(mongo_client, university_id, institute_id, department_id, new_department_name)

                    print("\n--- Testing MongoDB Connector (Department - Delete) ---")
                    if delete_department_pg(pg_conn, department_id):
                        time.sleep(5)
                        verify_mongo_department_deleted(mongo_client, university_id, institute_id, department_id)

                print("\n--- Testing MongoDB Connector (Institute - Delete) ---")
                if delete_institute_pg(pg_conn, institute_id):
                    time.sleep(5)
                    verify_mongo_institute_deleted(mongo_client, university_id, institute_id)

            print("\n--- Testing MongoDB Connector (University - Delete) ---")
            if delete_university_pg(pg_conn, university_id):
                time.sleep(5)
                verify_mongo_university_deleted(mongo_client, university_id)

    if pg_conn and es_client:
        department_id = get_existing_institute_id(pg_conn) # Reusing this to get a department_id
        if department_id:
            print("\n--- Testing Elasticsearch Connector (Course - Create) ---")
            course_name = fake.word() + " Course"
            course_id = create_course_pg(pg_conn, course_name, department_id)
            if course_id:
                time.sleep(5)
                verify_es_document(es_client, "postgres.public.course", course_id, {"name": course_name, "id_department": department_id})

                print("\n--- Testing Elasticsearch Connector (Course - Update) ---")
                new_course_name = fake.word() + " Updated Course"
                if update_course_pg(pg_conn, course_id, new_course_name):
                    time.sleep(5)
                    verify_es_document(es_client, "postgres.public.course", course_id, {"name": new_course_name, "id_department": department_id})

                print("\n--- Testing Elasticsearch Connector (Course - Delete) ---")
                if delete_course_pg(pg_conn, course_id):
                    time.sleep(5)
                    verify_es_document_deleted(es_client, "postgres.public.course", course_id)

            print("\n--- Testing Elasticsearch Connector (Student - Create) ---")
            group_id, _ = get_existing_group_id(pg_conn)
            if group_id:
                student_number, fullname, email, id_group, _ = create_student_pg(pg_conn, group_id)
                if student_number:
                    time.sleep(5)
                    verify_es_student(es_client, student_number, fullname, email, id_group)

                    print("\n--- Testing Elasticsearch Connector (Student - Update) ---")
                    new_fullname = fake.name()
                    new_email = fake.email()
                    if update_student_pg(pg_conn, student_number, new_fullname, new_email):
                        time.sleep(5)
                        verify_es_student(es_client, student_number, new_fullname, new_email, id_group)

                    print("\n--- Testing Elasticsearch Connector (Student - Delete) ---")
                    if delete_student_pg(pg_conn, student_number):
                        time.sleep(5)
                        verify_es_student_deleted(es_client, student_number)

    if pg_conn: pg_conn.close()
    if neo4j_driver: neo4j_driver.close()
    if mongo_client: mongo_client.close()