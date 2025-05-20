# Hierarchical CDC Handler для MongoDB Kafka Connect

Этот кастомный CDC Handler позволяет объединять данные из нескольких таблиц PostgreSQL (через Debezium) в один иерархический документ в MongoDB.

## Назначение

Обработчик предназначен для создания иерархической структуры в MongoDB на основе связанных таблиц в PostgreSQL:
- `university` (университеты)
- `institute` (институты, связанные с университетами)
- `department` (кафедры, связанные с институтами)

## Структура данных

Обработчик создаст следующую вложенную структуру в MongoDB:

```json
{
  "_id": 1,
  "name": "Университет",
  "institutes": [
    {
      "id": 1,
      "name": "Институт",
      "departments": [
        {
          "departmentId": 1,
          "name": "Кафедра"
        }
      ]
    }
  ]
}
```

## Использование

Для использования этого обработчика, добавьте следующую конфигурацию в настройки вашего MongoDB Kafka Connector:

```json
{
  "name": "mongodb-hierarchical-sink",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "topics": "postgres.public.university,postgres.public.institute,postgres.public.department",
    "connection.uri": "mongodb://mongo:27017",
    "database": "university", 
    "collection": "universities",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.BsonOidStrategy",
    "change.data.capture.handler": "com.mongodb.kafka.connect.sink.cdc.hierarchical.HierarchicalHandlerFactory",
    "delete.on.null.values": "true",
    "errors.tolerance": "all",
    "errors.log.enable": "true"
  }
}
```

## Требования к структуре таблиц

Для корректной работы обработчика, таблицы в PostgreSQL должны иметь следующую структуру:

### university
```sql
CREATE TABLE university (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);
```

### institute
```sql
CREATE TABLE institute (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    id_university INTEGER REFERENCES university(id)
);
```

### department
```sql
CREATE TABLE department (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    id_institute INTEGER REFERENCES institute(id)
);
```

## Обработка событий

Обработчик поддерживает следующие события CDC:
- **Вставка**: добавление новых университетов, институтов и кафедр
- **Обновление**: изменение данных в любой из таблиц
- **Удаление**: удаление записей из любой из таблиц

## Кэширование

Для улучшения производительности, обработчик использует внутренний кэш для хранения информации об университетах и институтах. Это позволяет избежать лишних запросов к MongoDB. 