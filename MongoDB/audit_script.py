from pymongo import MongoClient, UpdateOne
from datetime import datetime, timedelta
import os 
import json

# Подключение к бд
client = MongoClient("mongodb://localhost:27017/")
db = client["my_database"]
collection = db["user_events"]
archive_collection = db["archived_users"]

# Фильтрация дат
today = datetime.now()
registration_old = today - timedelta(days=30)  # Зарегистрирован более 30 дней назад
not_activity = today - timedelta(days=14)      # Не проявлял активность больше 14 дней

pipeline = [
    {"$match": {"user_info.registration_date": {"$lte": registration_old}}},
    {"$group": {
        "_id": "$user_id",
        "last_event": {"$max": "$event_time"},
        "massive_old_users": {"$push": "$_id"}
    }},
    {"$match": {"last_event": {"$lte": not_activity}}}
]

results = list(collection.aggregate(pipeline))
users_to_archive = [r["_id"] for r in results]

if not users_to_archive:
    print("Нет пользователей для архивирования")
else:
    docs_to_archive = list(collection.find({"user_id": {"$in": users_to_archive}}))

    if docs_to_archive:
        # Подготовка операций upsert для bulk_write
        operations = []
        for doc in docs_to_archive:
            operations.append(
                UpdateOne(
                    {"_id": doc["_id"]},
                    {"$set": doc},
                    upsert=True
                )
            )

        # Вставляем или обновляем документы в архивной коллекции
        archive_collection.bulk_write(operations)

        # Удаляем архивные документы из основной коллекции
        ids_to_delete = [doc["_id"] for doc in docs_to_archive]
        collection.delete_many({"_id": {"$in": ids_to_delete}})

    # Сохранение отчета в JSON
    report = {
        "date": today.strftime("%Y-%m-%d"),
        "archived_users_count": len(users_to_archive)
    }
    report_filename = f"/tmp/archive_report_users_{today.strftime('%Y-%m-%d')}.json"

    with open(report_filename, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=4)

    print(f"✅ Архивировано пользователей: {len(users_to_archive)}")
    print(f"Отчет сохранен в файл: {report_filename}")

print("Текущая директория:", os.getcwd())
