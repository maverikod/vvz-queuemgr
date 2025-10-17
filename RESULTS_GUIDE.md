# 🎯 Руководство по работе с результатами заданий

## 📋 Обзор

Queue Manager позволяет заданиям сохранять и возвращать результаты выполнения. Результаты автоматически сохраняются в реестре и доступны через API.

## 🔧 Как использовать результаты

### 1. В классе задания

```python
from queuemgr.jobs.base import QueueJobBase

class MyJob(QueueJobBase):
    def execute(self) -> None:
        # Выполняем работу
        data = self.process_data()
        
        # Сохраняем результат
        self.set_result({
            "processed_items": len(data),
            "total_time": time.time() - self.start_time,
            "data": data
        })
        
    def process_data(self):
        # Ваша логика обработки
        return ["item1", "item2", "item3"]
```

### 2. Получение результата

```python
from queuemgr.proc_api import proc_queue_system

with proc_queue_system() as queue:
    # Добавляем и запускаем задание
    queue.add_job(MyJob, "my-job-1", {"param": "value"})
    queue.start_job("my-job-1")
    
    # Ждем завершения
    time.sleep(5)
    
    # Получаем статус с результатом
    status = queue.get_job_status("my-job-1")
    result = status.get('result')
    
    if result:
        print(f"Обработано элементов: {result['processed_items']}")
        print(f"Время выполнения: {result['total_time']}")
```

### 3. Через CLI

```bash
# Показать статус задания с результатом
queuemgr-cli job status my-job-1

# Список всех заданий
queuemgr-cli job list
```

## 📊 Типы результатов

### Простые значения
```python
# Число
self.set_result(42)

# Строка
self.set_result("Processing completed")

# Булево значение
self.set_result(True)
```

### Сложные структуры
```python
# Словарь с данными
self.set_result({
    "status": "completed",
    "processed_files": 150,
    "total_size": 1024 * 1024 * 50,
    "errors": [],
    "warnings": ["File too large: large_file.txt"]
})

# Список результатов
self.set_result([
    {"file": "file1.txt", "size": 1024},
    {"file": "file2.txt", "size": 2048}
])
```

## 🎯 Примеры использования

### Обработка данных
```python
class DataProcessorJob(QueueJobBase):
    def execute(self) -> None:
        input_data = self.params.get("data", [])
        processed = []
        
        for item in input_data:
            processed.append(self.transform_item(item))
            
        self.set_result({
            "input_count": len(input_data),
            "output_count": len(processed),
            "processed_data": processed,
            "processing_time": time.time()
        })
```

### Анализ файлов
```python
class FileAnalyzerJob(QueueJobBase):
    def execute(self) -> None:
        file_path = self.params["file_path"]
        
        # Анализируем файл
        analysis = self.analyze_file(file_path)
        
        self.set_result({
            "file_path": file_path,
            "file_size": analysis["size"],
            "line_count": analysis["lines"],
            "word_count": analysis["words"],
            "recommendations": analysis["recommendations"]
        })
```

### Вычисления
```python
class CalculatorJob(QueueJobBase):
    def execute(self) -> None:
        operation = self.params["operation"]
        numbers = self.params["numbers"]
        
        if operation == "sum":
            result = sum(numbers)
        elif operation == "product":
            result = 1
            for n in numbers:
                result *= n
                
        self.set_result({
            "operation": operation,
            "numbers": numbers,
            "result": result,
            "timestamp": time.time()
        })
```

## 🔍 Получение результатов

### Через API
```python
# Получить статус задания
status = queue.get_job_status("job-id")
result = status.get('result')

# Проверить наличие результата
if result:
    print(f"Результат: {result}")
else:
    print("Результат не доступен")
```

### Через CLI
```bash
# Детальный статус с результатом
queuemgr-cli job status my-job-1

# Список заданий (показывает статус)
queuemgr-cli job list
```

### Через веб-интерфейс
```bash
# Запустить веб-интерфейс
queuemgr-web

# Открыть http://localhost:5000
# Перейти к заданию и посмотреть результат
```

## ⚠️ Важные моменты

### 1. Сериализация
Результаты должны быть JSON-сериализуемыми:
```python
# ✅ Хорошо
self.set_result({"data": [1, 2, 3], "status": "ok"})

# ❌ Плохо (не сериализуется)
self.set_result({"data": open("file.txt")})
```

### 2. Размер результатов
Не сохраняйте слишком большие результаты:
```python
# ✅ Хорошо - только метаданные
self.set_result({
    "processed_count": 1000,
    "file_size": 1024 * 1024,
    "sample_data": data[:10]  # Только первые 10 элементов
})

# ❌ Плохо - весь массив данных
self.set_result({"all_data": huge_array})
```

### 3. Время жизни
Результаты сохраняются в реестре до удаления задания:
```python
# Результат доступен пока задание существует
queue.delete_job("job-id")  # Результат удаляется
```

## 🚀 Полный пример

```python
#!/usr/bin/env python3
"""
Полный пример работы с результатами заданий.
"""

import time
from queuemgr.proc_api import proc_queue_system
from queuemgr.jobs.base import QueueJobBase

class ExampleJob(QueueJobBase):
    def __init__(self, job_id: str, params: dict):
        super().__init__(job_id, params)
        self.start_time = time.time()
        
    def execute(self) -> None:
        # Имитируем работу
        time.sleep(2)
        
        # Создаем результат
        result = {
            "job_id": self.job_id,
            "execution_time": time.time() - self.start_time,
            "processed_items": self.params.get("count", 10),
            "status": "completed",
            "timestamp": time.time()
        }
        
        # Сохраняем результат
        self.set_result(result)
        
    def on_start(self) -> None:
        print(f"Задание {self.job_id} запущено")
        
    def on_end(self) -> None:
        print(f"Задание {self.job_id} завершено")
        
    def on_error(self, exc: BaseException) -> None:
        print(f"Ошибка в задании {self.job_id}: {exc}")

def main():
    with proc_queue_system() as queue:
        # Добавляем задание
        queue.add_job(ExampleJob, "example-1", {"count": 5})
        
        # Запускаем
        queue.start_job("example-1")
        
        # Ждем завершения
        time.sleep(3)
        
        # Получаем результат
        status = queue.get_job_status("example-1")
        result = status.get('result')
        
        if result:
            print(f"✅ Задание завершено за {result['execution_time']:.2f} секунд")
            print(f"📊 Обработано элементов: {result['processed_items']}")
        else:
            print("❌ Результат не найден")

if __name__ == "__main__":
    main()
```

## 📚 Дополнительные ресурсы

- `queuemgr/examples/result_job.py` - Подробные примеры
- `queuemgr/examples/full_app_example.py` - Полное приложение
- CLI команды: `queuemgr-cli --help`
- Веб-интерфейс: `queuemgr-web`

---

**Готово!** Теперь вы знаете, как работать с результатами заданий в Queue Manager! 🎉
