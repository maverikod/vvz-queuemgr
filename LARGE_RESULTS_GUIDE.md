# 📊 Руководство по работе с большими результатами

## 🎯 Обзор

Queue Manager поддерживает сохранение больших результатов заданий (до нескольких мегабайт). Это полезно для:
- Обработки больших объемов данных
- Анализа и агрегации данных
- Генерации отчетов
- Кэширования результатов вычислений

## ⚠️ Важные ограничения

### 1. Размер реестра
- Большие результаты увеличивают размер файла реестра
- Каждый результат сохраняется как JSON в одной строке
- Рекомендуемый размер результата: до 10MB

### 2. Производительность
- Большие результаты замедляют загрузку реестра
- Увеличивается время сериализации/десериализации
- Больше потребление памяти

### 3. Сетевые ограничения
- При использовании веб-API большие результаты могут превышать лимиты HTTP
- Рекомендуется использовать CLI для больших результатов

## 🚀 Примеры использования

### Генерация больших данных

```python
class LargeDataGeneratorJob(QueueJobBase):
    def execute(self) -> None:
        # Генерируем большой массив данных
        data = []
        for i in range(100000):  # 100K записей
            data.append({
                "id": i,
                "value": random.uniform(0, 1000),
                "timestamp": time.time(),
                "category": random.choice(["A", "B", "C"])
            })
        
        # Сохраняем результат
        self.set_result({
            "data": data,
            "metadata": {
                "count": len(data),
                "generated_at": time.time(),
                "size_mb": len(json.dumps(data)) / (1024 * 1024)
            }
        })
```

### Анализ больших данных

```python
class DataAnalyzerJob(QueueJobBase):
    def execute(self) -> None:
        # Анализируем большой датасет
        analysis_result = {
            "statistics": {
                "total_records": 1000000,
                "mean": 500.5,
                "std_dev": 100.2,
                "percentiles": {
                    "25th": 400,
                    "50th": 500,
                    "75th": 600,
                    "95th": 800
                }
            },
            "patterns": [
                {"name": "trend", "confidence": 0.85},
                {"name": "seasonality", "confidence": 0.92}
            ],
            "recommendations": [
                "Implement data partitioning",
                "Add monitoring for anomalies"
            ],
            "sample_data": self._get_sample_data(1000)  # Только образец
        }
        
        self.set_result(analysis_result)
```

## 🔧 Оптимизация больших результатов

### 1. Сжатие данных

```python
import gzip
import base64

class CompressedDataJob(QueueJobBase):
    def execute(self) -> None:
        # Генерируем большие данные
        large_data = self._generate_large_data()
        
        # Сжимаем данные
        json_str = json.dumps(large_data)
        compressed = gzip.compress(json_str.encode())
        encoded = base64.b64encode(compressed).decode()
        
        # Сохраняем сжатую версию
        self.set_result({
            "compressed_data": encoded,
            "original_size": len(json_str),
            "compressed_size": len(compressed),
            "compression_ratio": len(compressed) / len(json_str),
            "metadata": {
                "compression": "gzip",
                "encoding": "base64"
            }
        })
    
    def _decompress_result(self, result):
        """Декомпрессия результата."""
        encoded = result["compressed_data"]
        compressed = base64.b64decode(encoded)
        json_str = gzip.decompress(compressed).decode()
        return json.loads(json_str)
```

### 2. Пагинация результатов

```python
class PaginatedDataJob(QueueJobBase):
    def execute(self) -> None:
        # Генерируем данные по частям
        all_data = []
        page_size = 10000
        
        for page in range(10):  # 10 страниц по 10K записей
            page_data = self._generate_page_data(page, page_size)
            all_data.extend(page_data)
        
        # Сохраняем с метаданными пагинации
        self.set_result({
            "data": all_data,
            "pagination": {
                "total_pages": 10,
                "page_size": page_size,
                "total_records": len(all_data)
            },
            "metadata": {
                "generated_at": time.time(),
                "size_mb": len(json.dumps(all_data)) / (1024 * 1024)
            }
        })
```

### 3. Выборочные данные

```python
class SampledDataJob(QueueJobBase):
    def execute(self) -> None:
        # Генерируем полный датасет
        full_data = self._generate_full_dataset()
        
        # Берем только образец для результата
        sample_size = 1000
        sample_data = random.sample(full_data, min(sample_size, len(full_data)))
        
        # Сохраняем образец + метаданные
        self.set_result({
            "sample_data": sample_data,
            "full_dataset_stats": {
                "total_records": len(full_data),
                "sample_size": len(sample_data),
                "sampling_ratio": len(sample_data) / len(full_data)
            },
            "metadata": {
                "sampling_method": "random",
                "confidence_level": 0.95
            }
        })
```

## 📊 Мониторинг больших результатов

### 1. Размер результата

```python
def check_result_size(result):
    """Проверка размера результата."""
    size_bytes = len(json.dumps(result))
    size_mb = size_bytes / (1024 * 1024)
    
    if size_mb > 10:
        print(f"⚠️ Большой результат: {size_mb:.2f} MB")
    elif size_mb > 1:
        print(f"📊 Средний результат: {size_mb:.2f} MB")
    else:
        print(f"✅ Небольшой результат: {size_mb:.2f} MB")
    
    return size_mb
```

### 2. Производительность

```python
import time

def measure_result_performance():
    """Измерение производительности работы с результатами."""
    start_time = time.time()
    
    # Получаем результат
    status = queue.get_job_status("large-job")
    result = status.get('result')
    
    load_time = time.time() - start_time
    
    if result:
        size_mb = len(json.dumps(result)) / (1024 * 1024)
        print(f"📊 Результат: {size_mb:.2f} MB")
        print(f"⏱️ Время загрузки: {load_time:.3f}s")
        print(f"🚀 Скорость: {size_mb/load_time:.2f} MB/s")
```

## 🛠️ Практические рекомендации

### 1. Структура результата

```python
# ✅ Хорошая структура
result = {
    "metadata": {
        "generated_at": time.time(),
        "version": "1.0",
        "size_mb": calculated_size
    },
    "summary": {
        "total_items": count,
        "processing_time": duration,
        "status": "completed"
    },
    "data": large_data_array,
    "statistics": {
        "mean": mean_value,
        "std_dev": std_value
    }
}

# ❌ Плохая структура
result = large_data_array  # Нет метаданных
```

### 2. Обработка ошибок

```python
def safe_get_large_result(job_id):
    """Безопасное получение большого результата."""
    try:
        status = queue.get_job_status(job_id)
        result = status.get('result')
        
        if not result:
            return None
            
        # Проверяем размер
        size_mb = len(json.dumps(result)) / (1024 * 1024)
        if size_mb > 50:  # Лимит 50MB
            raise ValueError(f"Результат слишком большой: {size_mb:.2f} MB")
            
        return result
        
    except MemoryError:
        print("❌ Недостаточно памяти для загрузки результата")
        return None
    except Exception as e:
        print(f"❌ Ошибка загрузки результата: {e}")
        return None
```

### 3. Очистка старых результатов

```python
def cleanup_old_results(registry_path, max_age_days=30):
    """Очистка старых результатов из реестра."""
    cutoff_time = time.time() - (max_age_days * 24 * 3600)
    
    with open(registry_path, 'r') as f:
        lines = f.readlines()
    
    filtered_lines = []
    for line in lines:
        record = json.loads(line)
        if record.get('updated_at'):
            updated_time = time.mktime(time.strptime(record['updated_at'], '%Y-%m-%dT%H:%M:%S.%f'))
            if updated_time > cutoff_time:
                filtered_lines.append(line)
    
    with open(registry_path, 'w') as f:
        f.writelines(filtered_lines)
```

## 🎯 Полный пример

```python
#!/usr/bin/env python3
"""
Пример работы с большими результатами.
"""

import json
import time
import random
from queuemgr.proc_api import proc_queue_system
from queuemgr.jobs.base import QueueJobBase

class BigDataJob(QueueJobBase):
    def __init__(self, job_id: str, params: dict):
        super().__init__(job_id, params)
        self.target_size_mb = params.get("size_mb", 1.0)
        
    def execute(self) -> None:
        print(f"Генерируем {self.target_size_mb}MB данных...")
        
        # Генерируем большие данные
        data = []
        target_bytes = int(self.target_size_mb * 1024 * 1024)
        current_bytes = 0
        
        while current_bytes < target_bytes:
            item = {
                "id": len(data),
                "value": random.uniform(0, 1000),
                "timestamp": time.time(),
                "data": ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=100))
            }
            data.append(item)
            current_bytes += len(json.dumps(item))
        
        # Сохраняем результат
        result = {
            "data": data,
            "metadata": {
                "size_mb": len(json.dumps(data)) / (1024 * 1024),
                "item_count": len(data),
                "generated_at": time.time()
            }
        }
        
        self.set_result(result)
        print(f"Сгенерировано {len(data)} элементов, {len(json.dumps(data))/(1024*1024):.2f}MB")
        
    def on_start(self) -> None:
        print(f"Запуск генерации больших данных...")
        
    def on_end(self) -> None:
        print(f"Генерация завершена")
        
    def on_error(self, exc: BaseException) -> None:
        print(f"Ошибка: {exc}")

def main():
    with proc_queue_system() as queue:
        # Добавляем задание с большим результатом
        queue.add_job(BigDataJob, "big-data-1", {"size_mb": 2.0})
        queue.start_job("big-data-1")
        
        # Ждем завершения
        time.sleep(5)
        
        # Получаем результат
        status = queue.get_job_status("big-data-1")
        result = status.get('result')
        
        if result:
            size_mb = result['metadata']['size_mb']
            item_count = result['metadata']['item_count']
            print(f"✅ Результат получен: {size_mb:.2f}MB, {item_count} элементов")
        else:
            print("❌ Результат не найден")

if __name__ == "__main__":
    main()
```

## 📚 Дополнительные ресурсы

- `queuemgr/examples/large_result_job.py` - Полный пример с большими результатами
- `queuemgr/examples/result_job.py` - Базовые примеры работы с результатами
- CLI команды: `queuemgr-cli job status <job-id>` - Просмотр результатов
- Веб-интерфейс: `queuemgr-web` - Визуализация результатов

---

**Готово!** Теперь вы знаете, как эффективно работать с большими результатами в Queue Manager! 🚀
