# 📦 Публикация Queue Manager на PyPI

## Подготовка к публикации

### 1. Установите необходимые инструменты

```bash
# Установите build и twine
pip install --upgrade build twine

# Или через pipx (рекомендуется)
pipx install build
pipx install twine
```

### 2. Создайте аккаунт на PyPI

1. Перейдите на https://pypi.org/account/register/
2. Создайте аккаунт
3. Включите двухфакторную аутентификацию

### 3. Создайте API токен

1. Перейдите в настройки аккаунта PyPI
2. Создайте API токен для проекта
3. Сохраните токен в безопасном месте

## Сборка и публикация

### 1. Очистите старые сборки

```bash
rm -rf build/ dist/ *.egg-info/
```

### 2. Соберите пакет

```bash
# Сборка с помощью build
python -m build

# Или с помощью setuptools
python setup.py sdist bdist_wheel
```

### 3. Проверьте сборку

```bash
# Проверка с помощью twine
twine check dist/*

# Тестовая загрузка на TestPyPI
twine upload --repository testpypi dist/*
```

### 4. Публикация на PyPI

```bash
# Загрузка на PyPI
twine upload dist/*

# Или с указанием репозитория
twine upload --repository pypi dist/*
```

## Автоматизация с GitHub Actions

Создайте файл `.github/workflows/publish.yml`:

```yaml
name: Publish to PyPI

on:
  release:
    types: [published]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.12'
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install build twine
    
    - name: Build package
      run: python -m build
    
    - name: Publish to PyPI
      env:
        TWINE_USERNAME: __token__
        TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}
      run: twine upload dist/*
```

## Проверка после публикации

### 1. Установите пакет

```bash
# Установка из PyPI
pip install queuemgr

# С дополнительными зависимостями
pip install queuemgr[web,examples]
```

### 2. Проверьте функциональность

```bash
# Проверьте CLI команды
queuemgr-cli --help
queuemgr-daemon --help
queuemgr-web --help

# Запустите пример
python -c "from queuemgr.examples.simple_job import main; main()"
```

## Обновление версии

### 1. Обновите версию

В файлах:
- `setup.py`: `version="1.0.1"`
- `pyproject.toml`: `version = "1.0.1"`
- `queuemgr/__init__.py`: `__version__ = "1.0.1"`

### 2. Создайте тег

```bash
git tag v1.0.1
git push origin v1.0.1
```

### 3. Создайте релиз на GitHub

Это автоматически запустит публикацию на PyPI через GitHub Actions.

## Структура пакета

После установки пользователи получат:

```
queuemgr/
├── __init__.py
├── core/           # Основные модули
├── jobs/           # Базовые классы заданий
├── queue/          # Система очередей
├── service/        # Сервисные модули
├── examples/        # Примеры использования
└── ...

# CLI команды
queuemgr-daemon     # Демон сервиса
queuemgr-cli        # CLI интерфейс
queuemgr-web        # Веб-интерфейс
```

## Использование после установки

```python
# Простое использование
from queuemgr.proc_api import proc_queue_system
from queuemgr.jobs.base import QueueJobBase

# Создание задания
class MyJob(QueueJobBase):
    def execute(self):
        print("Выполняю задание!")

# Использование
with proc_queue_system() as queue:
    queue.add_job(MyJob, "my-job", {})
    queue.start_job("my-job")
```

## Мониторинг загрузок

После публикации вы можете отслеживать:
- Количество загрузок на https://pypi.org/project/queuemgr/
- Статистику использования
- Отзывы и issues

## 🎉 Готово!

Ваш пакет Queue Manager теперь доступен на PyPI и может быть установлен командой:

```bash
pip install queuemgr
```
