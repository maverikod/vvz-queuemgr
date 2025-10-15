# Queue Manager Service

Полноценный сервис для управления очередями заданий с поддержкой systemd, CLI и веб-интерфейса.

## 🚀 Быстрый старт

### Установка сервиса

```bash
# Клонируйте репозиторий
git clone <repository-url>
cd queuemgr

# Запустите установку (требуются права root)
sudo ./install.sh
```

### Проверка статуса

```bash
# Проверить статус сервиса
sudo systemctl status queuemgr

# Посмотреть логи
sudo journalctl -u queuemgr -f
```

## 🎛️ Управление сервисом

### Systemd команды

```bash
# Запустить сервис
sudo systemctl start queuemgr

# Остановить сервис
sudo systemctl stop queuemgr

# Перезапустить сервис
sudo systemctl restart queuemgr

# Включить автозапуск
sudo systemctl enable queuemgr

# Отключить автозапуск
sudo systemctl disable queuemgr
```

### CLI интерфейс

```bash
# Проверить статус сервиса
/opt/queuemgr/.venv/bin/python -m queuemgr.service.cli service status

# Запустить сервис
/opt/queuemgr/.venv/bin/python -m queuemgr.service.cli service start

# Остановить сервис
/opt/queuemgr/.venv/bin/python -m queuemgr.service.cli service stop

# Показать все задания
/opt/queuemgr/.venv/bin/python -m queuemgr.service.cli job list

# Добавить задание
/opt/queuemgr/.venv/bin/python -m queuemgr.service.cli job add

# Запустить задание
/opt/queuemgr/.venv/bin/python -m queuemgr.service.cli job start

# Остановить задание
/opt/queuemgr/.venv/bin/python -m queuemgr.service.cli job stop

# Удалить задание
/opt/queuemgr/.venv/bin/python -m queuemgr.service.cli job delete

# Мониторинг в реальном времени
/opt/queuemgr/.venv/bin/python -m queuemgr.service.cli monitor
```

### Веб-интерфейс

```bash
# Запустить веб-интерфейс
/opt/queuemgr/.venv/bin/python -m queuemgr.service.web

# Веб-интерфейс будет доступен по адресу:
# http://localhost:5000
```

## 📁 Структура сервиса

```
/opt/queuemgr/                    # Основная директория
├── .venv/                        # Виртуальное окружение
├── queuemgr/                     # Код приложения
│   ├── service/                  # Сервисные модули
│   │   ├── daemon.py            # Демон сервиса
│   │   ├── cli.py               # CLI интерфейс
│   │   └── web.py               # Веб интерфейс
│   └── ...
└── queuemgr.service              # systemd сервис

/var/lib/queuemgr/                # Данные приложения
├── registry.jsonl               # Реестр заданий

/var/log/queuemgr/                # Логи
├── daemon.log                   # Лог демона

/var/run/queuemgr/                # Runtime файлы
├── daemon.pid                   # PID файл
└── proc/                        # IPC через /proc
```

## 🔧 Конфигурация

### Основные параметры

```python
# /opt/queuemgr/config.py
PROC_MANAGER_CONFIG = {
    "registry_path": "/var/lib/queuemgr/registry.jsonl",
    "proc_dir": "/var/run/queuemgr",
    "shutdown_timeout": 30.0,
    "cleanup_interval": 300.0,  # 5 минут
    "max_concurrent_jobs": 50
}
```

### Systemd настройки

```ini
# /etc/systemd/system/queuemgr.service
[Unit]
Description=Queue Manager Service
After=network.target

[Service]
Type=forking
User=queuemgr
Group=queuemgr
WorkingDirectory=/opt/queuemgr
ExecStart=/opt/queuemgr/.venv/bin/python -m queuemgr.service.daemon start --daemon
ExecStop=/opt/queuemgr/.venv/bin/python -m queuemgr.service.daemon stop
PIDFile=/var/run/queuemgr/daemon.pid
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## 📊 Мониторинг

### Логи сервиса

```bash
# Посмотреть все логи
sudo journalctl -u queuemgr

# Посмотреть логи за последний час
sudo journalctl -u queuemgr --since "1 hour ago"

# Следить за логами в реальном времени
sudo journalctl -u queuemgr -f
```

### Статистика заданий

```bash
# CLI статистика
/opt/queuemgr/.venv/bin/python -m queuemgr.service.cli service status

# Веб-интерфейс
curl http://localhost:5000/api/status
```

## 🛠️ Разработка

### Добавление новых типов заданий

1. Создайте класс задания:

```python
# queuemgr/jobs/my_job.py
from queuemgr.jobs.base import QueueJobBase

class MyJob(QueueJobBase):
    def __init__(self, job_id: str, params: dict):
        super().__init__(job_id, params)
        
    def execute(self) -> None:
        # Ваша логика задания
        pass
        
    def on_start(self) -> None:
        # Вызывается при запуске
        pass
        
    def on_stop(self) -> None:
        # Вызывается при остановке
        pass
        
    def on_end(self) -> None:
        # Вызывается при завершении
        pass
        
    def on_error(self, exc: Exception) -> None:
        # Вызывается при ошибке
        pass
```

2. Зарегистрируйте в CLI:

```python
# queuemgr/service/cli.py
def _import_job_class(self, job_class_name: str):
    if job_class_name == "MyJob":
        from queuemgr.jobs.my_job import MyJob
        return MyJob
    # ... другие классы
```

### Кастомизация веб-интерфейса

```python
# queuemgr/service/web.py
def create_web_app():
    app = Flask(__name__)
    
    # Добавьте свои маршруты
    @app.route('/api/custom')
    def custom_endpoint():
        return jsonify({"message": "Custom endpoint"})
        
    return app
```

## 🔒 Безопасность

### Права доступа

```bash
# Проверить права
ls -la /opt/queuemgr/
ls -la /var/lib/queuemgr/
ls -la /var/log/queuemgr/
ls -la /var/run/queuemgr/

# Исправить права (если нужно)
sudo chown -R queuemgr:queuemgr /opt/queuemgr
sudo chown -R queuemgr:queuemgr /var/lib/queuemgr
sudo chown -R queuemgr:queuemgr /var/log/queuemgr
sudo chown -R queuemgr:queuemgr /var/run/queuemgr
```

### Firewall настройки

```bash
# Открыть порт для веб-интерфейса (если нужно)
sudo ufw allow 5000/tcp
```

## 🐛 Отладка

### Проблемы с запуском

```bash
# Проверить статус
sudo systemctl status queuemgr

# Проверить логи
sudo journalctl -u queuemgr -n 50

# Проверить права
sudo -u queuemgr ls -la /var/lib/queuemgr/
sudo -u queuemgr ls -la /var/run/queuemgr/
```

### Проблемы с заданиями

```bash
# Проверить статус заданий
/opt/queuemgr/.venv/bin/python -m queuemgr.service.cli job list

# Проверить конкретное задание
/opt/queuemgr/.venv/bin/python -m queuemgr.service.cli job status <job_id>
```

## 📈 Производительность

### Настройка ресурсов

```bash
# Увеличить лимиты для systemd
sudo systemctl edit queuemgr

# Добавить:
[Service]
LimitNOFILE=65536
LimitNPROC=4096
```

### Мониторинг ресурсов

```bash
# Использование памяти
ps aux | grep queuemgr

# Использование CPU
top -p $(pgrep -f queuemgr)

# Использование диска
du -sh /var/lib/queuemgr/
du -sh /var/log/queuemgr/
```

## 🔄 Обновление

```bash
# Остановить сервис
sudo systemctl stop queuemgr

# Обновить код
cd /opt/queuemgr
git pull

# Перезапустить сервис
sudo systemctl start queuemgr
```

## 🗑️ Удаление

```bash
# Остановить и отключить сервис
sudo systemctl stop queuemgr
sudo systemctl disable queuemgr

# Удалить systemd сервис
sudo rm /etc/systemd/system/queuemgr.service
sudo systemctl daemon-reload

# Удалить файлы
sudo rm -rf /opt/queuemgr
sudo rm -rf /var/lib/queuemgr
sudo rm -rf /var/log/queuemgr
sudo rm -rf /var/run/queuemgr

# Удалить пользователя
sudo userdel queuemgr
```
