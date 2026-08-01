# Утилита отправки файлов в Apache ActiveMQ Artemis (AMQP 1.0)

Скрипт на Python для отправки бинарных файлов (или папок с файлами) с пользовательскими заголовками в очереди Artemis. Работает по защищенному протоколу **AMQP 1.0 (SSL/TLS)** с автоматическим игнорированием самоподписанных сертификатов.

---

## 1. Получение утилиты из GitHub

Склонируйте репозиторий с проектом на вашу локальную машину или целевой сервер:

### По протоколу HTTPS
```bash
git clone https://github.com/andreylartsev/amqp_send.git
cd amqp_send
```

---

## 2. Установка системных зависимостей

### WSL (Ubuntu) / Astra Linux

Поскольку библиотека `python-qpid-proton` компилирует свое C-ядро при установке, в системе должны быть установлены компилятор и заголовочные файлы Python. 

```bash
sudo apt update
sudo apt install -y build-essential python3-dev python3-venv python3-pip
```

### РЕД ОС (7.3 / 8.0)
```bash
sudo dnf check-update
sudo dnf groupinstall -y "Development Tools"
sudo dnf install -y python3-devel
```

### Windows

Для Windows **не требуется** устанавливать компиляторы C++, CMake или SWIG. Вместо сборки из исходников используется официальный заранее скомпилированный бинарный пакет (`wheel`).

---

## 3. Развертывание виртуального окружения (Python venv)

Перейдите в директорию проекта и выполните команды для изоляции зависимостей:

```bash
# 1. Создаем виртуальное окружение с именем .venv
python3 -m venv .venv

# 2. Активируем его
source ./.venv/bin/activate

# После активации в начале строки терминала появится префикс (.venv)
```

Windows PowerShell

```powershell

python3 -m venv .venv
# Скорее всего потребуется разрешить запуск скриптов в PowerShell, политики "по умолчанию" их запрещают
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
./.venv/Scripts/Activate.ps1

```

Обновите менеджер пакетов `pip` и установите зависимости из файла `requirements.txt`:

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

После чего можно проверить установились ли все зависимости и запускается ли скрипт

Windows PowerShell

```powershell
(.venv) PS C:\Users\andrey.larcev\Projects\amqp_send> python.exe .\amqp_send.py -h
usage: amqp_send.py [-h] [-q QUEUE] [-H HEADERS] path

Утилита для отправки файлов (одного или всей папки) в Apache ActiveMQ Artemis по протоколу AMQP 1.0.

positional arguments:
  path                  Путь к файлу или папке с файлами для отправки

options:
  -h, --help            show this help message and exit
  -q QUEUE, --queue QUEUE
                        Имя целевой очереди
  -H HEADERS, --headers HEADERS
                        Путь к JSON-файлу с заголовками
```

---

## 4. Конфигурация и структура файлов

### Шаг 1. Статические настройки брокера

Параметры зафиксированы в файле .env. При необходимости измените URL брокера, пользователя, имя очереди по умолчанию или пароль.
В репозитории находится файл примера конфигурации [.env.example](.env.example), переименуйте его в .env и установите в нем необходимые в вашем окружении параметры:

```python
# ==========================================
# Настройки подключения к брокеру сообщений
# ==========================================

# Адрес локального брокера (используем amqps для TLS)
BROKER_URL=amqps://127.0.0.1:6667

# Имя очереди по умолчанию для входящих пакетов
DEFAULT_QUEUE=TO.QUEUE

# Файл с метаданными и заголовками сообщения
DEFAULT_HEADERS_FILE=headers.json # Должен лежать в корне проекта

# Секретные данные для авторизации
USER_NAME=user
USER_PASSWORD=topsecret
```

После переименования файла, параметры будут применятся автоматически. 

Так же любой из параметров можно передать через переменные среды окружения:
(Windows PowerShell)
```powershell
(.venv) PS C:\Users\andrey.larcev\Projects\amqp_send> $env:USER_NAME="nobody"
(.venv) PS C:\Users\andrey.larcev\Projects\amqp_send> python.exe .\amqp_send.py .\README.md
Подключаемся к: amqps://xx.xx.xx.xx:yyyy, пользователем: nobody
Критическая сетевая ошибка (Transport Error)! Проверьте доступность хоста/порта или настройки SSL.
Детали: Condition('amqp:unauthorized-access', 'Authentication failed [mech=PLAIN]')

# У nobody нет доступа
```


### Шаг 2. Настройка JSON-заголовков сообщения

В корне папки есть файл [headers.json](headers.json). В нем описываются пользовательские свойства (Properties) AMQP-сообщения, которые уйдут вместе с файлом:

```json
{
  "custom-header-1": "myValue",
  "file-type": "binary",
  "version": 1.0
}
```

Можно создать собственный файл с заголовками и передавать его имя вместе с именем файла для отправки через параметры командной строки

---

## 5. Инструкция по использованию

Утилита автоматически проверяет наличие файлов, валидность JSON-структуры и наличие пароля в системе с помощью жестких проверок `assert`.

### Запуск со стандартными настройками
Задайте пароль от учетной записи брокера в переменную окружения `USER_PASSWORD` и передайте путь к отправляемому файлу в качестве аргумента:
```bash
USER_PASSWORD="ваш_секретный_пароль" python amqp_send.py путь_к_файлу.ext
```
в Windows PowerShell
``` powershell
$env:USER_PASSWORD="ваш_секретный_пароль"
python amqp_send.py путь_к_файлу.ext
```
*Скрипт автоматически возьмет заголовки из `headers.json` и отправит файл в очередь `TO.QUEUE`.*

### Отправка в другую очередь (Флаг `-q`)
Вы можете переопределить целевую очередь без изменения кода скрипта:
(Linux)
```bash
USER_PASSWORD="ваш_секретный_пароль" python amqp_send.py file.bin -q ANOTHER.QUEUE
```

### Использование альтернативного файла заголовков (Флаг `-c`)
Если для разных типов файлов нужны разные метаданные, укажите путь к кастомному JSON-конфигу:
(Linux)
```bash
USER_PASSWORD="ваш_секретный_пароль" python amqp_send.py file.bin -c custom_meta.json
```

### Ожидаемый результат работы
В случае успешного выполнения и получения официального подтверждения (ACK) от брокера, утилита выведет:
```text
Подключаемся к: amqps://127.0.0.1:6667, пользователем: main
Отправляем файл: README.md на адрес: TO.QUEUE...
Файл отправлен в сеть, ожидаем подтверждения (ACK) от брокера...
Успех! Брокер Artemis подтвердил получение и сохранение файла README.md!
```

### Альтернативно можно отправить файлы из папки указав вместо конкретного файла директорию:
(Windows PowerShell)
```powershell
(.venv) PS C:\Users\andrey.larcev\Projects\amqp_send> python.exe .\amqp_send.py .\files_to_send\
Режим: Отправка всех файлов из папки.
Подключаемся к: amqps://xxx:6667, пользователем: user
Отправляем файл [2 осталось]: file1.txt...
Успех! Брокер подтвердил получение файла: file1.txt
Отправляем файл [1 осталось]: file2.txt...
Успех! Брокер подтвердил получение файла: file2.txt
Все файлы успешно отправлены!
```

## Деактивация окружения
Выйти из виртуального окружения по окончании работы можно командой:
```bash
deactivate
```
