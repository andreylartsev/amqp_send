"""Утилита для отправки бинарных файлов в Apache ActiveMQ Artemis по протоколу AMQP 1.0."""

import argparse
import json
import os
import sys
from proton import Message, SSLDomain
from proton.handlers import MessagingHandler
from proton.reactor import Container

# ==================== СТАТИЧЕСКАЯ КОНФИГУРАЦИЯ БРОКЕРА ====================
BROKER_URL = "amqps://127.0.0.1:61627"  # Только адрес брокера (без очереди)
DEFAULT_QUEUE = "TO.QUEUE"                   # Очередь по умолчанию
DEFAULT_HEADERS_FILE = "headers.json"     # JSON-файл заголовков по умолчанию
USER_NAME = "main"                        # Имя пользователя брокера

# ==========================================================================

class Amqp10Sender(MessagingHandler):
    def __init__(self, broker_url, queue_name, file_path, custom_headers, user, password):
        super(Amqp10Sender, self).__init__()
        self.broker_url = broker_url
        self.queue_name = queue_name
        self.file_path = file_path
        self.headers = custom_headers
        self.user = user
        self.password = password
        self.sent = False

    def on_start(self, event):
        ssl_domain = SSLDomain(SSLDomain.MODE_CLIENT)
        ssl_domain.set_peer_authentication(SSLDomain.ANONYMOUS_PEER, "")

        conn = event.container.connect(
            self.broker_url, 
            user=self.user, 
            password=self.password, 
            ssl_domain=ssl_domain
        )
        print(f"Подключаемся к: {self.broker_url}, пользователем: {self.user}")
        event.container.create_sender(conn, target=self.queue_name)

    def on_sendable(self, event):
        if self.sent:
            return

        sender = event.sender
        if sender.credit > 0:
            print(f"Отправляем файл: {self.file_path} на адрес: {self.queue_name}...")
            with open(self.file_path, "rb") as f:
                binary_content = f.read()

            msg = Message()
            msg.properties = self.headers
            msg.body = binary_content
            msg.address = self.queue_name

            sender.send(msg)
            self.sent = True
            print("Файл отправлен в сеть, ожидаем подтверждения (ACK) от брокера...")

    def on_accepted(self, event):
        print(f"Успех! Брокер Artemis подтвердил получение и сохранение файла {self.file_path}!")
        event.sender.close()
        event.connection.close()

    def on_rejected(self, event):
        print(f"Ошибка: Брокер отклонил сообщение! Причина: {event.delivery.remote.condition}", file=sys.stderr)
        event.sender.close()
        event.connection.close()
        sys.exit(1)
            
    def on_transport_error(self, event):
        print(f"Сетевая ошибка AMQP/SSL: {event.transport.condition}", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    
    # Позиционный аргумент: путь к бинарному файлу
    parser.add_argument(
        "file", 
        type=str, 
        help="Путь к отправляемому файлу (например: my_photo.jpg или ./data.bin)"
    )
    
    # Опциональный аргумент: имя очереди
    parser.add_argument(
        "-q", "--queue", 
        type=str, 
        default=DEFAULT_QUEUE, 
        help=f"Имя целевой очереди (по умолчанию: {DEFAULT_QUEUE})"
    )

    # Опциональный аргумент: путь к JSON-файлу с заголовками
    parser.add_argument(
        "-c", "--config", 
        type=str, 
        default=DEFAULT_HEADERS_FILE, 
        help=f"Путь к JSON-файлу с заголовками сообщения (по умолчанию: {DEFAULT_HEADERS_FILE})"
    )

    args = parser.parse_args()

    # Чтение пароля
    USER_PASSWORD = os.environ.get("USER_PASSWORD")

    # Валидация аргументов файловой системы
    assert USER_PASSWORD, "Переменная окружения USER_PASSWORD должна быть установлена и не быть пустой"
    assert os.path.exists(args.file), f"Критическая ошибка: Файл для отправки не найден: {args.file}"
    assert os.path.isfile(args.file), f"Критическая ошибка: Указанный путь не является файлом (возможно, это директория): {args.file}"
    assert os.path.exists(args.config), f"Критическая ошибка: JSON-файл конфигурации заголовков не найден: {args.config}"

    # Чтение и парсинг JSON-файла с заголовками
    try:
        with open(args.config, "r", encoding="utf-8") as json_file:
            loaded_headers = json.load(json_file)
        assert isinstance(loaded_headers, dict), "Содержимое JSON-файла должно быть объектом (словарем dict)"
    except json.JSONDecodeError as e:
        print(f"Критическая ошибка: Не удалось распарсить JSON-файл заголовков! Причина: {e}", file=sys.stderr)
        sys.exit(1)

    # Запуск продюсера с динамическими заголовками
    handler = Amqp10Sender(
        broker_url=BROKER_URL,
        queue_name=args.queue,
        file_path=args.file,
        custom_headers=loaded_headers,
        user=USER_NAME,
        password=USER_PASSWORD
    )
    
    Container(handler).run()
