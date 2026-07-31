"""Утилита для отправки бинарных файлов в Apache ActiveMQ Artemis по протоколу AMQP 1.0."""

import argparse
import json
import os
import sys
from dotenv import load_dotenv 
from proton import Message, SSLDomain
from proton.handlers import MessagingHandler
from proton.reactor import Container

# Автоматически ищем и загружаем файл .env в системное окружение
load_dotenv()

# Читаем параметры из окружения (теперь они подтянулись из .env)
BROKER_URL = os.environ.get("BROKER_URL", "amqps://127.0.0.1:6667")
DEFAULT_QUEUE = os.environ.get("DEFAULT_QUEUE", "TO.QUEUE")
DEFAULT_HEADERS_FILE = os.environ.get("DEFAULT_HEADERS_FILE", "headers.json")
USER_NAME = os.environ.get("USER_NAME", "main")
USER_PASSWORD = os.environ.get("USER_PASSWORD")


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
        ssl_domain.set_peer_authentication(SSLDomain.ANONYMOUS_PEER, None)

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
        print(f"Критическая ошибка: Брокер отклонил сообщение! Причина: {event.delivery.remote.condition}", file=sys.stderr)
        event.sender.close()
        event.connection.close()
        sys.exit(1)

    def on_connection_error(self, event):
        cond = event.connection.remote_condition
        print(f"Критическая ошибка подключения к URL! Проверьте логин/пароль или адрес брокера.", file=sys.stderr)
        print(f"Детали от брокера: {cond}", file=sys.stderr)
        event.connection.close()
        sys.exit(1)

    def on_link_error(self, event):
        cond = event.link.remote_condition
        print(f"Критическая ошибка очереди! Очередь '{self.queue_name}' не существует, либо у пользователя '{self.user}' нет прав на запись (SEND).", file=sys.stderr)
        print(f"Детали от брокера: {cond}", file=sys.stderr)
        event.link.close()
        event.connection.close()
        sys.exit(1)
            
    def on_transport_error(self, event):
        print(f"Критическая сетевая ошибка (Transport Error)! Проверьте доступность хоста/порта или настройки SSL.", file=sys.stderr)
        print(f"Детали: {event.transport.condition}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    
    parser.add_argument(
        "file", 
        type=str, 
        help="Путь к отправляемому файлу (например: my_photo.jpg или ./data.bin)"
    )
    parser.add_argument(
        "-q", "--queue", 
        type=str, 
        default=DEFAULT_QUEUE, 
        help=f"Имя целевой очереди (по умолчанию из .env: {DEFAULT_QUEUE})"
    )
    parser.add_argument(
        "-c", "--config", 
        type=str, 
        default=DEFAULT_HEADERS_FILE, 
        help=f"Путь к JSON-файлу с заголовками сообщения (по умолчанию из .env: {DEFAULT_HEADERS_FILE})"
    )

    args = parser.parse_args()

    # Проверка обязательного пароля
    if not USER_PASSWORD:
        print("Критическая ошибка: Переменная USER_PASSWORD должна быть задана в файле .env или в окружении системы!", file=sys.stderr)
        sys.exit(1)
        
    assert os.path.exists(args.file), f"Критическая ошибка: Файл для отправки не найден: {args.file}"
    assert os.path.isfile(args.file), f"Критическая ошибка: Указанный путь не является файлом: {args.file}"
    assert os.path.exists(args.config), f"Критическая ошибка: JSON-файл конфигурации заголовков не найден: {args.config}"

    try:
        with open(args.config, "r", encoding="utf-8") as json_file:
            loaded_headers = json.load(json_file)
        assert isinstance(loaded_headers, dict), "Содержимое JSON-файла должно быть объектом (словарем dict)"
    except json.JSONDecodeError as e:
        print(f"Критическая ошибка: Не удалось распарсить JSON-файл заголовков! Причина: {e}", file=sys.stderr)
        sys.exit(1)

    handler = Amqp10Sender(
        broker_url=BROKER_URL,
        queue_name=args.queue,
        file_path=args.file,
        custom_headers=loaded_headers,
        user=USER_NAME,
        password=USER_PASSWORD
    )
    
    Container(handler).run()
