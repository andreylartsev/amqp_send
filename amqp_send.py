"""Утилита для отправки файлов (одного или всей папки) в Apache ActiveMQ Artemis по протоколу AMQP 1.0."""

import argparse
import json
import os
import sys
from pathlib import Path
from dotenv import load_dotenv 
from proton import Message, SSLDomain
from proton.handlers import MessagingHandler
from proton.reactor import Container

class CommandError(Exception):
    """Критическая ошибка, завершающая скрипт."""
    def __init__(self, message):
        super().__init__(f"Критическая ошибка: {message}")

# Автоматически ищем и загружаем файл .env в системное окружение
load_dotenv()

# Читаем параметры из окружения
BROKER_URL = os.environ.get("BROKER_URL", "amqps://127.0.0.1:6667")
DEFAULT_QUEUE = os.environ.get("DEFAULT_QUEUE", "TO.QUEUE")
DEFAULT_HEADERS_FILE = os.environ.get("DEFAULT_HEADERS_FILE", "headers.json")
USER_NAME = os.environ.get("USER_NAME", "main")
USER_PASSWORD = os.environ.get("USER_PASSWORD")


class Amqp10Sender(MessagingHandler):
    def __init__(self, broker_url, queue_name, file_queue: list[Path], custom_headers, user, password):
        super().__init__()
        self.broker_url = broker_url
        self.queue_name = queue_name
        self.custom_headers = custom_headers
        self.user = user
        self.password = password
        
        # Очередь файлов на отправку
        self.file_queue = file_queue
        self.current_file: Path = None
        self.sending_in_progress = False
        self.fatal_error: CommandError = None  # Объект исключения для передачи в основной поток

        if not self.file_queue:
            print("Предупреждение: Список файлов для отправки пуст.")

    def on_start(self, event):
        if not self.file_queue:
            event.container.stop()
            return

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

    def _send_next(self, sender):
        """Внутренний метод для последовательной отправки файлов."""
        if self.sending_in_progress or not self.file_queue:
            return

        if sender.credit <= 0:
            return

        self.current_file = self.file_queue.pop(0)
        self.sending_in_progress = True
        
        print(f"Отправляем файл [{len(self.file_queue) + 1} осталось]: {self.current_file.name}...")
        
        try:
            binary_content = self.current_file.read_bytes()
        except Exception as e:
            print(f"Предупреждение: Не удалось прочитать файл {self.current_file.name}: {e}", file=sys.stderr)
            self.sending_in_progress = False
            self._send_next(sender)
            return

        msg = Message()
        msg.properties = self.custom_headers
        msg.body = binary_content
        msg.address = self.queue_name

        sender.send(msg)

    def on_sendable(self, event):
        self._send_next(event.sender)

    def on_accepted(self, event):
        print(f"Успех! Брокер подтвердил получение файла: {self.current_file.name}")
        self.sending_in_progress = False
        
        if self.file_queue:
            self._send_next(event.sender)
        else:
            print("Все файлы успешно отправлены!")
            event.sender.close()
            event.connection.close()

    def on_rejected(self, event):
        reason = str(event.delivery.remote.condition)
        event.sender.close()
        event.connection.close()
        
        self.fatal_error = CommandError(f"Брокер отклонил файл {self.current_file.name}! Причина: {reason}")
        event.container.stop()

    def on_connection_error(self, event):
        reason = str(event.connection.remote_condition)
        event.connection.close()
        
        self.fatal_error = CommandError(f"Ошибка подключения! Проверьте логин/пароль или адрес брокера. Детали: {reason}")
        event.container.stop()

    def on_link_error(self, event):
        reason = event.link.remote_condition
        event.link.close()
        event.connection.close()
        
        self.fatal_error = CommandError(f"Ошибка очереди! Адрес '{self.queue_name}' недоступен. Детали: {reason}")
        event.container.stop()
            
    def on_transport_error(self, event):
        reason = event.transport.condition
        
        self.fatal_error = CommandError(f"Критическая сетевая ошибка! Детали: {reason}")
        event.container.stop()


def load_headers_json(headers_path: Path) -> dict:
    """Безопасно читает JSON-файл заголовков с автоподбором кодировок (UTF-8-SIG / UTF-16)."""
    try:
        try:
            content = headers_path.read_text(encoding="utf-8-sig")
            loaded_headers = json.loads(content)
        except UnicodeDecodeError:
            content = headers_path.read_text(encoding="utf-16")
            loaded_headers = json.loads(content)
            
        if not isinstance(loaded_headers, dict):
            raise CommandError("Содержимое JSON-файла должно быть объектом (словарем dict)")
            
        return loaded_headers
        
    except json.JSONDecodeError as e:
        raise CommandError(f"Не удалось распарсить JSON заголовков! Причина: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "path", 
        type=str, 
        help="Путь к файлу или папке с файлами для отправки"
    )
    parser.add_argument(
        "-q", "--queue", 
        type=str, 
        default=DEFAULT_QUEUE, 
        help="Имя целевой очереди"
    )
    parser.add_argument(
        "-H", "--headers", 
        type=str, 
        default=DEFAULT_HEADERS_FILE, 
        help="Путь к JSON-файлу с заголовками"
    )

    args = parser.parse_args()

    try:
        if not USER_PASSWORD:
            raise CommandError("Переменная USER_PASSWORD должна быть задана в файле .env или в окружении системы!")
            
        input_path = Path(args.path)
        headers_path = Path(args.headers)

        if not input_path.exists():
            raise CommandError(f"Указанный путь не существует: {input_path}")
        if not headers_path.exists():
            raise CommandError(f"JSON-файл заголовков не найден: {headers_path}")

        # Автоматическое определение типа переданного пути
        files_to_send = []
        if input_path.is_file():
            print("Режим: Отправка одиночного файла.")
            files_to_send.append(input_path)
        elif input_path.is_dir():
            print("Режим: Отправка всех файлов из папки.")
            files_to_send = [f for f in input_path.iterdir() if f.is_file()]
        else:
            raise CommandError(f"Указанный путь не поддерживается: {input_path}")

        # Чтение заголовков через функцию
        loaded_headers = load_headers_json(headers_path)

        handler = Amqp10Sender(
            broker_url=BROKER_URL,
            queue_name=args.queue,
            file_queue=files_to_send,
            custom_headers=loaded_headers,
            user=USER_NAME,
            password=USER_PASSWORD
        )
        
        # Запуск реактора AMQP
        Container(handler).run()
        
        # Если реактор сохранил ошибку, выбрасываем её наружу в блок обработки
        if handler.fatal_error:
            raise handler.fatal_error

    except CommandError as e:
        print(e, file=sys.stderr)
        sys.exit(1)
