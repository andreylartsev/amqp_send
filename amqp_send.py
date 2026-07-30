import os
import sys
from proton import Message, SSLDomain, symbol
from proton.handlers import MessagingHandler
from proton.reactor import Container

# ==================== КОНФИГУРАЦИЯ СКРИПТА ====================
BROKER_URL = "amqps://10.3.124.31:61627"  # Только адрес брокера (без очереди!)
QUEUE_NAME = "TO.KM"                      # Строго имя очереди
USER_NAME = "main"                        # Имя пользователя брокера
FILE_PATH = "README.md"                   # Путь к файлу

CUSTOM_HEADERS = {                        # Кастомные заголовки (Properties)
    "custom-header-1": "myValue",
    "file-type": "binary",
    "version": 1.0
}
# ==============================================================

# Безопасное чтение пароля
USER_PASSWORD = os.environ.get("USER_PASSWORD")

# Валидация конфигурации через assert
assert USER_PASSWORD, "Переменная окружения USER_PASSWORD должна быть установлена и не быть пустой"
assert isinstance(BROKER_URL, str) and len(BROKER_URL.strip()) > 0, "BROKER_URL должен быть непустой строкой"
assert isinstance(QUEUE_NAME, str) and len(QUEUE_NAME.strip()) > 0, "QUEUE_NAME должен быть непустой строкой"
assert isinstance(USER_NAME, str) and len(USER_NAME.strip()) > 0, "USER_NAME должен быть непустой строкой"
assert os.path.exists(FILE_PATH), f"Файл не найден по пути: {FILE_PATH}"


class Amqp10Sender(MessagingHandler):
    def __init__(self, broker_url, queue_name, file_path, custom_headers, user, password):
        super(Amqp10Sender, self).__init__()
        self.broker_url = broker_url
        self.queue_name = queue_name
        self.file_path = file_path
        self.headers = custom_headers
        self.user = user
        self.password = password
        self.sent = False  # Флаг для предотвращения повторной отправки (зацикливания)

    def on_start(self, event):
        ssl_domain = SSLDomain(SSLDomain.MODE_CLIENT)
        ssl_domain.set_peer_authentication(SSLDomain.ANONYMOUS_PEER, "")

        # Подключаемся строго к адресу брокера (без слеша и очереди в конце)
        conn = event.container.connect(
            self.broker_url, 
            user=self.user, 
            password=self.password, 
            ssl_domain=ssl_domain
        )
        print(f"Подключаемся к: {self.broker_url}, пользователем: {self.user}")
        
        # Явно создаем не-анонимного отправителя, привязанного к конкретной очереди
        event.container.create_sender(conn, target=self.queue_name)

    def on_sendable(self, event):
        # Если файл уже отправлен, игнорируем новые кредиты от брокера
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
            
            # Явно прописываем системное поле 'to' на случай, если брокер проверяет его
            msg.address = self.queue_name

            sender.send(msg)
            self.sent = True  # Фиксируем отправку одной копии
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
    handler = Amqp10Sender(
        broker_url=BROKER_URL,
        queue_name=QUEUE_NAME,
        file_path=FILE_PATH,
        custom_headers=CUSTOM_HEADERS,
        user=USER_NAME,
        password=USER_PASSWORD
    )
    
    Container(handler).run()
