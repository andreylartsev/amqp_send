import os
import sys
from proton import Message, Data, SSLDomain
from proton.handlers import MessagingHandler
from proton.reactor import Container

# ==================== КОНФИГУРАЦИЯ СКРИПТА ====================
BROKER_URL = "amqps://10.3.124.31:61627"  # Адрес брокера (обязательно amqps:// для SSL)
QUEUE_NAME = "TO.KM"                      # Имя целевой очереди (Address в Artemis)
USER_NAME = "main"                        # Имя пользователя брокера
FILE_PATH = "README.md"                   # Относительный или абсолютный путь к файлу

CUSTOM_HEADERS = {                        # Кастомные свойства/заголовки сообщения (Properties)
    "custom-header-1": "myValue",
    "file-type": "binary",
    "version": 1.0
}
# ==============================================================

# Безопасное чтение пароля из переменной окружения
USER_PASSWORD = os.environ.get("USER_PASSWORD")

# Валидация конфигурации через assert
assert USER_PASSWORD, "Переменная окружения USER_PASSWORD должна быть установлена и не быть пустой"
assert isinstance(BROKER_URL, str) and len(BROKER_URL.strip()) > 0, "BROKER_URL должен быть непустой строкой"
assert isinstance(QUEUE_NAME, str) and len(QUEUE_NAME.strip()) > 0, "QUEUE_NAME должен быть непустой строкой"
assert isinstance(USER_NAME, str) and len(USER_NAME.strip()) > 0, "USER_NAME должен быть непустой строкой"
assert isinstance(FILE_PATH, str) and len(FILE_PATH.strip()) > 0, "FILE_PATH должен быть непустой строкой"
assert os.path.exists(FILE_PATH), f"Целевой бинарный файл не найден по пути: {FILE_PATH}"
assert os.path.isfile(FILE_PATH), f"Указанный путь не является файлом: {FILE_PATH}"
assert isinstance(CUSTOM_HEADERS, dict), "CUSTOM_HEADERS должен быть словарем (dict)"


class Amqp10Sender(MessagingHandler):
    def __init__(self, broker_url, queue_name, file_path, custom_headers, user, password):
        super(Amqp10Sender, self).__init__()
        self.url = f"{broker_url}/{queue_name}"
        self.file_path = file_path
        self.headers = custom_headers
        self.user = user
        self.password = password

    def on_start(self, event):
        # Настройка SSL Домена для работы по защищенному протоколу amqps
        ssl_domain = SSLDomain(SSLDomain.MODE_CLIENT)
        
        # ANONYMOUS_PEER отключает проверку цепочки доверия CA.
        # Передача пустой строки "" в качестве name_of_peer полностью отключает проверку соответствия имени хоста.
        ssl_domain.set_peer_authentication(SSLDomain.ANONYMOUS_PEER, "")

        # Устанавливаем защищенное соединение
        conn = event.container.connect(
            self.url, 
            user=self.user, 
            password=self.password, 
            ssl_domain=ssl_domain
        )
        print(f"Подключаемся к (SSL без проверок): {self.url}, пользователем: {self.user}")
        event.container.create_sender(conn)

    def on_sendable(self, event):
        sender = event.sender
        if sender.credit > 0:
            print(f"Отправляем файл: {self.file_path}...")
            with open(self.file_path, "rb") as f:
                binary_content = f.read()

            msg = Message()
            msg.properties = self.headers
            msg.body = binary_content

            sender.send(msg)
            print(f"Файл {self.file_path} успешно отправлен по защищенному протоколу AMQP 1.0 (SSL)!")
            
            sender.close()
            event.connection.close()
            
    def on_transport_error(self, event):
        # Логирование сетевых, протокольных или SSL ошибок
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
