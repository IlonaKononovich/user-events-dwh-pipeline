import os
import logging
import requests

# Чтение переменных окружения с токеном бота и ID чата Telegram
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def notify_telegram(message: str, silent: bool = False)  -> None:
    """
    Отправка сообщения в Telegram через Bot API.

    :param message: текст сообщения
    :param silent: если True, отправить без звукового уведомления
    """
    # Пропускаем отправку, если не настроены переменные окружения
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.debug("Telegram: переменные окружения не заданы, отправка пропущена.")
        return

    try:
        # Выполняем POST-запрос к Telegram Bot API
        response = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "disable_notification": silent
            },
            timeout=5  # короткий таймаут, чтобы не блокировать процесс при зависании
        )
        # Логируем предупреждение, если ответ с ошибкой
        if response.status_code != 200:
            logging.warning(f"Telegram: ошибка {response.status_code} — {response.text}")
    except Exception as e:
        # Логируем исключения, чтобы знать о проблемах с сетью или API
        logging.error(f"Ошибка при отправке сообщения в Telegram: {e}")

if __name__ == "__main__":
    pass
