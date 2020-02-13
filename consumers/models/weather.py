"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        try:
            msg_json = json.loads(message.value())
            self.temperature = msg_json["temperature"]
            self.status = msg_json["status"]
        except KeyError as e:
            logger.error(f"error caugt in weather process_message {e}")
