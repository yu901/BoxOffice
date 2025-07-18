import os
import toml

class BaseConfig:
    def __init__(self):
        self.root_path = os.environ.get("ROOT_PATH", ".")
        self.set_secrets_path("/.streamlit/secrets.toml")

        with open(self.secrets_path, "r", encoding="utf8") as f:
            self.config = toml.load(f)

    def set_secrets_path(self, secrets_file_name):
        self.secrets_path = self.root_path + secrets_file_name

    def get_value(self, obj, key):
        if key in obj:
            return obj[key]
        else:
            raise KeyError(key)

class KobisConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.key = self.config["kobis"]["key"]

class SQLiteConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.db_path = self.config["sqlite"]["db_path"]

class GeminiConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.api_key = self.config["gemini"]["api_key"]