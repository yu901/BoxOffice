import os
import yaml

class BaseConfig:
    def __init__(self):
        self.root_path = os.environ.get("ROOT_PATH", ".")
        self.set_yml_path("/config/config.yml")
        if not os.path.exists(self.yml_path):
            encrypt_yml_path = "/config/config.yml"
            print(f"{self.yml_path} does not exist. use {encrypt_yml_path}")
            self.set_yml_path(encrypt_yml_path)

        with open(self.yml_path, "r", encoding="utf8") as f:
            self.config = yaml.load(f, Loader=yaml.FullLoader)

    def set_yml_path(self, yml_file_name):
        config_path = os.environ.get("CONFIG_PATH", yml_file_name)
        self.conf_dir = config_path
        self.yml_path = self.root_path + self.conf_dir

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