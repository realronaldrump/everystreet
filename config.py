from pydantic_settings import BaseSettings


class Config(BaseSettings):
    PIN: str
    CLIENT_ID: str
    CLIENT_SECRET: str
    REDIRECT_URI: str
    AUTH_CODE: str
    VEHICLE_ID: str
    DEVICE_IMEI: str
    ENABLE_GEOCODING: bool = False
    GOOGLE_MAPS_API: str
    DEBUG: bool = False
    ANTHROPIC_API_KEY: str
    OPENAI_API_KEY: str
    USERNAME: str
    PASSWORD: str
    SECRET_KEY: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
