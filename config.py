from pydantic_settings import BaseSettings

class Config(BaseSettings):
    AUTHORIZATION: str
    DEVICE_IMEI: str
    ENABLE_GEOCODING: bool = False
    GOOGLE_MAPS_API: str
    DEBUG: bool = False
    USERNAME: str
    PASSWORD: str
    SECRET_KEY: str
    FOURSQUARE_API_KEY: str
    PIN: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False