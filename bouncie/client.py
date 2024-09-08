import logging
import os
from bounciepy import AsyncRESTAPIClient

logger = logging.getLogger(__name__)


class BouncieClient:
    def __init__(self):
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")
        self.redirect_uri = os.getenv("REDIRECT_URI")
        self.auth_code = os.getenv("AUTH_CODE")
        self.device_imei = os.getenv("DEVICE_IMEI")
        self.vehicle_id = os.getenv("VEHICLE_ID")

        if not all(
            [
                self.client_id,
                self.client_secret,
                self.redirect_uri,
                self.auth_code,
                self.vehicle_id,
                self.device_imei,
            ]
        ):
            raise ValueError("Missing required environment variables for BouncieAPI")

        self.client = AsyncRESTAPIClient(
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_url=self.redirect_uri,
            auth_code=self.auth_code,
        )

    async def get_access_token(self):
        try:
            success = await self.client.get_access_token()
            if not success:
                logger.error("Failed to obtain access token.")
                return False
            return True
        except Exception as e:
            logger.error(f"Error getting access token: {e}")
            return False

    async def get_vehicle_by_imei(self):
        return await self.client.get_vehicle_by_imei(imei=self.device_imei)
