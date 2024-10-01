import logging
import aiohttp
import time

logger = logging.getLogger(__name__)

class BouncieClient:
    def __init__(
            self,
            client_id,
            client_secret,
            redirect_uri,
            auth_code,
            device_imei,
            vehicle_id):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.auth_code = auth_code
        self.device_imei = device_imei
        self.vehicle_id = vehicle_id
        self.access_token = None

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
            raise ValueError(
                "Missing required environment variables for BouncieAPI")

    async def get_access_token(self):
        current_time = time.time()

        # Check if token exists and has not expired
        if self.access_token and current_time < self.token_expiry:
            return self.access_token

        # Fetch a new token
        auth_url = "https://auth.bouncie.com/oauth/token"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "authorization_code",
            "code": self.auth_code,
            "redirect_uri": self.redirect_uri
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(auth_url, data=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.access_token = data.get('access_token')
                        expires_in = data.get('expires_in', 3600)  # Default to 1 hour
                        self.token_expiry = current_time + expires_in
                        return self.access_token
                    else:
                        logger.error(f"Failed to obtain access token. Status: {response.status}")
                        return None
        except Exception as e:
            logger.error(f"Error getting access token: {e}")
            return None

    async def get_vehicle_by_imei(self):
        access_token = await self.get_access_token()
        if not access_token:
            return None

        url = f"https://api.bouncie.dev/v1/vehicles?imei={self.device_imei}"
        headers = {
            "Authorization": access_token,
            "Content-Type": "application/json"
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data[0] if data else None
                    else:
                        logger.error(f"Failed to get vehicle data. Status: {response.status}")
                        return None
        except Exception as e:
            logger.error(f"Error getting vehicle data: {e}")
            return None
