"""
Service for validating addresses using Google's Address Validation API.
"""
import logging
import requests
from typing import Dict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AddressValidator:
    """Service for validating addresses using Google's Address Validation API."""

    def __init__(self, api_key: str):
        """
        Initialize the address validator with the Google API key.

        Args:
            api_key: Google API key with Address Validation API enabled
        """
        self.api_key = api_key
        self.validation_url = "https://addressvalidation.googleapis.com/v1:validateAddress"

    def validate_address(self, address: str) -> Dict:
        """
        Validate address using Google Address Validation API.

        Args:
            address: The address string to validate

        Returns:
            Dict containing the validation response

        Raises:
            Exception: If the API call fails
        """
        try:
            headers = {"Content-Type": "application/json"}
            payload = {
                "address": {"addressLines": [address]},
                "languageOptions": {"returnEnglishLatinAddress": "true"}
            }
            response = requests.post(
                f"{self.validation_url}?key={self.api_key}",
                headers=headers,
                json=payload
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Address validation failed: {str(e)}")
            raise