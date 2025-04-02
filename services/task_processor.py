"""
Service for processing and sending tasks to the API.
"""
import json
import logging
from typing import Dict, List
from datetime import date
import requests

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TaskProcessor:
    """Service for processing tasks and communicating with the API."""

    def __init__(self, base_url: str, api_username: str, password: str):
        """
        Initialize task processor with API credentials.

        Args:
            base_url: Base URL for the API
            api_username: API username
            password: API password
        """
        self.base_url = base_url
        self.api_username = api_username
        self.password = password

    def send_tasks(self, tasks: List[Dict], device_number: str) -> Dict:
        """
        Send tasks to the API.

        Args:
            tasks: List of task dictionaries
            device_number: Device number to associate with the tasks

        Returns:
            API response
        """
        # Process each task before sending
        processed_tasks = [self._prepare_task_for_api(task) for task in tasks]
        if len(processed_tasks) == 1:
            return self._send_single_task(processed_tasks[0], device_number)
        return self._send_bulk_tasks(processed_tasks, device_number)

    def _send_single_task(self, task: Dict, device_number: str) -> Dict:
        """
        Send single task to the API.

        Args:
            task: Task dictionary
            device_number: Device number

        Returns:
            API response
        """
        url = f"{self.base_url}{self.api_username}/tasks/{device_number}/last"
        payload = {
            "password": self.password,
            "planFromDevice": True,
            "task": task
        }
        return self._make_api_request(url, payload)

    def _send_bulk_tasks(self, tasks: List[Dict], device_number: str) -> Dict:
        """
        Send multiple tasks to the API.

        Args:
            tasks: List of task dictionaries
            device_number: Device number

        Returns:
            API response
        """
        url = f"{self.base_url}{self.api_username}/tasks/{device_number}/last/bulk"
        payload = {
            "password": self.password,
            "planFromDevice": True,
            "tasks": tasks
        }
        # Debug logging - can be removed in production
        with open("bulk_payload_debug.json", "w") as outfile:
            json.dump(payload, outfile)
        return self._make_api_request(url, payload)

    @staticmethod
    def _make_api_request(url: str, payload: Dict) -> Dict:
        """
        Make API request.

        Args:
            url: API endpoint URL
            payload: Request payload

        Returns:
            API response

        Raises:
            Exception: If the API request fails
        """
        try:
            headers = {
                "Content-Type": "application/json;charset=UTF-8"
            }
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"API request failed: {str(e)}")
            raise

    def fetch_tasks_by_ids(self, task_ids: List[str]) -> Dict:
        """
        Fetch tasks from API by their IDs.

        Args:
            task_ids: List of task IDs

        Returns:
            API response with task data

        Raises:
            Exception: If the API request fails
        """
        try:
            if len(task_ids) == 1:
                task_ids_param = task_ids[0]
            else:
                task_ids_param = ";".join(task_ids)
            url = f"{self.base_url}{self.api_username}/tasks?password={self.password}&taskIds={task_ids_param}"
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching tasks by IDs: {str(e)}")
            raise

    @staticmethod
    def _prepare_task_for_api(task: Dict) -> Dict:
        """
        Prepare task data for API submission by handling date serialization
        and removing None values.

        Args:
            task: Original task dictionary

        Returns:
            Processed task ready for API submission
        """
        # Create a copy to avoid modifying the original
        processed_task = task.copy()

        # Convert date object to YYYYMMDD string format if present
        if 'date' in processed_task and isinstance(processed_task['date'], date):
            processed_task['date'] = processed_task['date'].strftime('%Y%m%d')

        # Remove None values as they appear in the sample JSON
        processed_task = {k: v for k, v in processed_task.items() if v is not None}

        return processed_task