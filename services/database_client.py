"""
Database client for interacting with BigQuery.
"""
import json
import logging
import hashlib
import tempfile
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
from google.oauth2 import service_account

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseClient:
    """Client for database operations with BigQuery."""

    def __init__(self, project_id: str, service_account_json: str):
        """
        Initialize database client with project and service account.

        Args:
            project_id: Google Cloud project ID
            service_account_json: Path to service account JSON key file
        """

        service_account_info = json.loads(service_account_json)
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        self.client = bigquery.Client(project=project_id, credentials=credentials)
        self.dataset = "appdatabase"

        # Initialize buffers for batch loading
        self.address_buffer: List[Dict] = []
        self.order_buffer: List[Dict] = []
        self.coordinates_buffer: List[Dict] = []
        self.address_inputs_buffer: List[Dict] = []
        self.buffer_size = 1000  # Adjust based on your needs

    def get_address_by_input(self, input_address: str) -> Optional[Dict]:
        """
        Get address ID from the address_inputs table by original input address.
        This allows finding the right address without making a Google API call.

        Args:
            input_address: The original input address string

        Returns:
            Dict with address information if found, None otherwise
        """
        query = f"""
        SELECT 
            ai.address_id,
            a.formatted_address,
            a.google_lat,
            a.google_lng
        FROM `{self.dataset}.address_inputs` ai
        JOIN `{self.dataset}.addresses` a ON ai.address_id = a.address_id
        WHERE ai.input_address = @input_address
        LIMIT 1
        """
        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("input_address", "STRING", input_address)
            ]
        )
        results = self.client.query(query, job_config=job_config).result()
        return next(results) if results.total_rows > 0 else None

    def get_address(self, address: str) -> Optional[Dict]:
        """
        Get address from database if it exists.
        Now accepts both regular and uppercase addresses, converts to uppercase for comparison.

        Args:
            address: The formatted address string

        Returns:
            Dict with address information if found, None otherwise
        """
        query = f"""
        SELECT 
            address_id,
            formatted_address,
            google_lat,
            google_lng
        FROM `{self.dataset}.addresses`
        WHERE formatted_address = @address
        LIMIT 1
        """
        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("address", "STRING", address.upper())
            ]
        )
        results = self.client.query(query, job_config=job_config).result()
        return next(results) if results.total_rows > 0 else None

    def process_address(self, input_address: str, validation_result: Optional[Dict] = None) -> Tuple[
        int, float, float, bool, Dict]:
        """
        Process address, checking first if the input address exists in address_inputs table.
        Only calls Google API if the input address is not found.

        Args:
            input_address: The original input address
            validation_result: Optional pre-validated result (if API call was already made)

        Returns:
            Tuple of (address_id, lat, lng, is_verified, original_coords)
        """
        # First check if this input address is already mapped to an address_id
        existing_input = self.get_address_by_input(input_address)

        if existing_input:
            # We found the input address, no need for Google API call
            address_id = existing_input['address_id']

            # Check for verified coordinates
            verified_coords = self.get_verified_coordinates(address_id)

            if verified_coords:
                original_coords = {
                    "lat": existing_input['google_lat'],
                    "lng": existing_input['google_lng']
                }
                return address_id, verified_coords['lat'], verified_coords['lon'], True, original_coords
            else:
                original_coords = {
                    "lat": existing_input['google_lat'],
                    "lng": existing_input['google_lng']
                }
                return address_id, existing_input['google_lat'], existing_input['google_lng'], False, original_coords

        # If we got here, we need to validate the address with Google
        # Use the validation_result if provided, otherwise caller must provide it
        if not validation_result:
            raise ValueError("Validation result must be provided if address is not found in database")

        formatted_address = validation_result["result"]["englishLatinAddress"]["formattedAddress"]

        # Check if address already exists in addresses table
        existing_address = self.get_address(formatted_address)

        if existing_address:
            address_id = existing_address['address_id']

            # Store the input_address -> address_id mapping for future use
            self.insert_address_input(input_address, address_id)

            # Check for verified coordinates
            verified_coords = self.get_verified_coordinates(address_id)

            if verified_coords:
                original_coords = {
                    "lat": existing_address['google_lat'],
                    "lng": existing_address['google_lng']
                }
                return address_id, verified_coords['lat'], verified_coords['lon'], True, original_coords
            else:
                original_coords = {
                    "lat": existing_address['google_lat'],
                    "lng": existing_address['google_lng']
                }
                return address_id, existing_address['google_lat'], existing_address[
                    'google_lng'], False, original_coords
        else:
            # Insert new address
            address_data = self._prepare_address_data(validation_result)
            address_id = self.insert_address(validation_result)

            # Store the input_address -> address_id mapping
            self.insert_address_input(input_address, address_id)

            lat = validation_result["result"]["geocode"]["location"]["latitude"]
            lng = validation_result["result"]["geocode"]["location"]["longitude"]
            original_coords = {"lat": lat, "lng": lng}
            return address_id, lat, lng, False, original_coords

    def insert_address_input(self, input_address: str, address_id: int) -> None:
        """
        Store the original input address and its mapping to a validated address_id.

        Args:
            input_address: The original unvalidated address string
            address_id: The validated address ID in the database
        """
        address_input_data = {
            "input_address": input_address,
            "address_id": address_id,
            "created_at": datetime.now().isoformat()
        }

        self.address_inputs_buffer.append(address_input_data)

        # If buffer is full, perform batch load
        if len(self.address_inputs_buffer) >= self.buffer_size:
            self._batch_load_address_inputs()

    def _batch_load_address_inputs(self) -> None:
        """Perform batch load of buffered address inputs."""
        if not self.address_inputs_buffer:
            return

        table_id = f"{self.dataset}.address_inputs"

        # Load data using load_table_from_json
        job_config = bigquery.LoadJobConfig(
            schema=self._get_address_inputs_schema()
        )

        try:
            job = self.client.load_table_from_json(
                self.address_inputs_buffer,
                table_id,
                job_config=job_config
            )
            job.result()  # Wait for the job to complete

            logger.info(f"Successfully loaded {len(self.address_inputs_buffer)} address inputs")
            self.address_inputs_buffer.clear()

        except Exception as e:
            logger.error(f"Error in batch loading address inputs: {str(e)}")
            raise

    def _get_address_inputs_schema(self) -> List[bigquery.SchemaField]:
        """Define the schema for the address_inputs table."""
        return [
            bigquery.SchemaField("input_address", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("address_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED")
        ]

    def insert_task(self, task_data: Dict, address_id: int, device_number: str) -> None:
        """
        Buffer task data for batch insertion into orders table.

        Args:
            task_data: Dictionary containing task information
            address_id: The ID of the associated address
            device_number: The device number associated with the task
        """
        # Handle date conversion from YYYYMMDD to ISO format
        date_value = None
        if task_data.get("date"):
            try:
                # Convert YYYYMMDD string to datetime.date object
                if isinstance(task_data["date"], str):
                    date_value = datetime.strptime(task_data["date"], "%Y%m%d").date().isoformat()
                else:
                    date_value = task_data["date"].isoformat()
            except ValueError as e:
                logger.error(f"Error parsing date: {str(e)}")
                raise ValueError(f"Invalid date format. Expected YYYYMMDD, got: {task_data['date']}")

        # Convert camelCase to snake_case for database insertion
        order_data = {
            "task_id": task_data["task_id"],
            "address_id": address_id,
            "local_id": task_data["localId"],
            "device_number": device_number,
            "location_name": task_data.get("locationName"),
            "logist_comment": task_data.get("logistComment"),
            "action_tag": task_data.get("actionTag"),
            "action_tag_subtype": task_data.get("actionTagSubtype"),
            "parcel_weight": task_data.get("parcelWeight"),
            "date": date_value,
            "time_comment": task_data.get("timeComment"),
            "refuel_volume": task_data.get("refuelVolume"),
            "refuel_full_tank": task_data.get("refuelFullTank"),
            "adblue_volume": task_data.get("adblueVolume"),
            "adblue_full_tank": task_data.get("adblueFullTank"),
            "temperature_info": task_data.get("temperatureInfo"),
            "driver_atch_tags": task_data.get("driverAtchTags"),
            "driver_atch_tags_visit_disabled": task_data.get("driverAtchTagsVisitDisabled"),
            "created_at": datetime.now().isoformat()
        }

        self.order_buffer.append(order_data)

        # If buffer is full, perform batch load
        if len(self.order_buffer) >= self.buffer_size:
            self._batch_load_orders()

    def insert_address(self, validation_result: Dict) -> int:
        """
        Buffer address data for batch insertion.

        Args:
            validation_result: The Google API validation result

        Returns:
            The generated address ID
        """
        address_data = self._prepare_address_data(validation_result)
        self.address_buffer.append(address_data)

        # If buffer is full, perform batch load
        if len(self.address_buffer) >= self.buffer_size:
            self._batch_load_addresses()

        return address_data["address_id"]

    def _batch_load_addresses(self) -> None:
        """Perform batch load of buffered addresses."""
        if not self.address_buffer:
            return

        table_id = f"{self.dataset}.addresses"

        # Load data directly using load_table_from_json
        job_config = bigquery.LoadJobConfig(
            schema=self._get_address_schema()
        )

        try:
            job = self.client.load_table_from_json(
                self.address_buffer,
                table_id,
                job_config=job_config
            )
            job.result()  # Wait for the job to complete

            logger.info(f"Successfully loaded {len(self.address_buffer)} addresses")
            self.address_buffer.clear()

        except Exception as e:
            logger.error(f"Error in batch loading addresses: {str(e)}")
            raise

    def _batch_load_orders(self) -> None:
        """Perform batch load of buffered orders."""
        if not self.order_buffer:
            return

        table_id = f"{self.dataset}.orders"

        # Create a temporary file to store the data
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as temp_file:
            for order in self.order_buffer:
                json.dump(order, temp_file)
                temp_file.write('\n')
            temp_file_path = temp_file.name

        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=self._get_order_schema()
        )

        try:
            with open(temp_file_path, "rb") as source_file:
                load_job = self.client.load_table_from_file(
                    source_file,
                    table_id,
                    job_config=job_config
                )
                load_job.result()  # Wait for the job to complete

            logger.info(f"Successfully loaded {len(self.order_buffer)} orders")
            self.order_buffer.clear()

        except Exception as e:
            logger.error(f"Error in batch loading orders: {str(e)}")
            raise Exception("Failed to batch load orders") from e

    @staticmethod
    def _prepare_address_data(validation_result: Dict) -> Dict:
        """
        Prepare address data for insertion with uppercase addresses.

        Args:
            validation_result: The Google API validation result

        Returns:
            Dict with formatted address data for database insertion
        """
        result = validation_result["result"]

        # Create a mapping of component types to confirmation levels from original address
        confirmation_levels = {
            comp["componentType"]: comp.get("confirmationLevel")
            for comp in result["address"]["addressComponents"]
        }

        # Create address components using English text but original confirmation levels
        # Convert all address component values to uppercase
        address_components = {
            comp["componentType"]: {
                "value": comp["componentName"]["text"].upper(),
                "confirmation_level": confirmation_levels.get(comp["componentType"])
            }
            for comp in result["englishLatinAddress"]["addressComponents"]
        }

        # Convert formatted address to uppercase before generating hash
        # Also clean the address by removing newlines and extra whitespace
        formatted_address = result["englishLatinAddress"]["formattedAddress"]
        formatted_address = " ".join(formatted_address.replace("\n", " ").split()).upper()

        # Generate a consistent hash for the address ID using SHA-256
        # Use uppercase formatted address for hash generation to maintain consistency
        address_hash = hashlib.sha256(formatted_address.encode('utf-8')).hexdigest()
        # Convert first 16 characters of hex to integer (gives us a 64-bit number)
        address_id = int(address_hash[:15], 16)

        return {
            "address_id": address_id,
            "formatted_address": formatted_address,  # Now in uppercase and cleaned
            "street": address_components.get("route", {}).get("value"),  # Already uppercase
            "street_confirmation": address_components.get("route", {}).get("confirmation_level"),
            "number": address_components.get("street_number", {}).get("value"),  # Already uppercase
            "number_confirmation": address_components.get("street_number", {}).get("confirmation_level"),
            "city": address_components.get("locality", {}).get("value"),  # Already uppercase
            "city_confirmation": address_components.get("locality", {}).get("confirmation_level"),
            "postal_code": address_components.get("postal_code", {}).get("value"),  # Already uppercase
            "postal_code_confirmation": address_components.get("postal_code", {}).get("confirmation_level"),
            "country": address_components.get("country", {}).get("value"),  # Already uppercase
            "country_confirmation": address_components.get("country", {}).get("confirmation_level"),
            "google_lat": result["geocode"]["location"]["latitude"],
            "google_lng": result["geocode"]["location"]["longitude"],
            "created_at": datetime.now().isoformat()
        }

    def _get_address_schema(self) -> List[bigquery.SchemaField]:
        """Define the schema for the addresses table."""
        return [
            bigquery.SchemaField("address_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("formatted_address", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("street", "STRING"),
            bigquery.SchemaField("street_confirmation", "STRING"),
            bigquery.SchemaField("number", "STRING"),
            bigquery.SchemaField("number_confirmation", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("city_confirmation", "STRING"),
            bigquery.SchemaField("postal_code", "STRING"),
            bigquery.SchemaField("postal_code_confirmation", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("country_confirmation", "STRING"),
            bigquery.SchemaField("google_lat", "FLOAT"),
            bigquery.SchemaField("google_lng", "FLOAT"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("updated_at", "TIMESTAMP")
        ]

    def get_verified_coordinates(self, address_id: int) -> Optional[Dict]:
        """
        Get latest verified coordinates for an address if they exist.
        Uses ORDER BY created_at DESC to get the most recent entry without requiring updates.

        Args:
            address_id: The address ID to look up

        Returns:
            Dict with lat, lon if found, None otherwise
        """
        query = f"""
        SELECT lat, lon, created_at
        FROM `{self.dataset}.verified_coordinates`
        WHERE address_id = @address_id
        ORDER BY created_at DESC
        LIMIT 1
        """

        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("address_id", "INTEGER", address_id)
            ]
        )

        results = self.client.query(query, job_config=job_config).result()
        row = next(iter(results), None)

        if row:
            return {
                "lat": row.lat,
                "lon": row.lon,
                "created_at": row.created_at
            }
        return None

    def get_address_with_coordinates(self, address_id: int) -> Optional[Dict]:
        """
        Get address details with the best available coordinates.
        First tries to get verified coordinates, then falls back to Google coordinates.

        Args:
            address_id: The address ID to look up

        Returns:
            Dict with complete address details and coordinates
        """
        # First get the base address information
        query = f"""
        SELECT 
            address_id,
            formatted_address,
            street,
            number,
            city,
            postal_code,
            country,
            google_lat,
            google_lng
        FROM `{self.dataset}.addresses`
        WHERE address_id = @address_id
        """

        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("address_id", "INTEGER", address_id)
            ]
        )

        results = self.client.query(query, job_config=job_config).result()
        address_row = next(iter(results), None)

        if not address_row:
            return None

        # Convert to dictionary
        address_info = dict(address_row.items())

        # Check if we have verified coordinates
        verified_coords = self.get_verified_coordinates(address_id)

        if verified_coords:
            # Use verified coordinates if available
            address_info["lat"] = verified_coords["lat"]
            address_info["lng"] = verified_coords["lon"]
            address_info["coordinates_source"] = "verified"
        else:
            # Fall back to Google coordinates
            address_info["lat"] = address_info["google_lat"]
            address_info["lng"] = address_info["google_lng"]
            address_info["coordinates_source"] = "google"

        return address_info

    def _get_order_schema(self) -> List[bigquery.SchemaField]:
        """Define the schema for the orders table."""
        return [
            # Primary and foreign keys
            bigquery.SchemaField("task_id", "STRING", mode="REQUIRED"),  # PK
            bigquery.SchemaField("address_id", "INTEGER", mode="REQUIRED"),  # FK

            # Required fields
            bigquery.SchemaField("local_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("device_number", "STRING", mode="REQUIRED"),

            # Nullable fields
            bigquery.SchemaField("location_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("logist_comment", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("action_tag", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("action_tag_subtype", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("parcel_weight", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("time_comment", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("refuel_volume", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("refuel_full_tank", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("adblue_volume", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("adblue_full_tank", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("temperature_info", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("driver_atch_tags", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("driver_atch_tags_visit_disabled", "BOOLEAN", mode="NULLABLE"),

            # Metadata
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED")
        ]

    def insert_verified_coordinates(self, address_id: int, lat: float, lng: float) -> bool:
        """
        Buffer verified coordinates for batch insertion.

        Args:
            address_id: The address ID to associate with these coordinates
            lat: The latitude value
            lng: The longitude value

        Returns:
            True if coordinates are different from existing coordinates, False otherwise
        """
        # First check if we have existing verified coordinates
        existing_coords = self.get_verified_coordinates(address_id)

        if existing_coords:
            # If verified coordinates exist and are the same, don't buffer new record
            if abs(existing_coords["lat"] - lat) < 1e-7 and abs(existing_coords["lon"] - lng) < 1e-7:
                return False
        else:
            # If no verified coordinates exist, check against Google coordinates
            # Get Google coordinates from addresses table
            query = f"""
            SELECT google_lat, google_lng
            FROM `{self.dataset}.addresses`
            WHERE address_id = @address_id
            """

            job_config = QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("address_id", "INTEGER", address_id)
                ]
            )

            results = self.client.query(query, job_config=job_config).result()
            google_coords = next(iter(results), None)

            # If Google coordinates match the new coordinates (within small epsilon),
            # don't consider this an update
            if google_coords and abs(google_coords.google_lat - lat) < 1e-7 and abs(
                    google_coords.google_lng - lng) < 1e-7:
                return False

        # Add to coordinates buffer
        coordinate_data = {
            "address_id": address_id,
            "lat": lat,
            "lon": lng,
            "created_at": datetime.now().isoformat()
        }

        self.coordinates_buffer.append(coordinate_data)

        # If buffer is full, perform batch load
        if len(self.coordinates_buffer) >= self.buffer_size:
            self._batch_load_coordinates()

        return True

    def _batch_load_coordinates(self) -> None:
        """Perform batch load of buffered coordinates."""
        if not self.coordinates_buffer:
            return

        table_id = f"{self.dataset}.verified_coordinates"

        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("address_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("lat", "FLOAT", mode="REQUIRED"),
                bigquery.SchemaField("lon", "FLOAT", mode="REQUIRED"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED")
            ]
        )

        try:
            # Load data directly using load_table_from_json
            job = self.client.load_table_from_json(
                self.coordinates_buffer,
                table_id,
                job_config=job_config
            )
            job.result()  # Wait for the job to complete

            logger.info(f"Successfully loaded {len(self.coordinates_buffer)} coordinates")
            self.coordinates_buffer.clear()

        except Exception as e:
            logger.error(f"Error in batch loading coordinates: {str(e)}")
            raise

    def flush_buffers(self) -> None:
        """Force flush all remaining data in buffers."""
        if self.address_buffer:
            self._batch_load_addresses()
        if self.order_buffer:
            self._batch_load_orders()
        if self.coordinates_buffer:
            self._batch_load_coordinates()
        if self.address_inputs_buffer:
            self._batch_load_address_inputs()

    def get_unverified_addresses(self) -> List[Dict[str, Any]]:
        """
        Get tasks and their associated address IDs for addresses without verified coordinates.

        Returns:
            List of dictionaries containing task_id, address_id, and formatted_address
        """
        query = f"""
        SELECT DISTINCT t.task_id, t.address_id, a.formatted_address
        FROM `{self.dataset}.orders` t
        JOIN `{self.dataset}.addresses` a ON t.address_id = a.address_id
        LEFT JOIN `{self.dataset}.verified_coordinates` vc ON t.address_id = vc.address_id
        WHERE vc.address_id IS NULL
        """

        query_job = self.client.query(query)
        results = query_job.result()

        return [{"task_id": row.task_id, "address_id": row.address_id, "formatted_address": row.formatted_address} for row
                in results]