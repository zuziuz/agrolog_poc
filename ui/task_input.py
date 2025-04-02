"""
UI components for task input forms.
"""
import logging
import pandas as pd
import streamlit as st
from typing import Dict, List, Optional
from datetime import date

# Using relative imports that work when running streamlit directly
from models.task import TaskLocation, Route, RouteTask, ProcessingResult, TaskType
from services.address_validator import AddressValidator
from services.database_client import DatabaseClient
from services.task_processor import TaskProcessor
from utils.helpers import clean_address

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TaskInputUI:
    """UI components for task input."""

    def __init__(self,
                 address_validator: AddressValidator,
                 db_client: DatabaseClient,
                 task_processor: TaskProcessor):
        """
        Initialize task input UI with required services.

        Args:
            address_validator: Address validation service
            db_client: Database client service
            task_processor: Task processor service
        """
        self.address_validator = address_validator
        self.db_client = db_client
        self.task_processor = task_processor

        # Define common constants
        self.ACTION_TAG_OPTIONS = [
            "",  # Empty option for None
            "PARCEL_LOAD",
            "PARCEL_UNLOAD",
            "REST",
            "REFUEL",
            "CUSTOMS",
            "SERVICE",
            "DRIVER_CHANGE",
            "FERRY",
            "TRUCK_SWAP",
            "TRAILER_SWAP",
            "DOCUMENTS",
            "TRAIN",
            "CARWASH",
            "CONTAINER_SWAP",
            "TUNNEL",
            "TECHNICAL_INSPECTION",
            "TOLL_POINT",
            "BRIDGE",
            "PALLETS",
            "WEIGHING",
            "TRAIN_TRANSIT",
            "DOCUMENTS_POST"
        ]

    def create_task_form(self, key_prefix: str = "", prefill: Optional[TaskLocation] = None) -> Optional[TaskLocation]:
        """
        Create a form for entering task details.

        Args:
            key_prefix: Prefix for Streamlit widget keys to prevent duplicates
            prefill: Optional TaskLocation to prefill the form

        Returns:
            TaskLocation if form is submitted, None otherwise
        """
        col1, col2 = st.columns(2)

        with col1:
            local_id = st.text_input(
                "Local ID*",
                key=f"{key_prefix}_local_id",
                value=prefill.local_id if prefill else ""
            )

            location_name = st.text_input(
                "Location Name",
                key=f"{key_prefix}_location_name",
                value=prefill.location_name if prefill and prefill.location_name else ""
            )

            location_address = st.text_input(
                "Location Address*",
                key=f"{key_prefix}_location_address",
                value=prefill.location_address if prefill else ""
            )

            task_date = st.date_input(
                "Date",
                key=f"{key_prefix}_date",
                value=prefill.task_date if prefill and prefill.task_date else date.today()
            )

            time_comment = st.text_input(
                "Time Comment",
                key=f"{key_prefix}_time_comment",
                value=prefill.time_comment if prefill and prefill.time_comment else ""
            )

            logist_comment = st.text_input(
                "Logist Comment",
                key=f"{key_prefix}_logist_comment",
                value=prefill.logist_comment if prefill and prefill.logist_comment else ""
            )

        with col2:
            action_tag = st.selectbox(
                "Action Tag",
                options=self.ACTION_TAG_OPTIONS,
                key=f"{key_prefix}_action_tag",
                index=self.ACTION_TAG_OPTIONS.index(prefill.action_tag) if prefill and prefill.action_tag and prefill.action_tag in self.ACTION_TAG_OPTIONS else 0
            )

            # Handle numeric inputs with empty option
            has_parcel_weight = st.checkbox(
                "Include Parcel Weight",
                key=f"{key_prefix}_has_parcel_weight",
                value=prefill.parcel_weight is not None if prefill else False
            )

            parcel_weight = st.number_input(
                "Parcel Weight",
                value=prefill.parcel_weight if prefill and prefill.parcel_weight is not None else 0.0,
                key=f"{key_prefix}_parcel_weight",
                disabled=not has_parcel_weight
            ) if has_parcel_weight else None

            has_refuel_volume = st.checkbox(
                "Include Refuel Volume",
                key=f"{key_prefix}_has_refuel_volume",
                value=prefill.refuel_volume is not None if prefill else False
            )

            refuel_volume = st.number_input(
                "Refuel Volume",
                value=prefill.refuel_volume if prefill and prefill.refuel_volume is not None else 0.0,
                key=f"{key_prefix}_refuel_volume",
                disabled=not has_refuel_volume
            ) if has_refuel_volume else None

            refuel_full_tank = st.checkbox(
                "Refuel Full Tank",
                key=f"{key_prefix}_refuel_full_tank",
                value=prefill.refuel_full_tank if prefill else False
            )

            has_adblue_volume = st.checkbox(
                "Include Adblue Volume",
                key=f"{key_prefix}_has_adblue_volume",
                value=prefill.adblue_volume is not None if prefill else False
            )

            adblue_volume = st.number_input(
                "Adblue Volume",
                value=prefill.adblue_volume if prefill and prefill.adblue_volume is not None else 0.0,
                key=f"{key_prefix}_adblue_volume",
                disabled=not has_adblue_volume
            ) if has_adblue_volume else None

            adblue_full_tank = st.checkbox(
                "Adblue Full Tank",
                key=f"{key_prefix}_adblue_full_tank",
                value=prefill.adblue_full_tank if prefill else False
            )

            temperature_info = st.text_input(
                "Temperature Info",
                key=f"{key_prefix}_temperature_info",
                value=prefill.temperature_info if prefill and prefill.temperature_info else ""
            )

        # Build task location from form data
        if local_id and location_address:
            # Determine task type - if there's a prefill, keep its task type
            task_type = prefill.task_type if prefill and hasattr(prefill, 'task_type') else TaskType.PICKUP

            # Clean the address by removing newlines and extra spaces
            clean_location_address = clean_address(location_address)

            return TaskLocation(
                local_id=local_id,
                location_name=location_name if location_name else None,
                location_address=clean_location_address,
                logist_comment=logist_comment if logist_comment else None,
                action_tag=action_tag if action_tag else None,
                action_tag_subtype=None,  # Not exposed in UI
                parcel_weight=parcel_weight,
                task_date=task_date,
                time_comment=time_comment if time_comment else None,
                refuel_volume=refuel_volume,
                refuel_full_tank=refuel_full_tank if refuel_full_tank else None,
                adblue_volume=adblue_volume,
                adblue_full_tank=adblue_full_tank if adblue_full_tank else None,
                temperature_info=temperature_info if temperature_info else None,
                driver_atch_tags=None,  # Not exposed in UI
                driver_atch_tags_visit_disabled=None,  # Not exposed in UI
                task_type=task_type
            )

        return None

    def create_from_to_form(self) -> Optional[Route]:
        """
        Create form for route input with From and To locations.

        Returns:
            Route object if the form is submitted and valid, None otherwise
        """
        st.subheader("Route Input")

        # Device number at the top
        device_number = st.text_input("Select Truck (Device Number)*", key="device_number")

        # Create From section
        st.subheader("From")
        from_task = self.create_task_form(key_prefix="from")
        if from_task:
            from_task.task_type = TaskType.PICKUP

        # Create To section
        st.subheader("To")
        to_task = self.create_task_form(key_prefix="to")
        if to_task:
            to_task.task_type = TaskType.DELIVERY

        if st.button("Process Route"):
            if not device_number:
                st.error("Please enter a Device Number")
                return None

            if not from_task:
                st.error("Please fill in all required From fields marked with *")
                return None

            if not to_task:
                st.error("Please fill in all required To fields marked with *")
                return None

            # Create a Route object with individual tasks
            route = Route(
                tasks=[
                    RouteTask(task=from_task, device_number=device_number),
                    RouteTask(task=to_task, device_number=device_number)
                ],
                device_number=device_number
            )
            return route

        return None

    def process_route(self, route: Route) -> List[ProcessingResult]:
        """
        Process a route by validating addresses and sending to API.

        Args:
            route: Route object with tasks

        Returns:
            List of ProcessingResult objects
        """
        results = []

        with st.spinner("Processing route..."):
            try:
                device_number = route.device_number

                for task in route.get_locations():
                    result = self.process_single_task(task, device_number)
                    result.task_type = task.task_type  # Store the task type in result
                    results.append(result)

                # Force flush all buffers
                self.db_client.flush_buffers()

                # Check if verified coordinates were used for any tasks
                if not all(result.is_verified for result in results):
                    st.warning("⚠️ No verified coordinates available for one or more addresses. Please review coordinates on loctracker.")

                # Show success with task IDs
                task_ids = ", ".join(result.task_id for result in results)
                st.success(f"✅ Route processed successfully! Task IDs: {task_ids}")

                return results

            except Exception as e:
                st.error(f"Error processing route: {str(e)}")
                import traceback
                st.code(traceback.format_exc(), language="python")
                return []

    def process_single_task(self, task: TaskLocation, device_number: str) -> ProcessingResult:
        """
        Process a single task by validating address and sending to API.

        Args:
            task: TaskLocation to process
            device_number: Device number to associate with the task

        Returns:
            ProcessingResult with processing details
        """
        # Store original input address
        input_address = task.location_address

        with st.spinner(f"Processing task {task.local_id}..."):
            # Try to find the address in our database first
            existing_input = self.db_client.get_address_by_input(input_address)

            if existing_input:
                # We found the input address, no need for Google API call
                st.success(f"📍 Found '{input_address}' in database")

                address_id = existing_input['address_id']

                # Check for verified coordinates
                verified_coords = self.db_client.get_verified_coordinates(address_id)

                if verified_coords:
                    task.lat = verified_coords['lat']
                    task.lng = verified_coords['lon']
                    is_verified = True
                    original_coords = {
                        "lat": existing_input['google_lat'],
                        "lng": existing_input['google_lng']
                    }
                    # Get formatted address for display
                    formatted_address = existing_input['formatted_address']
                else:
                    task.lat = existing_input['google_lat']
                    task.lng = existing_input['google_lng']
                    is_verified = False
                    original_coords = {
                        "lat": task.lat,
                        "lng": task.lng
                    }
                    # Get formatted address for display
                    formatted_address = existing_input['formatted_address']
            else:
                # Not found, need to validate with Google API
                st.info("Address not found in database, validating with Google API...")
                validation_result = self.address_validator.validate_address(input_address)

                # Process address and get coordinates
                address_id, lat, lng, is_verified, original_coords = self.db_client.process_address(
                    input_address, validation_result)

                # Update task with coordinates
                task.lat = lat
                task.lng = lng

                # Get formatted address from validation result
                formatted_address = validation_result["result"]["englishLatinAddress"]["formattedAddress"]
                st.success(f"Address validated: {formatted_address}")

            # Send task to API
            api_dict = task.to_api_dict()
            api_response = self.task_processor.send_tasks([api_dict], device_number)

            # Extract task_id from response
            task_id = str(api_response.get("taskId", ""))

            # Update task with task_id and address_id
            task.task_id = task_id
            task.address_id = address_id

            # Insert task into database
            api_dict["task_id"] = task_id
            self.db_client.insert_task(api_dict, address_id, device_number)

            # Create and return processing result
            return ProcessingResult(
                task_id=task_id,
                address=formatted_address,
                is_verified=is_verified,
                original_lat=original_coords["lat"],
                original_lng=original_coords["lng"],
                verified_lat=task.lat if is_verified else None,
                verified_lng=task.lng if is_verified else None
            )

    def process_uploaded_csv(self, uploaded_file) -> List[ProcessingResult]:
        """
        Process uploaded CSV file with task data.

        Args:
            uploaded_file: Streamlit uploaded file object

        Returns:
            List of ProcessingResult objects
        """
        results = []

        try:
            # Read CSV with proper error handling
            tasks_df = pd.read_csv(uploaded_file)

            # Validate required columns
            required_columns = ['localId', 'deviceNumber', 'locationAddress']
            missing_columns = [col for col in required_columns if col not in tasks_df.columns]

            if missing_columns:
                st.error(f"Missing required columns: {', '.join(missing_columns)}")
                return []

            # Remove any rows with missing required values
            tasks_df = tasks_df.dropna(subset=required_columns)

            if tasks_df.empty:
                st.error("No valid tasks found in CSV after removing rows with missing required values")
                return []

            st.info(f"Processing {len(tasks_df)} tasks...")

            # Process the tasks
            results = self._process_csv_tasks(tasks_df)

            return results

        except Exception as e:
            st.error(f"Error processing CSV file: {str(e)}")
            return []

    def _process_csv_tasks(self, tasks_df: pd.DataFrame) -> List[ProcessingResult]:
        """
        Process tasks from a DataFrame.

        Args:
            tasks_df: DataFrame with task data

        Returns:
            List of ProcessingResult objects
        """
        results = []

        # Counter for API calls saved
        api_calls_saved = 0
        # Container to display addresses reused from database
        address_reuse_container = st.expander("Addresses Reused From Database", expanded=True)
        reused_addresses = []

        # Group tasks by device number for batch processing
        device_groups = tasks_df.groupby('deviceNumber')
        total_groups = len(device_groups)

        progress_bar = st.progress(0)
        status_text = st.empty()

        for idx, (device_number, device_tasks) in enumerate(device_groups):
            try:
                status_text.text(f"Processing device {device_number} ({idx + 1}/{total_groups})")
                processed_tasks = []
                address_info = []  # Store address information for database insertion

                for _, row in device_tasks.iterrows():
                    try:
                        # Convert row to clean task dict
                        clean_task = self._prepare_task_dict(row)

                        # Store the original input address for processing
                        input_address = clean_task["locationAddress"]

                        # Try to find address in our database first
                        existing_input = self.db_client.get_address_by_input(input_address)

                        if existing_input:
                            # We found the input address, no need for Google API call
                            api_calls_saved += 1
                            # Add to reused addresses list for display
                            reused_addresses.append({
                                "Original Input": input_address,
                                "Matched Address": existing_input['formatted_address'],
                                "Task ID": clean_task["localId"]
                            })

                            address_id = existing_input['address_id']

                            # Check for verified coordinates
                            verified_coords = self.db_client.get_verified_coordinates(address_id)

                            if verified_coords:
                                lat = verified_coords['lat']
                                lng = verified_coords['lon']
                                is_verified = True
                                original_coords = {
                                    "lat": existing_input['google_lat'],
                                    "lng": existing_input['google_lng']
                                }
                                formatted_address = existing_input['formatted_address']
                            else:
                                lat = existing_input['google_lat']
                                lng = existing_input['google_lng']
                                is_verified = False
                                original_coords = {
                                    "lat": lat,
                                    "lng": lng
                                }
                                formatted_address = existing_input['formatted_address']
                        else:
                            # Not found, need to validate with Google API
                            validation_result = self.address_validator.validate_address(input_address)

                            # Process address and get coordinates
                            address_id, lat, lng, is_verified, original_coords = self.db_client.process_address(
                                input_address, validation_result)

                            formatted_address = validation_result["result"]["englishLatinAddress"]["formattedAddress"]

                        # Update task with coordinates
                        clean_task["lat"] = lat
                        clean_task["lng"] = lng

                        # Store task and address info for later database insertion
                        processed_tasks.append(clean_task)
                        address_info.append({
                            "address_id": address_id,
                            "local_id": clean_task["localId"],
                            "is_verified": is_verified,
                            "original_coords": original_coords,
                            "formatted_address": formatted_address
                        })

                    except Exception as e:
                        st.warning(f"Error processing task {row.get('localId', 'unknown')}: {str(e)}")
                        continue

                # Send batch to API if we have processed tasks
                if processed_tasks:
                    try:
                        # Send tasks to API
                        bulk_response = self.task_processor.send_tasks(processed_tasks, str(device_number))

                        if bulk_response.get("status") == 200 and "taskIds" in bulk_response:
                            task_ids = bulk_response["taskIds"]

                            # Insert tasks into database with received task IDs
                            for task_id, task, addr_info in zip(task_ids, processed_tasks, address_info):
                                try:
                                    # Add task_id to the task data
                                    task["task_id"] = str(task_id)  # Convert to string as per schema

                                    # Insert into database
                                    self.db_client.insert_task(
                                        task_data=task,
                                        address_id=addr_info["address_id"],
                                        device_number=str(device_number)
                                    )

                                    # Create result object
                                    result = ProcessingResult(
                                        task_id=task_id,
                                        address=addr_info["formatted_address"],
                                        is_verified=addr_info["is_verified"],
                                        original_lat=addr_info["original_coords"]["lat"],
                                        original_lng=addr_info["original_coords"]["lng"],
                                        verified_lat=task["lat"] if addr_info["is_verified"] else None,
                                        verified_lng=task["lng"] if addr_info["is_verified"] else None
                                    )
                                    results.append(result)

                                    # If verified coordinates were used, show notification
                                    if addr_info["is_verified"]:
                                        st.info(
                                            f"Used verified coordinates for task {addr_info['local_id']}. " +
                                            f"Original: ({addr_info['original_coords']['lat']:.6f}, {addr_info['original_coords']['lng']:.6f}), " +
                                            f"Verified: ({task['lat']:.6f}, {task['lng']:.6f})"
                                        )

                                except Exception as db_error:
                                    st.warning(
                                        f"Error inserting task {addr_info['local_id']} into database: {str(db_error)}"
                                    )
                                    continue
                        else:
                            st.error(f"API Error: {bulk_response.get('message', 'Unknown error')}")
                            continue

                    except Exception as api_error:
                        st.error(f"API Error for device {device_number}: {str(api_error)}")
                        continue

                # Update progress
                progress_bar.progress((idx + 1) / total_groups)

            except Exception as e:
                st.error(f"Error processing device {device_number}: {str(e)}")
                continue

        status_text.text("Processing complete!")
        progress_bar.progress(1.0)

        # Show addresses reused from database
        if reused_addresses:
            # Convert to DataFrame for better display
            reused_df = pd.DataFrame(reused_addresses)
            with address_reuse_container:
                st.write(f"**{len(reused_addresses)} addresses were reused from the database:**")
                st.dataframe(reused_df)

        # Show number of Google API calls saved
        if api_calls_saved > 0:
            st.success(f"Saved {api_calls_saved} Google API calls by using stored address mappings!")

        # Ensure all buffered data is written to database
        self.db_client.flush_buffers()

        return results

    def _show_verification_summary(self, results: List[ProcessingResult]):
        """
        Show summary of which tasks used verified coordinates.

        Args:
            results: List of ProcessingResult objects
        """
        if not results:
            st.warning("No results to display")
            return

        st.subheader("Coordinate Verification Summary")

        # Just use string data for everything to avoid conversion issues
        data = []
        for result in results:
            row = {
                "Type": "Pickup" if hasattr(result,
                                            'task_type') and result.task_type == TaskType.PICKUP else "Delivery",
                "Task ID": str(result.task_id),
                "Address": str(result.address),
                "Using Verified Coordinates": "✅" if result.is_verified else "❌",
                "Original Lat": str(result.original_lat),
                "Original Lng": str(result.original_lng),
            }

            # Add verified coordinates as strings, using "N/A" when not available
            if result.is_verified and result.verified_lat is not None:
                row["Verified Lat"] = str(result.verified_lat)
                row["Verified Lng"] = str(result.verified_lng)
            else:
                row["Verified Lat"] = "N/A"
                row["Verified Lng"] = "N/A"

            data.append(row)

        # Count verified vs unverified
        verified_count = sum(1 for r in results if r.is_verified)
        unverified_count = len(results) - verified_count

        # Show counts
        st.info(f"✅ Using verified coordinates: {verified_count} tasks")
        st.warning(f"❌ Using original coordinates: {unverified_count} tasks")

        # Create DataFrame from dictionaries
        df = pd.DataFrame(data)

        # Show detailed table - all values should already be strings
        st.dataframe(df)

    @staticmethod
    def _prepare_task_dict(row: pd.Series) -> Dict:
        """
        Convert a pandas Series (CSV row) to a clean task dictionary with proper type conversions.

        Args:
            row: A row from the tasks DataFrame

        Returns:
            Dict: Cleaned task dictionary with proper types and field names
        """
        # Define field mappings and type conversions
        FIELD_MAPPINGS = {
            # Required fields (string conversion)
            'localId': ('localId', str),
            'locationAddress': ('locationAddress', str),

            # Optional fields with their type conversions
            'locationName': ('locationName', str),
            'logistComment': ('logistComment', str),
            'actionTag': ('actionTag', str),
            'actionTagSubtype': ('actionTagSubtype', str),
            'parcelWeight': ('parcelWeight', float),
            'date': ('date', lambda x: str(int(x)) if pd.notna(x) else None),
            'timeComment': ('timeComment', str),
            'refuelVolume': ('refuelVolume', float),
            'refuelFullTank': ('refuelFullTank', bool),
            'adblueVolume': ('adblueVolume', float),
            'adblueFullTank': ('adblueFullTank', bool),
            'temperatureInfo': ('temperatureInfo', str),
            'driverAtchTags': ('driverAtchTags', str),
            'driverAtchTagsVisitDisabled': ('driverAtchTagsVisitDisabled', bool)
        }

        clean_task = {}

        # Process each field according to mappings
        for csv_field, (api_field, converter) in FIELD_MAPPINGS.items():
            if csv_field in row and pd.notna(row[csv_field]):
                try:
                    clean_task[api_field] = converter(row[csv_field])
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error converting field {csv_field}: {str(e)}")
                    # Skip this field if conversion fails
                    continue

        return clean_task