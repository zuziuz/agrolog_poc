"""
UI components for updating address coordinates.
"""
import logging
import pandas as pd
import streamlit as st
from typing import Dict, List

from services.database_client import DatabaseClient
from services.task_processor import TaskProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AddressUpdateUI:
    """UI components for updating address coordinates."""

    def __init__(self, db_client: DatabaseClient, task_processor: TaskProcessor):
        """
        Initialize address update UI with required services.

        Args:
            db_client: Database client service
            task_processor: Task processor service
        """
        self.db_client = db_client
        self.task_processor = task_processor

    def create_update_form(self):
        """Create form for updating addresses with coordinates."""
        st.subheader("Update Addresses")

        st.markdown("""
        ### Address Update Tool

        This tool fetches all addresses without verified coordinates from the database, 
        retrieves their current coordinates from the API, and updates the database with the latest coordinates.

        The updated list will show:
        - The full formatted address
        - Previous coordinates (if available)
        - New updated coordinates
        """)

        # Add containers for displaying progress and results
        progress_container = st.empty()
        results_container = st.empty()
        address_details_container = st.empty()

        if st.button("Update All Unverified Addresses"):
            self._process_address_updates(
                progress_container,
                results_container,
                address_details_container
            )

    def _process_address_updates(self, progress_container, results_container, address_details_container):
        """
        Process address updates by fetching tasks from API and updating coordinates.

        Args:
            progress_container: Streamlit container for progress messages
            results_container: Streamlit container for results summary
            address_details_container: Streamlit container for address details
        """
        try:
            # Get unverified tasks with their address IDs and formatted addresses
            progress_container.info("Fetching unverified addresses...")
            tasks = self.db_client.get_unverified_addresses()

            if not tasks:
                results_container.info("No addresses need verification!")
                return

            # Initialize counters and updated address info
            updated_count = 0
            unchanged_count = 0
            failed_count = 0
            updated_addresses = []

            # Process tasks in chunks
            chunks = self._chunk_tasks(tasks)
            total_chunks = len(chunks)

            progress_bar = st.progress(0)

            for chunk_idx, chunk in enumerate(chunks):
                try:
                    # Extract task IDs for this chunk
                    task_ids = [task["task_id"] for task in chunk]

                    # Create mapping of task_id to address info for this chunk
                    task_to_address = {task["task_id"]: {
                        "address_id": task["address_id"],
                        "formatted_address": task["formatted_address"]
                    } for task in chunk}

                    # Fetch tasks from API
                    response = self.task_processor.fetch_tasks_by_ids(task_ids)

                    if response.get("status") == 200 and "tasks" in response:
                        for task in response["tasks"]:
                            try:
                                task_id = str(task["taskId"])  # Convert to string to match database format
                                # Get the address info from our mapping
                                address_info = task_to_address.get(task_id)

                                if address_info:
                                    address_id = address_info["address_id"]
                                    formatted_address = address_info["formatted_address"]

                                    # Round coordinates for consistency
                                    task["lat"] = round(task["lat"], 7)
                                    task["lng"] = round(task["lng"], 7)

                                    # Get the address with all coordinates
                                    address_details = self.db_client.get_address_with_coordinates(address_id)

                                    # Show original Google coordinates if no verified coordinates exist yet
                                    if address_details:
                                        old_coords = {
                                            "lat": address_details["lat"],
                                            "lng": address_details["lng"],
                                            "source": address_details["coordinates_source"]
                                        }

                                    # Try to insert new coordinates
                                    was_inserted = self.db_client.insert_verified_coordinates(
                                        address_id,
                                        task["lat"],
                                        task["lng"]
                                    )

                                    if was_inserted:
                                        updated_count += 1
                                        # Store address details for display
                                        updated_addresses.append({
                                            "formatted_address": formatted_address,
                                            "old_coords": old_coords,
                                            "new_coords": {
                                                "lat": task["lat"],
                                                "lng": task["lng"]
                                            }
                                        })
                                    else:
                                        unchanged_count += 1
                                else:
                                    failed_count += 1
                                    logger.warning(f"Could not find address_id for task {task_id}")

                            except Exception as e:
                                failed_count += 1
                                logger.error(f"Error processing task {task.get('taskId')}: {str(e)}")

                    # Update progress
                    progress = (chunk_idx + 1) / total_chunks
                    progress_bar.progress(progress)
                    progress_container.info(f"Processing chunk {chunk_idx + 1}/{total_chunks}")

                except Exception as e:
                    st.error(f"Error processing chunk {chunk_idx + 1}: {str(e)}")
                    continue

            # Clear progress container
            progress_container.empty()

            # Display final results
            results = f"""
            📊 Update Complete:
            ✅ Updated: {updated_count} addresses
            ℹ️ Unchanged: {unchanged_count} addresses
            ❌ Failed: {failed_count} addresses
            """
            results_container.success(results)

            # Display detailed information about updated addresses
            if updated_addresses:
                st.subheader("Updated Addresses")

                # Create dataframe for better display
                address_data = []
                for addr in updated_addresses:
                    old_lat = "N/A"
                    old_lng = "N/A"
                    if addr["old_coords"]:
                        old_lat = addr["old_coords"]["lat"]
                        old_lng = addr["old_coords"]["lng"]

                    address_data.append({
                        "Address": addr["formatted_address"],
                        "Old Latitude": old_lat,
                        "Old Longitude": old_lng,
                        "New Latitude": addr["new_coords"]["lat"],
                        "New Longitude": addr["new_coords"]["lng"]
                    })

                # Display as a dataframe
                address_df = pd.DataFrame(address_data)
                st.dataframe(address_df)

        except Exception as e:
            st.error(f"Error updating addresses: {str(e)}")
            import traceback
            st.code(traceback.format_exc(), language="python")

    @staticmethod
    def _chunk_tasks(tasks: List[Dict], chunk_size: int = 50) -> List[List[Dict]]:
        """
        Split tasks into chunks to avoid too long URLs.

        Args:
            tasks: List of task dictionaries
            chunk_size: Maximum number of tasks per chunk

        Returns:
            List of task chunks
        """
        return [tasks[i:i + chunk_size] for i in range(0, len(tasks), chunk_size)]