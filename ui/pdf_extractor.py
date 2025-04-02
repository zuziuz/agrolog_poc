"""
UI components for PDF order extraction.
"""
import logging
import streamlit as st
import tempfile
import os
import base64
import uuid
from typing import List
import pandas as pd

from models.task import TaskLocation, ProcessingResult, TaskType
from services.pdf_extractor import PDFExtractor, Order
from services.address_validator import AddressValidator
from services.database_client import DatabaseClient
from services.task_processor import TaskProcessor
from ui.task_input import TaskInputUI

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PDFExtractorUI:
    """UI components for PDF order extraction."""

    def __init__(self,
                 pdf_extractor: PDFExtractor,
                 address_validator: AddressValidator,
                 db_client: DatabaseClient,
                 task_processor: TaskProcessor):
        """
        Initialize PDF extractor UI with required services.

        Args:
            pdf_extractor: PDF extraction service
            address_validator: Address validation service
            db_client: Database client service
            task_processor: Task processor service
        """
        self.pdf_extractor = pdf_extractor
        self.address_validator = address_validator
        self.db_client = db_client
        self.task_processor = task_processor
        self.task_input_ui = TaskInputUI(address_validator, db_client, task_processor)

        # Initialize session state for PDF extraction
        if 'pdf_orders' not in st.session_state:
            st.session_state.pdf_orders = []
        if 'pdf_tasks' not in st.session_state:
            st.session_state.pdf_tasks = []
        if 'current_pdf_file' not in st.session_state:
            st.session_state.current_pdf_file = None
        if 'task_index' not in st.session_state:
            st.session_state.task_index = 0
        if 'pdf_device_number' not in st.session_state:
            st.session_state.pdf_device_number = "DEVICE NUMBER"

    def render_pdf_extraction_ui(self):
        """Render the PDF extraction UI."""
        st.subheader("Extract Orders from PDF")

        st.markdown("""
        Upload a PDF file containing shipping orders. The system will extract load and unload 
        addresses and allow you to review, modify, and submit them as individual tasks.
        
        1. Upload a PDF file
        2. Review extracted tasks
        3. Arrange them in the desired sequence
        4. Submit them as tasks
        """)

        # PDF upload
        uploaded_file = st.file_uploader("Upload PDF file", type="pdf")

        if uploaded_file:
            # Check if we've already processed this file
            if st.session_state.current_pdf_file != uploaded_file.name:
                st.session_state.current_pdf_file = uploaded_file.name
                st.session_state.pdf_orders = []
                st.session_state.pdf_tasks = []
                st.session_state.task_index = 0
                st.session_state.pdf_device_number = "TRUCK-001"

                # Extract orders from PDF
                with st.spinner("Extracting orders from PDF..."):
                    try:
                        orders = self.pdf_extractor.extract_orders_from_uploaded_file(uploaded_file)
                        st.session_state.pdf_orders = orders.orders

                        # Create individual tasks from orders
                        self._create_tasks_from_orders(orders.orders)

                        st.success(f"Extracted {len(orders.orders)} orders from PDF!")
                    except Exception as e:
                        st.error(f"Error extracting orders from PDF: {str(e)}")
                        return

            # Create two columns for PDF viewer and task form
            col1, col2 = st.columns([1, 1])

            with col1:
                # Display PDF
                self._display_pdf(uploaded_file)

                # Device number input (used for all tasks)
                device_number = st.text_input(
                    "Truck (Device Number)",
                    value=st.session_state.pdf_device_number,
                    key="pdf_device_input"
                )
                # Update the session state with the new value
                st.session_state.pdf_device_number = device_number

            with col2:
                # If we have tasks, show them with a navigation interface
                if st.session_state.pdf_tasks:
                    self._render_task_management_ui()
                else:
                    st.warning("No orders extracted from PDF. Please upload a different file.")

    def _create_tasks_from_orders(self, orders: List[Order]):
        """
        Create individual tasks from extracted orders.

        Args:
            orders: List of Order objects with load and unload addresses
        """
        st.session_state.pdf_tasks = []

        # For each order, create separate pickup and delivery tasks
        for i, order in enumerate(orders):
            # Create pickup task
            pickup_task = TaskLocation.from_pdf_order(
                order_data=order.dict(),
                is_pickup=True,
                local_id_prefix=f"ORD{i+1}"
            )
            pickup_task.sequence = len(st.session_state.pdf_tasks)

            # Create delivery task
            delivery_task = TaskLocation.from_pdf_order(
                order_data=order.dict(),
                is_pickup=False,
                local_id_prefix=f"ORD{i+1}"
            )
            delivery_task.sequence = len(st.session_state.pdf_tasks) + 1

            # Add tasks to the list
            st.session_state.pdf_tasks.append(pickup_task)
            st.session_state.pdf_tasks.append(delivery_task)

    def _render_task_management_ui(self):
        """Show the task management UI."""
        # Show total tasks count and current task
        total_tasks = len(st.session_state.pdf_tasks)
        current_index = st.session_state.task_index
        current_task = st.session_state.pdf_tasks[current_index]

        # Create task type indicators (PICKUP or DELIVERY)
        task_type_label = "PICKUP" if current_task.task_type == TaskType.PICKUP else "DELIVERY"

        # Show task info header with colored badge for type
        st.markdown(
            f"""
            <div style="display: flex; align-items: center; margin-bottom: 1rem;">
                <div style="font-size: 1.2rem; font-weight: bold; margin-right: 10px;">
                    Task {current_index + 1} of {total_tasks}
                </div>
                <div style="background-color: {'#28a745' if current_task.task_type == TaskType.PICKUP else '#dc3545'}; 
                            color: white; padding: 3px 10px; border-radius: 10px; font-weight: bold;">
                    {task_type_label}
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

        # Create navigation controls
        col_nav1, col_nav2, col_nav3, col_nav4 = st.columns([1, 1, 1, 1])

        with col_nav1:
            if st.button("Previous", disabled=current_index <= 0):
                st.session_state.task_index = max(0, current_index - 1)
                st.rerun()

        with col_nav2:
            if st.button("Next", disabled=current_index >= total_tasks - 1):
                st.session_state.task_index = min(total_tasks - 1, current_index + 1)
                st.rerun()

        with col_nav3:
            # Button to add a new blank task after the current one
            if st.button("Add New Task"):
                self._add_new_task(after_index=current_index)
                st.rerun()

        with col_nav4:
            # Button to delete the current task
            if st.button("Delete Task", type="secondary"):
                if len(st.session_state.pdf_tasks) > 1:  # Don't allow deleting the last task
                    del st.session_state.pdf_tasks[current_index]
                    # Adjust index if we deleted the last task
                    if current_index >= len(st.session_state.pdf_tasks):
                        st.session_state.task_index = len(st.session_state.pdf_tasks) - 1
                    st.rerun()
                else:
                    st.error("Cannot delete the last task")

        # Task type selector
        task_type = st.selectbox(
            "Task Type",
            options=[TaskType.PICKUP, TaskType.DELIVERY],
            index=0 if current_task.task_type == TaskType.PICKUP else 1,
            format_func=lambda x: "Pickup (From)" if x == TaskType.PICKUP else "Delivery (To)",
            key=f"task_type_{current_index}"
        )

        # Update task type if changed
        current_task.task_type = task_type
        st.session_state.pdf_tasks[current_index] = current_task

        # Task editor form
        updated_task = self.task_input_ui.create_task_form(
            key_prefix=f"task_{current_index}",
            prefill=current_task
        )

        if updated_task:
            # Preserve the task type and sequence
            updated_task.task_type = current_task.task_type
            updated_task.sequence = current_task.sequence
            st.session_state.pdf_tasks[current_index] = updated_task

        # Show task sequence controls
        self._show_task_sequence_controls()

        # Add button to submit all tasks
        if st.button("Submit All Tasks in Sequence", key="submit_all"):
            self._process_all_tasks()

    def _add_new_task(self, after_index: int, task_type: TaskType = TaskType.PICKUP):
        """
        Add a new blank task to the list.

        Args:
            after_index: Index to insert the task after
            task_type: Type of task to add
        """
        # Create a new blank task
        new_task = TaskLocation(
            local_id=f"NEW-{'PICKUP' if task_type == TaskType.PICKUP else 'DELIVERY'}-{uuid.uuid4().hex[:6]}",
            task_type=task_type,
            location_address="",
            sequence=after_index + 1
        )

        # Insert after current index
        st.session_state.pdf_tasks.insert(after_index + 1, new_task)
        st.session_state.task_index = after_index + 1

        # Update sequence numbers
        self._update_sequence_numbers()

    def _show_task_sequence_controls(self):
        """Show controls for arranging the tasks in sequence."""
        st.subheader("Task Sequence")

        st.info("""
        Arrange the order of tasks to create a route. You can move tasks up and down
        to optimize your route - for example, collect multiple items before delivering them.
        """)

        # Show the current sequence of tasks
        tasks_df = self._create_tasks_summary_df()
        st.dataframe(tasks_df, height=400)

        # Controls for moving tasks up and down
        col_arr1, col_arr2 = st.columns(2)

        with col_arr1:
            if st.button("Move Current Task Up", disabled=st.session_state.task_index <= 0):
                self._move_task_up()

        with col_arr2:
            if st.button("Move Current Task Down", disabled=st.session_state.task_index >= len(st.session_state.pdf_tasks) - 1):
                self._move_task_down()

    def _create_tasks_summary_df(self) -> pd.DataFrame:
        """
        Create a DataFrame summarizing all tasks.

        Returns:
            DataFrame with task summaries
        """
        data = []

        for i, task in enumerate(st.session_state.pdf_tasks):
            data.append({
                "Sequence": i + 1,
                "Type": "Pickup" if task.task_type == TaskType.PICKUP else "Delivery",
                "Local ID": task.local_id,
                "Address": task.location_address,
                "Current": "➡️" if i == st.session_state.task_index else ""
            })

        return pd.DataFrame(data)

    def _move_task_up(self):
        """Move the current task up in the sequence."""
        if st.session_state.task_index > 0:
            current_index = st.session_state.task_index
            tasks = st.session_state.pdf_tasks
            tasks[current_index], tasks[current_index - 1] = tasks[current_index - 1], tasks[current_index]
            st.session_state.pdf_tasks = tasks
            st.session_state.task_index = current_index - 1
            self._update_sequence_numbers()

    def _move_task_down(self):
        """Move the current task down in the sequence."""
        if st.session_state.task_index < len(st.session_state.pdf_tasks) - 1:
            current_index = st.session_state.task_index
            tasks = st.session_state.pdf_tasks
            tasks[current_index], tasks[current_index + 1] = tasks[current_index + 1], tasks[current_index]
            st.session_state.pdf_tasks = tasks
            st.session_state.task_index = current_index + 1
            self._update_sequence_numbers()

    def _update_sequence_numbers(self):
        """Update sequence numbers for all tasks based on their positions."""
        for i, task in enumerate(st.session_state.pdf_tasks):
            task.sequence = i

    def _process_all_tasks(self):
        """Process all tasks in the current sequence."""
        # Validate tasks first
        invalid_tasks = []
        device_number = st.session_state.pdf_device_number

        if not device_number:
            st.error("Please enter a device number")
            return

        for i, task in enumerate(st.session_state.pdf_tasks):
            if not task.location_address:
                invalid_tasks.append(f"Task {i+1}: Missing address")

        if invalid_tasks:
            st.error("Please fix the following errors before submitting:")
            for error in invalid_tasks:
                st.error(error)
            return

        # Process all tasks
        results = []
        progress_bar = st.progress(0)
        status_text = st.empty()

        for i, task in enumerate(st.session_state.pdf_tasks):
            status_text.text(f"Processing task {i+1} of {len(st.session_state.pdf_tasks)}")

            try:
                # Process the task
                result = self.task_input_ui.process_single_task(task, device_number)
                result.task_type = task.task_type  # Add task type to result
                results.append(result)
            except Exception as e:
                st.error(f"Error processing task {i+1}: {str(e)}")
                continue

            # Update progress
            progress_bar.progress((i + 1) / len(st.session_state.pdf_tasks))

        status_text.text("Processing complete!")
        progress_bar.progress(1.0)

        # Show summary of verified coordinates
        self._show_verification_summary(results)

    def _show_verification_summary(self, results: List[ProcessingResult]):
        """
        Show summary of which tasks used verified coordinates.

        Args:
            results: List of ProcessingResult objects
        """
        if not results:
            st.warning("No results to display")
            return

        st.subheader("PDF Extraction Results")

        # First, create a summary table for the extraction results
        pickup_count = sum(1 for r in results if hasattr(r, 'task_type') and r.task_type == TaskType.PICKUP)
        delivery_count = len(results) - pickup_count

        st.markdown(f"""
        ### Summary:
        - Total Tasks Processed: **{len(results)}**
        - Pickup Tasks: **{pickup_count}**
        - Delivery Tasks: **{delivery_count}**
        """)

        # Show coordinates verification status
        verified_count = sum(1 for r in results if r.is_verified)
        unverified_count = len(results) - verified_count

        # Use columns for status display
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"✅ Using verified coordinates: {verified_count} tasks")
        with col2:
            st.warning(f"❌ Using original coordinates: {unverified_count} tasks")

        # Create a simple data structure with strings only
        st.subheader("Task Details")

        # Simple list of dictionaries approach - all values are strings
        table_data = []
        for i, result in enumerate(results):
            # Get the task type with a fallback
            task_type = "Pickup" if hasattr(result, 'task_type') and result.task_type == TaskType.PICKUP else "Delivery"

            # Format coordinates nicely
            orig_lat = f"{float(result.original_lat):.6f}"
            orig_lng = f"{float(result.original_lng):.6f}"

            # Handle verified coordinates
            if result.is_verified and result.verified_lat is not None:
                ver_lat = f"{float(result.verified_lat):.6f}"
                ver_lng = f"{float(result.verified_lng):.6f}"
            else:
                ver_lat = "N/A"
                ver_lng = "N/A"

            # Create a row as dictionary with all string values
            row = {
                "Sequence": str(i + 1),
                "Type": task_type,
                "Task ID": str(result.task_id),
                "Address": str(result.address),
                "Verified?": "✅" if result.is_verified else "❌",
                "Original Coordinates": f"({orig_lat}, {orig_lng})",
                "Verified Coordinates": f"({ver_lat}, {ver_lng})" if result.is_verified else "N/A"
            }

            table_data.append(row)

        # Create and display the DataFrame - all values are already strings
        df = pd.DataFrame(table_data)
        st.dataframe(df, use_container_width=True)

        # Add a success message
        st.success("All PDF-extracted tasks have been processed successfully!")

        # Reset PDF extraction state now that we're done
        st.session_state.pdf_orders = []
        st.session_state.pdf_tasks = []
        st.session_state.current_pdf_file = None
        st.session_state.task_index = 0

    @staticmethod
    def _display_pdf(uploaded_file):
        """
        Display the uploaded PDF file.

        Args:
            uploaded_file: Streamlit uploaded file object
        """
        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
            temp_file.write(uploaded_file.getvalue())
            temp_file_path = temp_file.name

        # Display PDF using HTML iframe
        with open(temp_file_path, "rb") as f:
            base64_pdf = base64.b64encode(f.read()).decode('utf-8')

        # Embed PDF viewer
        pdf_display = f"""
        <iframe
            src="data:application/pdf;base64,{base64_pdf}#pagemode=none"
            width="100%"
            height="800"
            style="border: none;"
        ></iframe>
        """
        st.markdown(pdf_display, unsafe_allow_html=True)

        # Clean up the temporary file
        os.unlink(temp_file_path)