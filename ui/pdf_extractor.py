"""
UI components for PDF order extraction.
"""
import logging
import streamlit as st
import tempfile
import os
import base64
from typing import List
import pandas as pd
from datetime import datetime
import glob

from models.task import TaskLocation, ProcessingResult, TaskType
from services.pdf_extractor import PDFExtractor, Order
from services.address_validator import AddressValidator
from services.database_client import DatabaseClient
from services.task_processor import TaskProcessor
from ui.task_input import TaskInputUI
from utils.helpers import clean_address

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
        if 'pdf_device_number' not in st.session_state:
            st.session_state.pdf_device_number = "12345678912046"
        if 'showing_samples' not in st.session_state:
            st.session_state.showing_samples = True
        if 'sample_path' not in st.session_state:
            st.session_state.sample_path = None

    def render_pdf_extraction_ui(self):
        """Render the PDF extraction UI."""
        st.subheader("Extract Orders from PDF")

        # Only show sample selection if no PDF is currently being processed
        if st.session_state.showing_samples and not st.session_state.current_pdf_file:
            self._show_pdf_samples()

        # Device number input (moved above the PDF and task table)
        device_number = st.text_input(
            "Truck (Device Number)",
            value=st.session_state.pdf_device_number,
            key="pdf_device_input"
        )
        # Update the session state with the new value
        st.session_state.pdf_device_number = device_number

        # PDF upload - only show if no sample is selected
        if not st.session_state.sample_path:
            uploaded_file = st.file_uploader("Upload PDF file", type="pdf")
        else:
            uploaded_file = None

        # Process either uploaded file or selected sample
        if uploaded_file or st.session_state.sample_path:
            # Create columns for PDF viewer and tasks table
            col1, col2 = st.columns([1, 1])

            with col1:
                # Display PDF
                if uploaded_file:
                    # Check if this is a new file
                    if st.session_state.current_pdf_file != uploaded_file.name:
                        st.session_state.current_pdf_file = uploaded_file.name
                        self._process_uploaded_pdf(uploaded_file)

                    self._display_pdf(uploaded_file)

                elif st.session_state.sample_path:
                    # Display sample PDF
                    if st.session_state.current_pdf_file != os.path.basename(st.session_state.sample_path):
                        st.session_state.current_pdf_file = os.path.basename(st.session_state.sample_path)
                        self._process_sample_pdf(st.session_state.sample_path)

                    # Pass the file path directly to the display method
                    self._display_pdf_from_file(st.session_state.sample_path)

            with col2:
                # If we have tasks, show editable table
                if st.session_state.pdf_tasks:
                    self._render_editable_task_table()
                else:
                    st.warning("No orders extracted from PDF. Please upload a different file.")


    def _show_pdf_samples(self):
        """Show available PDF samples from the pdf_samples directory."""
        st.subheader("Sample PDFs")

        try:
            # Get list of PDF files in pdf_samples directory
            sample_files = glob.glob("pdf_samples/*.pdf")

            if not sample_files:
                st.info("No sample PDFs available. Please upload your own PDF file.")
                return

            # Create a selectbox with file names
            sample_names = [os.path.basename(f) for f in sample_files]
            selected_sample = st.selectbox(
                "Select a sample PDF",
                [""] + sample_names,
                key="sample_selector"
            )

            if selected_sample:
                # User selected a sample
                selected_path = os.path.join("pdf_samples", selected_sample)

                # Store the sample path
                st.session_state.sample_path = selected_path
                st.session_state.showing_samples = False

                # Rerun to refresh UI
                st.rerun()

        except Exception as e:
            st.error(f"Error loading PDF samples: {str(e)}")

    def _process_uploaded_pdf(self, uploaded_file):
        """
        Process an uploaded PDF file.

        Args:
            uploaded_file: Streamlit uploaded file object
        """
        # Reset state
        st.session_state.pdf_orders = []
        st.session_state.pdf_tasks = []

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

    def _process_sample_pdf(self, sample_path):
        """
        Process a sample PDF file.

        Args:
            sample_path: Path to the sample PDF file
        """
        # Reset state
        st.session_state.pdf_orders = []
        st.session_state.pdf_tasks = []

        # Extract orders from PDF
        with st.spinner("Extracting orders from PDF..."):
            try:
                # Read the PDF file
                with open(sample_path, "rb") as file:
                    pdf_bytes = file.read()

                # Create a temporary file for processing
                with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
                    temp_file.write(pdf_bytes)
                    temp_file_path = temp_file.name

                # Process the PDF file
                orders = None

                # Try using the extract_orders_from_file method if it exists
                if hasattr(self.pdf_extractor, 'extract_orders_from_file'):
                    orders = self.pdf_extractor.extract_orders_from_file(temp_file_path)
                else:
                    # Create a simple file-like object
                    with open(temp_file_path, "rb") as file:
                        # Create a simple uploaded file substitute
                        class SimpleUploadedFile:
                            def __init__(self, name, file_data):
                                self.name = name
                                self._file_data = file_data

                            def getvalue(self):
                                return self._file_data

                        # Read the entire file content
                        file_data = file.read()
                        simple_file = SimpleUploadedFile(os.path.basename(sample_path), file_data)
                        orders = self.pdf_extractor.extract_orders_from_uploaded_file(simple_file)

                # Clean up the temporary file
                os.unlink(temp_file_path)

                if orders and hasattr(orders, 'orders'):
                    st.session_state.pdf_orders = orders.orders
                    # Create individual tasks from orders
                    self._create_tasks_from_orders(orders.orders)
                    st.success(f"Extracted {len(orders.orders)} orders from PDF!")
                else:
                    st.error("Failed to extract orders from PDF: No orders found")

            except Exception as e:
                st.error(f"Error extracting orders from PDF: {str(e)}")
                logger.error(f"Sample PDF extraction error: {str(e)}", exc_info=True)

    def _create_tasks_from_orders(self, orders: List[Order]):
        """
        Create individual tasks from extracted orders with timestamp in local_id.

        Args:
            orders: List of Order objects with load and unload addresses
        """
        st.session_state.pdf_tasks = []

        # Generate timestamp for local ID
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        # For each order, create separate pickup and delivery tasks
        for i, order in enumerate(orders):
            # Create pickup task with timestamp in local_id
            pickup_task = TaskLocation.from_pdf_order(
                order_data=order.dict(),
                is_pickup=True,
                local_id_prefix=f"{timestamp}_ORD{i+1}"
            )
            pickup_task.sequence = len(st.session_state.pdf_tasks)

            # Create delivery task with timestamp in local_id
            delivery_task = TaskLocation.from_pdf_order(
                order_data=order.dict(),
                is_pickup=False,
                local_id_prefix=f"{timestamp}_ORD{i+1}"
            )
            delivery_task.sequence = len(st.session_state.pdf_tasks) + 1

            # Add tasks to the list
            st.session_state.pdf_tasks.append(pickup_task)
            st.session_state.pdf_tasks.append(delivery_task)

    def _render_editable_task_table(self):
        """Show an editable table of tasks."""
        st.subheader("Extracted Tasks")

        # Create a DataFrame from tasks for display
        tasks_data = []
        for i, task in enumerate(st.session_state.pdf_tasks):
            tasks_data.append({
                "sequence": i + 1,
                "type": "Pickup" if task.task_type == TaskType.PICKUP else "Delivery",
                "local_id": task.local_id,
                "address": task.location_address
            })

        df = pd.DataFrame(tasks_data)

        # Use data_editor to make the address column editable and hide the index
        edited_df = st.data_editor(
            df,
            column_config={
                "sequence": st.column_config.NumberColumn("Sequence", disabled=True),
                "type": st.column_config.SelectboxColumn(
                    "Type",
                    options=["Pickup", "Delivery"],
                    disabled=True
                ),
                "local_id": st.column_config.TextColumn("Local ID", disabled=True),
                "address": st.column_config.TextColumn("Address", disabled=False),
            },
            use_container_width=True,
            key="task_table",
            hide_index=True
        )

        # Update tasks with edited addresses
        for i, row in edited_df.iterrows():
            if i < len(st.session_state.pdf_tasks) and row["address"] != st.session_state.pdf_tasks[i].location_address:
                # Clean the address before storing it
                st.session_state.pdf_tasks[i].location_address = clean_address(row["address"])

        # Add button to submit all tasks
        if st.button("Submit All Tasks", key="submit_all"):
            self._process_all_tasks()
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
        st.session_state.sample_path = None
        st.session_state.showing_samples = True

    @staticmethod
    def _display_pdf(uploaded_file):
        """
        Display the uploaded PDF file using the latest PDF.js version for better browser compatibility.

        Args:
            uploaded_file: Streamlit uploaded file object
        """
        # Get PDF bytes from uploaded file
        pdf_bytes = uploaded_file.getvalue()

        # Create a download button for the PDF
        st.download_button(
            label="📥 Download PDF",
            data=pdf_bytes,
            file_name=uploaded_file.name,
            mime="application/pdf"
        )

        # Encode PDF to base64
        base64_pdf = base64.b64encode(pdf_bytes).decode('utf-8')

        # Use PDF.js viewer with the base64 data
        # Using latest version (5.0.375) with ES module support
        pdf_js_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>PDF Viewer</title>
            <style>
                #pdf-container {{
                    width: 100%;
                    height: 750px;
                    overflow: auto;
                    background: #fafafa;
                    border: 1px solid #e0e0e0;
                }}
                .pdf-page-canvas {{
                    display: block;
                    margin: 5px auto;
                    border: 1px solid #e0e0e0;
                }}
            </style>
        </head>
        <body>
            <div id="pdf-container"></div>
            
            <script type="module">
                // Import the PDF.js library (using ES modules)
                import * as pdfjsLib from 'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/5.0.375/pdf.min.mjs';
                
                // Set the worker source path
                pdfjsLib.GlobalWorkerOptions.workerSrc = 'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/5.0.375/pdf.worker.min.mjs';
                
                // Base64 data of the PDF
                const pdfData = atob('{base64_pdf}');
                
                // Convert base64 to Uint8Array
                const pdfBytes = new Uint8Array(pdfData.length);
                for (let i = 0; i < pdfData.length; i++) {{
                    pdfBytes[i] = pdfData.charCodeAt(i);
                }}
                
                // Load the PDF document
                const loadingTask = pdfjsLib.getDocument({{ data: pdfBytes }});
                loadingTask.promise.then(function(pdf) {{
                    console.log('PDF loaded');
                    
                    // Container for all pages
                    const container = document.getElementById('pdf-container');
                    
                    // Render pages sequentially
                    for (let pageNum = 1; pageNum <= pdf.numPages; pageNum++) {{
                        pdf.getPage(pageNum).then(function(page) {{
                            const scale = 1.5;
                            const viewport = page.getViewport({{scale: scale}});
                            
                            // Create canvas for this page
                            const canvas = document.createElement('canvas');
                            canvas.className = 'pdf-page-canvas';
                            canvas.width = viewport.width;
                            canvas.height = viewport.height;
                            container.appendChild(canvas);
                            
                            // Render PDF page into canvas context
                            const context = canvas.getContext('2d');
                            const renderContext = {{
                                canvasContext: context,
                                viewport: viewport
                            }};
                            page.render(renderContext);
                        }});
                    }}
                }}).catch(function(error) {{
                    console.error('Error loading PDF:', error);
                    document.getElementById('pdf-container').innerHTML = 
                        '<div style="color: red; padding: 20px;">Error loading PDF. Please try downloading instead.</div>';
                }});
            </script>
        </body>
        </html>
        """

        # Display the PDF.js viewer HTML
        st.components.v1.html(pdf_js_html, height=800)

    @staticmethod
    def _display_pdf_from_file(file_path_or_handle):
        """
        Display a PDF using Mozilla's PDF.js viewer (latest version) for better browser compatibility.

        Args:
            file_path_or_handle: Either a file path string or open file handle
        """
        # Get the PDF bytes
        if isinstance(file_path_or_handle, str):
            with open(file_path_or_handle, "rb") as f:
                pdf_bytes = f.read()
        else:
            file_path_or_handle.seek(0)
            pdf_bytes = file_path_or_handle.read()

        # Create a download button for the PDF
        st.download_button(
            label="📥 Download PDF",
            data=pdf_bytes,
            file_name="document.pdf",
            mime="application/pdf"
        )

        # Encode PDF to base64
        base64_pdf = base64.b64encode(pdf_bytes).decode('utf-8')

        # Use PDF.js viewer with the base64 data
        # Using latest version (5.0.375) with ES module support
        pdf_js_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>PDF Viewer</title>
            <style>
                #pdf-container {{
                    width: 100%;
                    height: 750px;
                    overflow: auto;
                    background: #fafafa;
                    border: 1px solid #e0e0e0;
                }}
                .pdf-page-canvas {{
                    display: block;
                    margin: 5px auto;
                    border: 1px solid #e0e0e0;
                }}
            </style>
        </head>
        <body>
            <div id="pdf-container"></div>
            
            <script type="module">
                // Import the PDF.js library (using ES modules)
                import * as pdfjsLib from 'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/5.0.375/pdf.min.mjs';
                
                // Set the worker source path
                pdfjsLib.GlobalWorkerOptions.workerSrc = 'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/5.0.375/pdf.worker.min.mjs';
                
                // Base64 data of the PDF
                const pdfData = atob('{base64_pdf}');
                
                // Convert base64 to Uint8Array
                const pdfBytes = new Uint8Array(pdfData.length);
                for (let i = 0; i < pdfData.length; i++) {{
                    pdfBytes[i] = pdfData.charCodeAt(i);
                }}
                
                // Load the PDF document
                const loadingTask = pdfjsLib.getDocument({{ data: pdfBytes }});
                loadingTask.promise.then(function(pdf) {{
                    console.log('PDF loaded');
                    
                    // Container for all pages
                    const container = document.getElementById('pdf-container');
                    
                    // Render pages sequentially
                    for (let pageNum = 1; pageNum <= pdf.numPages; pageNum++) {{
                        pdf.getPage(pageNum).then(function(page) {{
                            const scale = 1.5;
                            const viewport = page.getViewport({{scale: scale}});
                            
                            // Create canvas for this page
                            const canvas = document.createElement('canvas');
                            canvas.className = 'pdf-page-canvas';
                            canvas.width = viewport.width;
                            canvas.height = viewport.height;
                            container.appendChild(canvas);
                            
                            // Render PDF page into canvas context
                            const context = canvas.getContext('2d');
                            const renderContext = {{
                                canvasContext: context,
                                viewport: viewport
                            }};
                            page.render(renderContext);
                        }});
                    }}
                }}).catch(function(error) {{
                    console.error('Error loading PDF:', error);
                    document.getElementById('pdf-container').innerHTML = 
                        '<div style="color: red; padding: 20px;">Error loading PDF. Please try downloading instead.</div>';
                }});
            </script>
        </body>
        </html>
        """

        # Display the PDF.js viewer HTML
        st.components.v1.html(pdf_js_html, height=800)
