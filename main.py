"""
Main entry point for the Trucking Tasks Processor application.
"""
import streamlit as st
import logging
from typing import Dict

from services.address_validator import AddressValidator
from services.database_client import DatabaseClient
from services.task_processor import TaskProcessor
from services.pdf_extractor import PDFExtractor
from ui.task_input import TaskInputUI
from ui.address_update import AddressUpdateUI
from ui.pdf_extractor import PDFExtractorUI
from utils.helpers import load_config, ensure_directories_exist

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def initialize_services(config: Dict) -> tuple:
    """
    Initialize all application services.

    Args:
        config: Configuration dictionary

    Returns:
        Tuple of initialized services
    """
    # Initialize components
    address_validator = AddressValidator(config["google_address_validator_api_key"])

    db_client = DatabaseClient(config["project_id"], config["bigquery_service_account_json"])

    task_processor = TaskProcessor(
        base_url=config["api_base_url"],
        api_username=config["loctracker_api_username"],
        password=config["loctracker_api_password"]
    )

    pdf_extractor = PDFExtractor(
        api_key=config["gemini_api_key"],
        few_shot_examples_path=config.get("few_shot_examples_path")
    )

    return address_validator, db_client, task_processor, pdf_extractor


def initialize_ui_components(services: tuple) -> tuple:
    """
    Initialize UI components with services.

    Args:
        services: Tuple of services

    Returns:
        Tuple of UI components
    """
    address_validator, db_client, task_processor, pdf_extractor = services

    # Initialize UI components
    task_input_ui = TaskInputUI(
        address_validator=address_validator,
        db_client=db_client,
        task_processor=task_processor
    )

    address_update_ui = AddressUpdateUI(
        db_client=db_client,
        task_processor=task_processor
    )

    pdf_extractor_ui = PDFExtractorUI(
        pdf_extractor=pdf_extractor,
        address_validator=address_validator,
        db_client=db_client,
        task_processor=task_processor
    )

    return task_input_ui, address_update_ui, pdf_extractor_ui


def main():
    """Main application entry point."""
    # Set page config
    st.set_page_config(
        page_title="Trucking Tasks Processor",
        page_icon="🚚",
        layout="wide",
        initial_sidebar_state="collapsed"
    )

    st.title("Trucking Tasks Processor")

    # Add app description
    st.markdown("""
    This application processes trucking tasks, validates addresses, updates coordinates, 
    and now supports extracting orders from PDF files.

    **Features:**
    - Process tasks individually or in bulk via CSV
    - Update coordinates for existing addresses
    - Extract shipping orders from PDF documents
    - Arrange pick-up and delivery routes in custom order
    """)

    # Ensure required directories exist
    ensure_directories_exist()

    try:
        # Load configuration from Streamlit secrets
        config = load_config(st.secrets)

        # Initialize services and UI components
        services = initialize_services(config)
        ui_components = initialize_ui_components(services)

        # Store services and UI components in session state for access in other functions
        address_validator, db_client, task_processor, pdf_extractor = services
        task_input_ui, address_update_ui, pdf_extractor_ui = ui_components

        # Create tabs for different functionalities
        task_tab, update_tab, pdf_tab = st.tabs([
            "Process Tasks",
            "Update Addresses",
            "Extract Orders from PDF"
        ])

        with task_tab:
            # CSV upload section
            st.subheader("Upload Tasks")
            st.write("Upload a CSV file with tasks or create a new route manually.")

            uploaded_file = st.file_uploader("Upload tasks CSV", type="csv")

            if uploaded_file:
                # Process uploaded CSV
                task_input_ui.process_uploaded_csv(uploaded_file)
            else:
                # Create form for manual route input
                route = task_input_ui.create_from_to_form()

                if route:
                    # Process the route
                    task_input_ui.process_route(route)

        with update_tab:
            # Address update form
            address_update_ui.create_update_form()

        with pdf_tab:
            # PDF extraction UI
            pdf_extractor_ui.render_pdf_extraction_ui()

        # Ensure all buffered data is written to database at the end
        db_client.flush_buffers()

    except Exception as e:
        st.error(f"Error initializing application: {str(e)}")
        logger.error(f"Application initialization error: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()