"""
Service for extracting order information from PDFs using Google's Gemini API.
"""
import json
import logging
import tempfile
import os
from typing import List, Any
from pathlib import Path

from google import genai
from google.genai import types
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Order(BaseModel):
    """
    Represents a single order with load and unload locations.

    Attributes:
        load: The loading location information (company, address, postal code, city)
        unload: The unloading location information (company, address, postal code, city)
    """
    load: str
    unload: str

    def clean_addresses(self):
        """Clean addresses by removing newlines and extra spaces."""
        if self.load:
            self.load = " ".join(self.load.replace("\n", " ").split())
        if self.unload:
            self.unload = " ".join(self.unload.replace("\n", " ").split())


class Orders(BaseModel):
    """
    Container for a list of orders.

    Attributes:
        orders: A list of Order objects, must contain at least one item
    """
    orders: List[Order] = Field(..., min_items=1)


class PDFExtractor:
    """Service for extracting order information from PDFs."""

    def __init__(self, api_key: str, few_shot_examples_path: str = None):
        """
        Initialize PDF extractor with Google API key.

        Args:
            api_key: Google Gemini API key
            few_shot_examples_path: Path to few-shot examples JSON file
        """
        self.client = genai.Client(api_key=api_key)
        self.few_shot_examples_path = few_shot_examples_path

        self.system_prompt = """
        You are an AI assistant that extracts logistics orders information from PDFs.
        Extract all pairs of load and unload addresses.
        Do not confuse and be extra careful with 'Sender' with 'Loading point' and 'Receiver' with 'Unloading point'.
        Only extract 'Loading point' and 'Unloading point'.
        Be thorough and accurate in your extraction.
        """

        self.main_prompt = "Please extract the order(s) information from the attached PDF document."

    def create_few_shot_examples(self) -> List[Any]:
        """
        Create few-shot examples for the Gemini model.

        Returns:
            List of example prompts and responses
        """
        if not self.few_shot_examples_path or not os.path.exists(self.few_shot_examples_path):
            return []

        few_shot_examples = []
        with open(self.few_shot_examples_path, "r") as f:
            examples = json.load(f)

        for example in examples:
            filepath = Path(example["pdf_path"])
            if not filepath.exists():
                logger.warning(f"Example PDF file not found: {example['pdf_path']}")
                continue

            few_shot_examples.extend(
                [
                    "Please extract data from the following PDF",
                    types.Part.from_bytes(
                        data=filepath.read_bytes(),
                        mime_type="application/pdf",
                    ),
                    json.dumps(example["expected_output"], indent=2),
                ],
            )

        return few_shot_examples

    def extract_orders_from_pdf(self, pdf_data: bytes) -> Orders:
        """
        Extract orders from PDF data.

        Args:
            pdf_data: PDF file data as bytes

        Returns:
            Orders object containing extracted orders

        Raises:
            Exception: If extraction fails
        """
        try:
            few_shot_examples = self.create_few_shot_examples()

            response = self.client.models.generate_content(
                model="models/gemini-2.0-flash",
                contents=[
                    self.system_prompt,
                    *few_shot_examples,
                    types.Part.from_bytes(
                        data=pdf_data,
                        mime_type="application/pdf",
                    ),
                    self.main_prompt,
                ],
                config={"response_mime_type": "application/json", "response_schema": Orders},
            )

            # Parse the JSON response
            result = response.parsed

            # Clean the addresses in each order
            for order in result.orders:
                order.clean_addresses()

            return result

        except Exception as e:
            logger.error(f"Error extracting orders from PDF: {str(e)}")
            raise

    def extract_orders_from_file(self, pdf_file_path: str) -> Orders:
        """
        Extract orders from a PDF file.

        Args:
            pdf_file_path: Path to PDF file

        Returns:
            Orders object containing extracted orders
        """
        with open(pdf_file_path, "rb") as f:
            pdf_data = f.read()

        return self.extract_orders_from_pdf(pdf_data)

    def extract_orders_from_uploaded_file(self, uploaded_file) -> Orders:
        """
        Extract orders from a Streamlit uploaded file.

        Args:
            uploaded_file: Streamlit uploaded file object

        Returns:
            Orders object containing extracted orders
        """
        # For Streamlit uploaded files, save to temp file first
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
            temp_file.write(uploaded_file.getvalue())
            temp_file_path = temp_file.name

        try:
            return self.extract_orders_from_file(temp_file_path)
        finally:
            # Clean up the temp file
            os.unlink(temp_file_path)