from dotenv import load_dotenv
load_dotenv()

import logging
import os
import json
import io
import re

from azure.storage.blob import BlobServiceClient
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.core.credentials import AzureKeyCredential
from azure.ai.textanalytics import TextAnalyticsClient
from langchain_text_splitters import RecursiveCharacterTextSplitter
# from tiktoken import encoding_for_model # Uncomment if you use tiktoken for length_function


# --- YOUR AZURE AI SERVICE AND STORAGE CREDENTIALS ---
DOC_INTEL_ENDPOINT = os.getenv("AZURE_AI_DOCUMENT_INTELLIGENCE_ENDPOINT")
DOC_INTEL_KEY = os.getenv("AZURE_AI_DOCUMENT_INTELLIGENCE_KEY")
LANGUAGE_SERVICE_ENDPOINT = os.getenv("AZURE_LANGUAGE_SERVICE_ENDPOINT")
LANGUAGE_SERVICE_KEY = os.getenv("AZURE_LANGUAGE_SERVICE_KEY")
BLOB_STORAGE_CONNECTION_STRING = os.getenv("CONNECTION_STRING")


# --- YOUR AZURE BLOB STORAGE CONTAINER NAMES ---
# UPDATED based on your provided information
TARGET_CONTAINER_NAME = "rawdocument"
PROCESSED_CONTAINER_NAME = "processed-text-metadata"


# --- Initialize Azure Service Clients ---
# These are global variables so they can be accessed by functions
doc_intel_client = None
text_analytics_client = None
blob_service_client = None

try:
    logging.info("Initializing Azure clients...")
    print("DEBUG: Attempting to initialize Azure clients...")

    # Document Intelligence Client
    doc_intel_client = DocumentIntelligenceClient(
        endpoint=DOC_INTEL_ENDPOINT,
        credential=AzureKeyCredential(DOC_INTEL_KEY)
    )
    print("DEBUG: Document Intelligence client assigned.")

    # Azure AI Language Client
    text_analytics_client = TextAnalyticsClient(
        endpoint=LANGUAGE_SERVICE_ENDPOINT,
        credential=AzureKeyCredential(LANGUAGE_SERVICE_KEY)
    )
    print("DEBUG: Azure AI Language client assigned.")

    # Blob Storage Client - Using the hardcoded connection string for local testing
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_STORAGE_CONNECTION_STRING)
    print("DEBUG: Blob Storage client assigned.")

    logging.info("Azure clients initialized successfully.")
    print("DEBUG: Azure clients initialized successfully.")

except Exception as e:
    logging.error(f"Error initializing Azure clients: {e}")
    print(f"DEBUG: !!! CRITICAL ERROR during client initialization: {e}")
    raise # Re-raise the exception to stop execution


# --- 2. PHI Redaction Function (Simplified Example) ---
def redact_phi(text: str) -> str:
    logging.info("Attempting to redact PHI...")
    redacted_text = text

    # Simple regex for common PHI patterns (PLACEHOLDERS, NOT PRODUCTION-READY)
    redacted_text = re.sub(r'\b(Mr\.|Mrs\.|Ms\.|Dr\.)?\s?[A-Z][a-z]+\s[A-Z][a-z]+\b', '[PATIENT_NAME]', redacted_text)
    redacted_text = re.sub(r'\b\d{1,2}/\d{1,2}/\d{2,4}\b|\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s\d{1,2},\s\d{4}\b|\b\d{4}-\d{2}-\d{2}\b', '[DATE]', redacted_text)
    redacted_text = re.sub(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', '[PHONE_NUMBER]', redacted_text)
    redacted_text = re.sub(r'\d{3}-\d{2}-\d{4}', '[SSN]', redacted_text)
    redacted_text = re.sub(r'\b\d+\s[A-Za-z]+\s(Street|St|Road|Rd|Avenue|Ave|Boulevard|Blvd|Lane|Ln|Drive|Dr)\b', '[ADDRESS]', redacted_text)

    # Example of using Azure AI Language PII Detection (uncomment and integrate if you want to use it)
    # from azure.ai.textanalytics import PIIEntityCollection
    # try:
    #     response = text_analytics_client.recognize_pii_entities(documents=[text], language="en")
    #     for doc in response:
    #         if doc.pii_entities:
    #             for entity in doc.pii_entities:
    #                 redacted_text = redacted_text.replace(entity.text, f'[{entity.category.upper()}]')
    # except Exception as e:
    #     logging.warning(f"PII detection failed: {e}")

    logging.info("PHI redaction complete.")
    return redacted_text


# --- 3. Main Local Processing Logic (Processes all PDFs in TARGET_CONTAINER_NAME) ---
def process_all_documents_in_container(): # Renamed for clarity
    try:
        logging.info("Starting processing of documents in container.")

        # Get a client for the target container
        container_client = blob_service_client.get_container_client(TARGET_CONTAINER_NAME)
        logging.info(f"Connected to input container: {TARGET_CONTAINER_NAME}")

        # List all blobs in the container
        blob_list = container_client.list_blobs()
        document_count = 0
        processed_successfully_count = 0

        print(f"\n--- Starting processing of documents in '{TARGET_CONTAINER_NAME}' ---")

        # Iterate through each blob found in the container
        for blob_item in blob_list:
            blob_name = blob_item.name # This gets the name of the current blob in the loop
            
            # Process only PDF files
            if not blob_name.lower().endswith(".pdf"):
                logging.info(f"Skipping non-PDF file: {blob_name}")
                print(f"INFO: Skipping non-PDF file: {blob_name}")
                continue

            document_count += 1
            logging.info(f"Attempting to process document {document_count}: {blob_name}")
            print(f"\n--- Processing '{blob_name}' (Document {document_count}/{len(list(container_client.list_blobs()))} total) ---") # Added total count for better logging

            try:
                # 1. Download the blob content (PDF)
                logging.info(f"Downloading {blob_name} from {TARGET_CONTAINER_NAME}...")
                blob_client = container_client.get_blob_client(blob_name)
                download_stream = blob_client.download_blob()
                pdf_bytes = download_stream.readall()
                logging.info(f"Downloaded {blob_name}. Size: {len(pdf_bytes)} bytes.")
                print(f"DEBUG: Downloaded '{blob_name}'.")

                # 2. Analyze document with Document Intelligence
                logging.info(f"Analyzing {blob_name} with Document Intelligence ('prebuilt-document' model)...")
                poller = doc_intel_client.begin_analyze_document(
                    "prebuilt-document", # Use 'prebuilt-document' for general purpose
                    pdf_bytes,
                    content_type="application/pdf"
                )
                result = poller.result()
                logging.info(f"Document Intelligence analysis for {blob_name} completed.")
                print(f"DEBUG: Document Intelligence analysis for '{blob_name}' completed.")

                # Extract markdown content for chunking
                markdown_content = result.content if result.content else ""
                logging.info(f"Extracted {len(markdown_content)} characters in Markdown format from {blob_name}.")

                if not markdown_content.strip():
                    logging.warning(f"No text content was extracted by Document Intelligence for {blob_name}. Skipping chunking for this document.")
                    print(f"WARNING: No text content extracted for '{blob_name}'. Skipping.")
                    continue # Move to the next blob if no content

                # 3. Chunk the extracted text
                logging.info(f"Chunking extracted text for {blob_name}...")
                text_splitter = RecursiveCharacterTextSplitter(
                    chunk_size=490,  # Max characters per chunk (adjust based on your LLM's context window)
                    chunk_overlap=88, # Overlap to maintain context between chunks
                    length_function=len, # Use character length for simplicity
                    separators=[
                        "\n\n\n", # Triple newline for large breaks
                        "\n\n",  # Double newline for paragraphs
                        "\n",    # Single newline for lines
                        " ",     # Space for words
                        "",      # Fallback for characters
                    ]
                )
                chunks = text_splitter.create_documents([markdown_content])
                logging.info(f"Created {len(chunks)} chunks for {blob_name}.")
                print(f"DEBUG: Created {len(chunks)} chunks for '{blob_name}'.")

                processed_chunks_data = []
                for i, chunk_doc in enumerate(chunks):
                    chunk_content = chunk_doc.page_content
                    
                    # 4. Redact PHI from each chunk
                    redacted_chunk_content = redact_phi(chunk_content)

                    # 5. (Optional) Extract Key Phrases/Entities with Azure AI Language
                    key_phrases = []
                    entities = []
                    try:
                        # Only perform Language Service calls if the text is substantial
                        if len(redacted_chunk_content.strip()) > 10 and text_analytics_client: # Added check for text_analytics_client being initialized
                            language_response_kp = text_analytics_client.extract_key_phrases(documents=[redacted_chunk_content])
                            if language_response_kp and language_response_kp[0].key_phrases:
                                key_phrases = [kp for kp in language_response_kp[0].key_phrases]

                            language_response_ent = text_analytics_client.recognize_entities(documents=[redacted_chunk_content])
                            if language_response_ent and language_response_ent[0].entities:
                                entities = [{"text": e.text, "category": e.category} for e in language_response_ent[0].entities]
                        elif not text_analytics_client:
                             logging.warning("Azure AI Language client not initialized. Skipping key phrase and entity extraction.")
                             print("WARNING: Azure AI Language client not initialized. Skipping key phrase and entity extraction.")
                        else:
                            logging.info(f"Chunk {i} for {blob_name} too short for Language Service analysis.")


                    except Exception as e:
                        logging.warning(f"Azure AI Language processing failed for chunk {i} of {blob_name}: {e}")
                        print(f"WARNING: Azure AI Language processing failed for chunk {i}: {e}")
                        # Continue processing other chunks even if one fails

                    # Prepare chunk for storage
                    chunk_data = {
                        "chunk_id": f"{os.path.splitext(blob_name)[0].replace(' ', '_')}_chunk_{i:03d}", # Replace spaces for cleaner filenames
                        "source_document": blob_name,
                        "original_chunk_content": chunk_content, # Keep original for reference or remove for strict PHI
                        "redacted_chunk_content": redacted_chunk_content,
                        "key_phrases": key_phrases,
                        "entities": entities,
                        "metadata": chunk_doc.metadata # Langchain adds source page/chunk info here
                    }
                    processed_chunks_data.append(chunk_data)

                logging.info(f"Redaction/Processing complete for all chunks in {blob_name}.")

                # 6. Upload processed chunks as JSON
                output_filename = f"{os.path.splitext(blob_name)[0].replace(' ', '_')}_chunks.json" # Replace spaces for cleaner filenames
                output_blob_client = blob_service_client.get_blob_client(
                    PROCESSED_CONTAINER_NAME, output_filename
                )

                logging.info(f"Uploading processed chunks for {blob_name} to {PROCESSED_CONTAINER_NAME}/{output_filename}...")
                output_json_content = json.dumps(processed_chunks_data, indent=2, ensure_ascii=False) # ensure_ascii for non-English chars
                output_blob_client.upload_blob(output_json_content, overwrite=True)
                logging.info(f"Successfully uploaded processed chunks for {blob_name}.")
                print(f"SUCCESS: Processed chunks for '{blob_name}' uploaded to '{PROCESSED_CONTAINER_NAME}/{output_filename}'")
                processed_successfully_count += 1

            except Exception as e:
                logging.error(f"Error processing individual document '{blob_name}': {e}", exc_info=True)
                print(f"ERROR: Could not process '{blob_name}': {e}")
                # Continue to the next document in the loop even if one fails


        if document_count == 0:
            print(f"No PDF documents found in the '{TARGET_CONTAINER_NAME}' container.")
        else:
            print(f"\n--- Finished processing {processed_successfully_count}/{document_count} documents. ---")
            logging.info(f"Script finished processing {processed_successfully_count}/{document_count} documents in main block.")

    except Exception as e:
        logging.error(f"FATAL ERROR during script execution: {e}", exc_info=True)
        print(f"ERROR: FATAL ERROR during script execution: {e}")
        raise # Re-raise to show traceback for unhandled fatal errors


# --- Entry point for running the script ---
if __name__ == "__main__":
    # Configure logging to also show DEBUG messages in console
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # If you want more verbose output for debugging during runtime, change logging.INFO to logging.DEBUG above.
    
    print("DEBUG: Script execution started.")
    try:
        process_all_documents_in_container() # Call the main processing function
        logging.info("Local document processing script finished successfully.")
        print("DEBUG: Local document processing script finished successfully.")
    except Exception as e:
        logging.error(f"Script terminated with an unhandled error: {e}", exc_info=True)
        print(f"DEBUG: !!! Script terminated with an unhandled error: {e}")
