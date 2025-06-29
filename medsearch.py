import os
import json
from azure.storage.blob import BlobServiceClient
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv
import logging

# Configure logging to show INFO messages and above
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

print("Starting medsearch.py script...") # Added print
load_dotenv()
print(".env file loaded.") # Added print

# --- Azure Blob Storage Configuration ---
CONNECTION_STRING = os.getenv("CONNECTION_STRING")
PROCESSED_CONTAINER_NAME = "processed-text-metadata"

# --- Azure AI Search Configuration ---
AZURE_SEARCH_ENDPOINT = os.getenv("AZURE_SEARCH_ENDPOINT")
AZURE_SEARCH_API_KEY = os.getenv("AZURE_SEARCH_API_KEY")
AZURE_SEARCH_INDEX_NAME = os.getenv("AZURE_SEARCH_INDEX_NAME")

print(f"DEBUG: Blob Connection String exists? {bool("AZURE_CONNECTION_STRING")}") # Added print
print(f"DEBUG: Search Endpoint exists? {bool(AZURE_SEARCH_ENDPOINT)}") # Added print
print(f"DEBUG: Search API Key exists? {bool(AZURE_SEARCH_API_KEY)}") # Added print
print(f"DEBUG: Search Index Name: {AZURE_SEARCH_INDEX_NAME}") # Added print


# --- Initialize Clients ---
blob_service_client = None
processed_container_client = None
search_client = None

try:
    if not CONNECTION_STRING:
        raise ValueError("BLOB_STORAGE_CONNECTION_STRING not found in .env")
    blob_service_client = BlobServiceClient.from_connection_string("CONNECTION_STRING")
    processed_container_client = blob_service_client.get_container_client(PROCESSED_CONTAINER_NAME)
    print(f"DEBUG: Blob container client initialized for '{PROCESSED_CONTAINER_NAME}'.") # Added print
    
    if not AZURE_SEARCH_ENDPOINT or not AZURE_SEARCH_API_KEY or not AZURE_SEARCH_INDEX_NAME:
        raise ValueError("Azure Search credentials or index name not found in .env")
    search_client = SearchClient(
        endpoint=AZURE_SEARCH_ENDPOINT,
        index_name=AZURE_SEARCH_INDEX_NAME,
        credential=AzureKeyCredential(AZURE_SEARCH_API_KEY)
    )
    logging.info("Azure clients initialized successfully.")
    print("DEBUG: Azure Search client initialized.") # Added print

except Exception as e:
    logging.error(f"Error initializing Azure clients: {e}")
    print(f"ERROR: Client initialization failed: {e}") # Added print
    exit(1)

def ingest_documents_to_search():
    logging.info(f"Starting ingestion to Azure AI Search index: {AZURE_SEARCH_INDEX_NAME}")
    print("DEBUG: Starting ingest_documents_to_search function.") # Added print
    
    documents_to_upload = []
    
    # List all blobs in the processed-text-metadata container
    try:
        blob_list = list(processed_container_client.list_blobs()) # Convert to list to iterate fully
        print(f"DEBUG: Found {len(blob_list)} blobs in '{PROCESSED_CONTAINER_NAME}'.") # Added print
    except Exception as e:
        print(f"ERROR: Could not list blobs: {e}") # Added print
        logging.error(f"Error listing blobs: {e}")
        return

    if not blob_list:
        print("DEBUG: No blobs found in the processed container. Nothing to ingest.") # Added print
        return # No blobs, nothing to do

    for blob in blob_list:
        if blob.name.endswith('.json'):
            print(f"DEBUG: Processing blob: {blob.name}") # Added print
            try:
                blob_client = processed_container_client.get_blob_client(blob.name)
                download_stream = blob_client.download_blob()
                json_content = download_stream.readall()
                json_data = json.loads(json_content)
                
                print(f"DEBUG: Successfully downloaded and parsed {blob.name}.") # Added print

                document_name = json_data.get("document_name", "unknown").replace('.pdf', '') # Clean name
                chunks = json_data.get("chunks", []) 

                if not chunks:
                    logging.warning(f"No 'chunks' key or empty list found in {blob.name}, skipping.")
                    print(f"WARNING: No 'chunks' found in {blob.name}.") # Added print
                    continue

                for i, chunk in enumerate(chunks):
                    chunk_id = f"{document_name.replace(' ', '_').replace('/', '_')}-{chunk.get('page', 0)}-{i}"
                    
                    medical_entities_text = [entity.get("text") for entity in chunk.get("entities", []) if entity and entity.get("text")]

                    search_document = {
                        "id": chunk_id,
                        "document_name": document_name,
                        "page_number": chunk.get("page", 0),
                        "chunk_text": chunk.get("text", ""),
                        "medical_entities": medical_entities_text
                    }
                    documents_to_upload.append(search_document)
                
                logging.info(f"Prepared {len(chunks)} chunks from {blob.name} for upload.")
                print(f"DEBUG: Added {len(chunks)} chunks from {blob.name} to upload list.") # Added print

            except json.JSONDecodeError as jde:
                print(f"ERROR: JSON decoding failed for {blob.name}: {jde}") # Added print
                logging.error(f"JSON decoding failed for blob {blob.name}: {jde}")
            except Exception as e:
                print(f"ERROR: Error processing blob {blob.name}: {e}") # Added print
                logging.error(f"Error processing blob {blob.name}: {e}")

    if documents_to_upload:
        print(f"DEBUG: Total documents prepared for upload: {len(documents_to_upload)}") # Added print
        try:
            results = search_client.upload_documents(documents_to_upload)
            for result in results:
                if not result.succeeded:
                    logging.error(f"Failed to upload document {result.key}: {result.error_message}")
                    print(f"ERROR: Failed to upload document {result.key}: {result.error_message}") # Added print
            logging.info(f"Successfully uploaded {len(documents_to_upload)} documents to Azure AI Search.")
            print(f"DEBUG: Upload successful for {len(documents_to_upload)} documents.") # Added print
        except Exception as e:
            logging.error(f"Error uploading documents to Azure AI Search: {e}")
            print(f"ERROR: Final upload to Azure Search failed: {e}") # Added print
    else:
        logging.info("No documents to upload to Azure AI Search.")
        print("DEBUG: No documents were prepared for upload.") # Added print

if __name__ == "__main__":
    ingest_documents_to_search()
