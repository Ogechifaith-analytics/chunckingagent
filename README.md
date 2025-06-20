# Chunkingagent (Medical Document Processor)

## Project Description

This project provides a robust solution for processing medical documents. It automates the extraction of text and key information from various document types (e.g., consultation summaries, discharge summaries, laboratory reports), performs language analysis (like PII detection or sentiment analysis), and then intelligently chunks the text before storing the processed data and metadata in Azure Blob Storage. This helps in organizing unstructured medical data for further analysis or downstream applications.

## Features

* **Document Text Extraction:** Utilizes Azure Document Intelligence to accurately extract text from diverse medical document formats.
* **Language Analysis:** Integrates with Azure Language Service to perform operations like Named Entity Recognition (NER) for PII detection or other linguistic insights.
* **Adaptive Text Chunking:** Breaks down large documents into manageable, semantically coherent chunks using `RecursiveCharacterTextSplitter` from `langchain-text-splitters`, making it suitable for retrieval-augmented generation (RAG) or further processing.
* **Azure Blob Storage Integration:** Seamlessly reads raw documents from a specified input container and stores processed text and metadata in an output container.
* **Secure Credential Handling:** Utilizes environment variables (`.env` file for local development) to keep sensitive API keys and connection strings out of the codebase.

## Installation

To set up and run this project locally, follow these steps:

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/Ogechifaith-analytics/chunckingagent.git](https://github.com/Ogechifaith-analytics/chunckingagent.git)
    cd chunckingagent/med_processor
    ```

2.  **Create and activate a Python virtual environment:**
    It's highly recommended to use a virtual environment to manage dependencies.
    ```bash
    python -m venv .med
    .med\Scripts\activate   # On Windows
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Set up Environment Variables:**
    Create a file named `.env` in the root of your `med_processor` directory (the same directory as `local_processor.py`). Populate it with your Azure service credentials:

    ```env
    DOC_INTEL_ENDPOINT = os.getenv("AZURE_AI_DOCUMENT_INTELLIGENCE_ENDPOINT")
DOC_INTEL_KEY = os.getenv("AZURE_AI_DOCUMENT_INTELLIGENCE_KEY")
LANGUAGE_SERVICE_ENDPOINT = os.getenv("AZURE_LANGUAGE_SERVICE_ENDPOINT")
LANGUAGE_SERVICE_KEY = os.getenv("AZURE_LANGUAGE_SERVICE_KEY")
BLOB_STORAGE_CONNECTION_STRING = os.getenv("CONNECTION_STRING")
    ```
   
## Usage

1.  **Upload Raw Documents:**
    Place your raw medical documents (e.g., PDFs) into the Azure Blob Storage container specified by `TARGET_CONTAINER_NAME` (which is typically `rawdocument`).

2.  **Run the Processor Script:**
    Ensure your virtual environment is active and your `.env` file is correctly configured.
    ```bash
    python local_processor.py
    ```

    The script will:
    * Iterate through documents in the `rawdocument` container.
    * Extract text and perform initial analysis using Azure Document Intelligence.
    * Perform language analysis (e.g., PII detection) using Azure Language Service.
    * Chunk the extracted text based on the defined strategy.
    * Upload the processed text and associated metadata to the `processed-text-metadata` container.

## Configuration

The following parameters can be adjusted within `local_processor.py`:

* `TARGET_CONTAINER_NAME`: The name of the Azure Blob Storage container where raw documents are uploaded (default: "rawdocument").
* `PROCESSED_CONTAINER_NAME`: The name of the Azure Blob Storage container where processed text and metadata will be stored (default: "processed-text-metadata").
* **Chunking Parameters:**
    * `chunk_size`: Maximum size of each text chunk (default: 1000 characters).
    * `chunk_overlap`: Overlap between consecutive chunks (default: 200 characters).
    * `separators`: A list of strings used to attempt to split the text. Longer, more meaningful separators are tried first (`\n\n\n`, `\n\n`, `\n`, space, then individual characters).

## Dependencies

The project relies on the following Python libraries, listed in `requirements.txt`:

* `azure-storage-blob`
* `azure-ai-documentintelligence`
* `azure-core`
* `azure-ai-textanalytics`
* `langchain-text-splitters`
* `python-dotenv`
* `tiktoken` 

## License

 MIT License
---

Feel free to customize the "Project Title", "Project Description", and any other sections to better reflect your specific project's nuances and goals!
