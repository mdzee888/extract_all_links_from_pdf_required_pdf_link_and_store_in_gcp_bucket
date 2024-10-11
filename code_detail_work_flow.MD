# PDFLinkExtractor

The `PDFLinkExtractor` class automates the process of downloading a PDF, extracting links from it, and uploading data to Google BigQuery and Google Cloud Storage (GCP). This guide explains how the class works step by step.

## Prerequisites
To use this class, ensure you have the following Python packages installed:

```bash
pip install pandas google-cloud-bigquery pandas-gbq pdfplumber requests fake_useragent google-cloud-storage

Class Overview
The PDFLinkExtractor class has the following main responsibilities:


bigquery table structure
field name	    	type	
company_id		    STRING	
company_name		  STRING	
sr_pdf_link		    STRING	
links_with_pgno		STRING	
links_pdf		      STRING	
inserted_dt		    TIMESTAMP	
updated_dt		    TIMESTAMP	

Download a PDF from the web.
Extract links from the PDF.
Insert or update the extracted links into a BigQuery table.
Upload the PDF and extracted links to Google Cloud Storage (GCP).
Libraries Used
pandas: For handling tabular data.
google-cloud-bigquery: To interact with BigQuery.
pandas_gbq: For uploading data to BigQuery.
pdfplumber: To extract content from PDF files.
requests: To download files from the internet.
tempfile: To create temporary files for storing PDFs.
fake_useragent: To generate random user agents for making requests.
os: For file management (e.g., deleting temporary files).
google-cloud-storage: For uploading files to GCP.
Class Initialization
__init__(self, company_id, company_name, pdf_link, year)
The constructor initializes the following:

company_id: Unique identifier for the company (e.g., "123").
company_name: Name of the company (e.g., "Example Corp").
pdf_link: URL of the company's PDF report.
year: Year of the report (e.g., "2023").
Automatically extracts all links from the PDF by calling extract_links_from_pdf.
python
Copy code
extractor = PDFLinkExtractor("123", "Example Corp", "http://example.com/report.pdf", "2023")
Methods
download_pdf()
Downloads the PDF file from the given pdf_link and saves it to a temporary file. Returns the file path for further processing.

extract_links_from_pdf()
Extracts all the links and PDF links from the downloaded PDF. This method:

Opens the PDF.
Finds annotations (which contain links) on each page.
Stores all links found in a list called all_link.
Extracts .pdf links separately in all_pdf_link.
Deletes the temporary file after extraction.
upsert_to_bigquery()
Inserts new data or updates existing data in BigQuery.

If company_id exists: It updates the row.
If company_id does not exist: It inserts a new row.
Uses pandas_gbq to insert data into the BigQuery table.
upload_sr_to_gcp(link, company_name, year)
Uploads the main PDF to Google Cloud Storage (GCP).

Sanitizes the company name (replaces spaces with underscores and converts to lowercase).
Uploads the file to the sr_organisation_data bucket on GCP.
upload_sr_all_pdf_to_gcp(link, company_name, year)
Uploads each extracted PDF link to GCP.

Similar to upload_sr_to_gcp, it sanitizes the company name and uploads PDFs from the extracted links.
filter_links_by_company(all_links, company)
Filters the extracted links by company name.

Input: A list of all links and the company name.
Output: A dictionary with two lists: filter_link (links that contain the company name) and without_filter (links that do not).
uploadsr_to_gcp()
This method:

Uploads the main PDF using upload_sr_to_gcp.
Filters the links using filter_links_by_company.
Uploads only the filtered links using upload_sr_all_pdf_to_gcp.
Example Usage
python
Copy code
# Initialize the PDFLinkExtractor
extractor = PDFLinkExtractor("123", "Example Corp", "http://example.com/report.pdf", "2023")

# Extract links and upload data to BigQuery
extractor.upsert_to_bigquery()

# Upload the PDF and filtered links to Google Cloud Storage
extractor.uploadsr_to_gcp()
Variable Dependencies
company_id, company_name, pdf_link, year: Passed when creating the object. Used throughout the class methods.
sr_pdf_links_table: BigQuery table where the links are stored.
all_link, all_pdf_link: Extracted in the extract_links_from_pdf method and used when uploading data or PDFs.
How It Works Together
Initialize the Class:

extractor = PDFLinkExtractor("123", "Example Corp", "http://example.com/report.pdf", "2023")

When the class is initialized, it automatically downloads and processes the PDF.

Upload Data to BigQuery: Call upsert_to_bigquery to insert or update data in BigQuery.

Upload PDFs to GCP: Call uploadsr_to_gcp to upload the main PDF and filtered PDFs to GCP.

Error Handling
Catches and handles exceptions during PDF downloads, file uploads, and data insertions.
Prints error messages for any issues that occur, providing feedback to the user.
Conclusion
This class automates the extraction of links from PDFs and their subsequent upload to BigQuery and GCP. It ensures that data is up-to-date and efficiently handles PDF processing tasks in an automated manner.
