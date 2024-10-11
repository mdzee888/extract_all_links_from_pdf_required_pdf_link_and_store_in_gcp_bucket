import pandas as pd
from gcp_bq_utils import BigQueryUtils
import pandas_gbq
from datetime import datetime
import pdfplumber
import requests
import tempfile
from fake_useragent import UserAgent
import requests
import os
from google.cloud import storage

class PDFLinkExtractor:
    def __init__(self, company_id, company_name, pdf_link, year):
        self.client = BigQueryUtils()  # Initialize BigQuery client
        self.company_id = company_id
        self.company_name = company_name
        self.pdf_link = pdf_link
        self.year = year
        self.sr_pdf_links_table = "provide here bigquery table name"
        self.all_link, self.all_pdf_link = self.extract_links_from_pdf()

    def download_pdf(self):
        """Downloads the PDF from the link and returns the temporary file path."""
        response = requests.get(self.pdf_link)
        response.raise_for_status()  # Raise an error for bad responses

        # Create a temporary file to store the PDF
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
            temp_file.write(response.content)
            return temp_file.name  # Return the path of the temporary file

    # All pdf link extract
    def extract_links_from_pdf(self):
        # Download the PDF and get the temporary file path
        pdf_path = self.download_pdf()
        links = {}

        # Open the PDF
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):  # start=1 for 1-based page numbers
                # Extract the annotations that contain links
                if page.annots:
                    for annot in page.annots:
                        uri = annot.get("uri")
                        if uri:
                            if page_num not in links:
                                links[page_num] = []  # Create a list for the page if not present
                            links[page_num].append(uri)  # Append the link to the list for this page

        # Extract all links into a single list
        all_links = [link for links in links.values() for link in links]
        pdf_links = [link for link in all_links if link.endswith('.pdf')]

        # Cleanup the temporary PDF file
        os.remove(pdf_path)  # Delete the temporary file after extraction

        return links, pdf_links

    # Update or insert data in BigQuery
    def upsert_to_bigquery(self):
        # Create a DataFrame with the new data
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data = {
            "company_id": [self.company_id],
            "company_name": [self.company_name],
            "sr_pdf_link": [str(self.pdf_link)],
            "links_with_pgno": [str(self.all_link)],
            "links_pdf": [str(self.all_pdf_link)],
            "inserted_dt": [current_time],
            "updated_dt": [current_time]
        }
        schema = [
            {'name': 'company_id', 'type': 'STRING'},
            {'name': 'company_name', 'type': 'STRING'},
            {'name': 'sr_pdf_link', 'type': 'STRING'},
            {'name': 'links_with_pgno', 'type': 'STRING'},
            {'name': 'links_pdf', 'type': 'STRING'},
            {'name': 'inserted_dt', 'type': 'TIMESTAMP'},
            {'name': 'updated_dt', 'type': 'TIMESTAMP'}
        ]

        df = pd.DataFrame(data)
        # Ensure datetime columns are correctly typed as datetime64
        df['inserted_dt'] = pd.to_datetime(df['inserted_dt'])
        df['updated_dt'] = pd.to_datetime(df['updated_dt'])

        # Check if the company_id already exists
        query = f"""
        SELECT company_id FROM `{self.sr_pdf_links_table}`
        WHERE company_id = '{self.company_id}'
        """
        existing_company = self.client.execute_qry(query, 'drl')

        if existing_company:
            # If company_id exists, update the row
            update_query = f"""
            UPDATE `{self.sr_pdf_links_table}`
            SET
                company_name = @company_name,
                sr_pdf_link = @sr_pdf_link,
                links_with_pgno = @all_link,
                links_pdf = @all_pdf_link,
                updated_dt = @updated_dt
            WHERE company_id = @company_id
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("company_name", "STRING", self.company_name),
                    bigquery.ScalarQueryParameter("sr_pdf_link", "STRING", str(self.pdf_link)),
                    bigquery.ScalarQueryParameter("all_link", "STRING", str(self.all_link)),
                    bigquery.ScalarQueryParameter("all_pdf_link", "STRING", str(self.all_pdf_link)),
                    bigquery.ScalarQueryParameter("updated_dt", "TIMESTAMP", current_time),
                    bigquery.ScalarQueryParameter("company_id", "STRING", self.company_id)
                ]
            )
            self.client.bq_client.query(update_query, job_config=job_config).result()  # Execute the update query
            print("Data updated for company_id in BigQuery.")
        else:
            # If company_id does not exist, insert the new row
            pandas_gbq.to_gbq(df, self.sr_pdf_links_table, if_exists='append', table_schema=schema)
            print("Data inserted all links in BigQuery.")
    #sr_assessment_pdf upload in gcp
    def upload_sr_to_gcp(self, link, company_name, year):
        try:
            # Sanitize company name: replace spaces with underscores and convert to lowercase
            company_name = company_name.replace(' ', '_').lower()
            # Define the destination path
            destination_blob_name = f"{company_name}/{year}/esg_report/landing/{os.path.basename(link)}"
            # Initialize a GCP storage client
            storage_client = storage.Client()
            # Define the bucket name
            bucket_name = 'sr_organisation_data'
            # Get the bucket
            bucket = storage_client.bucket(bucket_name)
            # Create a blob object
            blob = bucket.blob(destination_blob_name)
            # Check if the blob already exists
            if blob.exists():
                print(f"File already exists at {destination_blob_name}, updating it.")
            # Download the PDF from the link
            headers = {'User-Agent': UserAgent().random}
            response = requests.get(link, headers=headers, allow_redirects=True)
            response.raise_for_status()  # Ensure the request was successful
            # Upload the PDF to GCP
            blob.upload_from_string(response.content, content_type='application/pdf')
            print(f"File uploaded to {destination_blob_name}")
        except requests.exceptions.RequestException as e:
            print(f"Error downloading PDF: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
    # sr_pdf in pdf link download in gcp
    def upload_sr_all_pdf_to_gcp(self, link, company_name, year):
        try:
            # Sanitize company name: replace spaces with underscores and convert to lowercase
            company_name = company_name.replace(' ', '_').lower()
            # Define the destination path
            destination_blob_name = f"{company_name}/{year}/esg_report/sr_in_all_pdf/{os.path.basename(link)}"
            # Initialize a GCP storage client
            storage_client = storage.Client()
            # Define the bucket name
            bucket_name = 'sr_organisation_data'
            # Get the bucket
            bucket = storage_client.bucket(bucket_name)
            # Create a blob object
            blob = bucket.blob(destination_blob_name)
            # Check if the blob already exists
            if blob.exists():
                print(f"File already exists at {destination_blob_name}, updating it.")
            # Download the PDF from the link
            headers = {'User-Agent': UserAgent().random}
            response = requests.get(link, headers=headers, allow_redirects=True)
            response.raise_for_status()  # Ensure the request was successful
            # Upload the PDF to GCP
            blob.upload_from_string(response.content, content_type='application/pdf')
            print(f"File uploaded to {destination_blob_name}")
        except requests.exceptions.RequestException as e:
            print(f"Error downloading PDF: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

    #filter links by company domain name
    def filter_links_by_company(self, all_links, company):
        filter_keyword = company.split()[0].lower()  # Use 'Biocon' for filtering
        ans = {'filter_link': [], 'without_filter': []}  
        for link in all_links:
            if filter_keyword in link.lower():
                ans['filter_link'].append(link)
            else:
                ans['without_filter'].append(link) 
        return ans

    # def uploadsr_to_gcp(self):
    #   self.upload_sr_to_gcp(self.pdf_link, self.company_name, self.year)
    #   filter_dict = self.filter_links_by_company(self.all_pdf_link , self.company_name)
    #   for link in filter_dict['filter_link']:
    #     self.upload_sr_all_pdf_to_gcp(link, self.company_name, self.year)

    def uploadsr_to_gcp(self):
        try:
            # Call method to upload main PDF
            self.upload_sr_to_gcp(self.pdf_link, self.company_name, self.year)
        except Exception as e:
            print(f"Error while uploading main PDF for {self.company_name}: {e}")
        try:
            # Filter links by company name
            # print(len(self.all_pdf_link))
            filter_dict = self.filter_links_by_company(self.all_pdf_link, self.company_name)
        except Exception as e:
            print(f"Error while filtering links for {self.company_name}: {e}")
            filter_dict = {'filter_link': [], 'without': []}  # Ensure it's a valid structure to avoid KeyErrors later
        # Check if filter_link list is empty
        if not filter_dict.get('filter_link'):
            print(f"No filtered links found for {self.company_name}.")
        else:
            # Loop through filtered links, handle exceptions during each upload
            # print(len(filter_dict['filter_link']), type(filter_dict['filter_link']))
            for link in filter_dict['filter_link']:
                try:
                    self.upload_sr_all_pdf_to_gcp(link, self.company_name, self.year)
                except Exception as e:
                    print(f"Error while uploading PDF link {link} for {self.company_name}: {e}")




#Note:  this code run in the same project envirement in gcp or same colab of gcp project..