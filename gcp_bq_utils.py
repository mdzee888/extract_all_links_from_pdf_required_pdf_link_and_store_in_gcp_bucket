from google.cloud import bigquery
import traceback

class BigQueryUtils:
    def __init__(self) :
        self.bq_client = bigquery.Client()
    
    def execute_qry(self,qry,qry_typ):
        results=[]
        try:
            query_job = self.bq_client.query(qry)
        
            if qry_typ == 'drl':
                for row in query_job:
                    results.append(row)
                return results
        except Exception as e:
            print(e)
            print(traceback.format_exc())