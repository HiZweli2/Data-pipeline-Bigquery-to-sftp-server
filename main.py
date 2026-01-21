import requests
import logging
from google.cloud import bigquery
import datetime
import time
from html.parser import HTMLParser
from config import  bigquery_credentials, moosedesk_creds_dict
import html
import logging
from google.cloud import bigquery
import http
import re
import os
import pandas as pd
import json  # <-- ADD THIS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Path to your service account JSON file
SERVICE_ACCOUNT_FILE = "dna-staging-test.json"


moosedesk_token = moosedesk_creds_dict.get("TOKEN")

@http
def handle_request(request):
    print("Function started")
    logger.info("Function started")
    
    data = main()

    print(data)
    
    print("Function completed")
    logger.info("Function completed")
    return {"message": "function complete."}, 200


class HTMLTextExtractor(HTMLParser):
    """Custom HTML parser to extract clean text from HTML content"""
    def __init__(self):
        super().__init__()
        self.text_parts = []
        
    def handle_data(self, data):
        # Add non-empty text data
        text = data.strip()
        if text:
            self.text_parts.append(text)
    
    def get_text(self):
        # Join all text parts with spaces
        return ' '.join(self.text_parts)

class TicketsToBigQuery:
    def __init__(self, api_base_url, headers, project_id, dataset_id, table_id, credentials_path=None ):
        """
        Initialize the tickets to BigQuery syncer
        """
        self.api_base_url = api_base_url
        self.headers = headers
        
        # Initialize BigQuery client with credentials
        if credentials_path and os.path.exists(credentials_path):
            logger.info(f"Using credentials from: {credentials_path}")
            self.bq_client = bigquery.Client.from_service_account_json(
                credentials_path,
                project=project_id
            )
        else:
            logger.info("Using default credentials (Cloud Function environment)")
            self.bq_client = bigquery.Client(project=project_id)
            
        self.table_ref = f"{project_id}.{dataset_id}.{table_id}"
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
    
    def get_existing_ticket_ids(self):
        """
        Fetch all existing ticket IDs from BigQuery
        """
        query = f"""
            SELECT DISTINCT _id
            FROM `{self.table_ref}`
            WHERE _id IS NOT NULL
        """
        
        try:
            logger.info("Fetching existing ticket IDs from BigQuery...")
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            existing_ids = {row._id for row in results}
            logger.info(f"Found {len(existing_ids)} existing tickets in BigQuery")
            return existing_ids
            
        except Exception as e:
            logger.error(f"Error fetching existing ticket IDs: {e}")
            return set()
    
    def clean_html_description(self, description):
        """
        Extract clean text from HTML description
        """
        if not description:
            return ""
        
        try:
            unescaped = html.unescape(description)
            parser = HTMLTextExtractor()
            parser.feed(unescaped)
            clean_text = parser.get_text()
            clean_text = re.sub(r'\s+', ' ', clean_text).strip()
            logger.debug(f"Cleaned description from {len(description)} to {len(clean_text)} characters")
            return clean_text
            
        except Exception as e:
            logger.warning(f"Error cleaning HTML description: {e}")
            return re.sub(r'\s+', ' ', description).strip()
        
    def fetch_all_ticket_ids(self):
        """
        Fetch all ticket IDs from the filters-all endpoint
        """
        ticket_ids = []
        page = 1
        results_per_page = 100
        
        logger.info("Starting to fetch all ticket IDs...")
        
        while page <= 20:
            url = (
                f"{self.api_base_url}/ticket/filters-all"
                f"?limit={results_per_page}&page={page}"
                f"&query=&customer=&tags=&status=&priority="
                f"&agentObjectId=&agentObjectLabel=&sortBy=createdOn&sortOrder=-1"
                f"&idViewTicket=&type=Ticket&agent=&isTag=false&isTrash=false"
                f"&filterByIndex=Ticket"
            )
            
            try:
                logger.info(f"Fetching page {page}...")
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                tickets = data.get('data', [])
                
                if not tickets:
                    logger.info(f"No more tickets found on page {page}")
                    break
                
                for ticket in tickets:
                    ticket_id = ticket.get('_id')
                    if ticket_id:
                        ticket_ids.append(ticket_id)
                
                logger.info(f"Page {page}: Found {len(tickets)} tickets")
                
                metadata = data.get('metadata', {})
                total_pages = metadata.get('totalPage', 0)
                
                if page >= total_pages:
                    logger.info(f"Reached last page ({total_pages})")
                    break
                
                page += 1
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching page {page}: {e}")
                break
        
        logger.info(f"Total ticket IDs fetched: {len(ticket_ids)}")
        return ticket_ids
    
    def fetch_ticket_details(self, ticket_id):
        """
        Fetch detailed data for a specific ticket
        """
        url = f"{self.api_base_url}/ticket/{ticket_id}"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            return data.get('data')
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching details for ticket {ticket_id}: {e}")
            return None
    
    def transform_ticket_for_bigquery(self, ticket):
        def to_str(value):
            return str(value) if value is not None else None

        raw_description = ticket.get('description', '')
        clean_description = self.clean_html_description(raw_description)

        def email_struct(email_data):
            if not email_data:
                return None
            return {
                'email': email_data.get('email'),
                'name': email_data.get('name')
            }

        # --- Safe handling of stats ---
        stats_value = ticket.get('stats')

        # If it's a string, try to parse JSON
        if isinstance(stats_value, str):
            try:
                stats_value = json.loads(stats_value)
            except Exception:
                stats_value = None

        # Only proceed if it's a dict
        if isinstance(stats_value, dict):
            stats_struct = {
                'resolvedAt': to_str(stats_value.get('resolvedAt')),
                'firstRespondedAt': to_str(stats_value.get('firstRespondedAt'))
            }
        else:
            stats_struct = {
                'resolvedAt': None,
                'firstRespondedAt': None
            }
           
        # --- SAFE TIMESTAMP CONVERSION ---
        def seconds_to_readable(sec):
                    if not sec or sec == 0:
                        return None
                    try:
                        dt = datetime.datetime.fromtimestamp(sec, tz=datetime.timezone.utc)
                        return dt.strftime("%Y-%m-%d %H:%M:%S")   # → 2025-11-04 14:22:33
                    except:
                        return None

        def seconds_to_date_only(sec):
            if not sec or sec == 0:
                return None
            try:
                dt = datetime.datetime.fromtimestamp(sec, tz=datetime.timezone.utc)
                return dt.strftime("%Y-%m-%d")            # → 2025-11-04
            except:
                return None

        # Extract raw values (they are in SECONDS)
        raw_created     = ticket.get('createdTimestamp')
        raw_updated     = ticket.get('updatedTimestamp')
        raw_deleted     = ticket.get('deletedTimestamp')
        raw_last_msg    = ticket.get('lastMessageTimestamp')  # ← this one too!

        # Convert correctly (NO / 1000.0 !)
        created_dt      = seconds_to_readable(raw_created)
        created_date    = seconds_to_date_only(raw_created)
        updated_dt      = seconds_to_readable(raw_updated)
        updated_date    = seconds_to_date_only(raw_updated)
        deleted_dt      = seconds_to_readable(raw_deleted)
        deleted_date    = seconds_to_date_only(raw_deleted)
        last_msg_dt     = seconds_to_readable(raw_last_msg)
        last_msg_date   = seconds_to_date_only(raw_last_msg)



        return {
            '_id': to_str(ticket.get('_id')),
            'createdDatetime': created_dt,           # "2025-11-04 14:22:33"
            'createdDate': created_date,             # "2025-11-04"
            'updatedDatetime': updated_dt,
            'updatedDate': updated_date,
            'deletedDatetime': deleted_dt,
            'deletedDate': deleted_date,
            'lastMessageDatetime': last_msg_dt,      # ← NEW: human readable
            'lastMessageDate': last_msg_date,        # ← NEW: just the date
            'lastMessageTimestamp': str(raw_last_msg) if raw_last_msg else None,
            'createdTimestamp': str(raw_created) if raw_created else None,
            'updatedTimestamp': str(raw_updated) if raw_updated else None,
            'deletedTimestamp': str(raw_deleted) if raw_deleted else None,
             'createdBy': to_str(ticket.get('createdBy')),
            'updatedBy': to_str(ticket.get('updatedBy')),
            'deleted': bool(ticket.get('deleted', False)),  # BOOL
            'deletedBy': to_str(ticket.get('deletedBy')),
            'storeId': to_str(ticket.get('storeId')),
            'incoming': to_str(ticket.get('incoming', False)),
            'subject': to_str(ticket.get('subject')),
            'ticketId': to_str(ticket.get('ticketId')),
            'description': clean_description or None,
            'status': to_str(ticket.get('status')),
            'priority': to_str(ticket.get('priority')),
            'fromEmail': email_struct(ticket.get('fromEmail')),  # STRUCT
            'fromEmailStr': to_str(ticket.get('fromEmailStr')),
            'senderConfigId': to_str(ticket.get('senderConfigId')),
            'customerObjectId': to_str(ticket.get('customerObjectId')),
            'toEmails': [email_struct(e) for e in (ticket.get('toEmails') or [])],  # ARRAY<STRUCT>
            'toEmailStr': to_str(ticket.get('toEmailStr')),
            'ccEmails': to_str(ticket.get('ccEmails', [])),
            'bccEmails': to_str(ticket.get('bccEmails', [])),
            'tags': to_str(ticket.get('tags', [])),
            'attachmentIds': to_str(ticket.get('attachmentIds', [])),
            'discounts': to_str(ticket.get('discounts', [])),
            'sendEmailFailureCount': to_str(ticket.get('sendEmailFailureCount', 0)),
            'createdViaWidget': bool(ticket.get('createdViaWidget', False)),  # BOOL
            'permanentlyDeleted': bool(ticket.get('permanentlyDeleted', False)),  # BOOL
            'meta': json.dumps(ticket.get('meta')) if ticket.get('meta') else None,  # STRUCT
            'isRead': to_str(ticket.get('isRead', False)),
            'channel': to_str(ticket.get('channel')),
            'lastReplyBy': to_str(ticket.get('lastReplyBy')),
            'mailMessageId': to_str(ticket.get('mailMessageId')),
            'agentEmail': to_str(ticket.get('agentEmail')),
            'agentName': to_str(ticket.get('agentName')),
            'agentObjectId': to_str(ticket.get('agentObjectId')),
            'stats': stats_struct, # STRUCT
            'attachments': json.dumps(ticket.get('attachments', [])) if ticket.get('attachments') else None,
            'text': to_str(ticket.get('text', ''))
        }
        
    def upsert_tickets_to_bigquery(self, tickets):
        results = {
            'success': 0,
            'failed': 0,
            'inserted': 0,
            'updated': 0,
            'errors': []
        }

        if not tickets:
            print("[WARNING] No tickets to upsert")
            return results

        print(f"[BIGQUERY] Preparing {len(tickets)} tickets for upload...")

        df = pd.DataFrame(tickets)
        temp_table_id = f"{self.table_id}_temp_{int(time.time())}"
        temp_table_ref = f"{self.project_id}.{self.dataset_id}.{temp_table_id}"

        # ---- 1. Load into a temp table (always works) ----
        print(f"[BIGQUERY] Creating temp table: {temp_table_id}")
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        job = self.bq_client.load_table_from_dataframe(df, temp_table_ref, job_config=job_config)
        job.result()
        print(f"[BIGQUERY] Loaded {len(df)} rows into temp table")

        # ---- 2. Try MERGE – if target missing, create it first ----
        merge_sql = f"""
            MERGE `{self.table_ref}` T
            USING `{temp_table_ref}` S
            ON T._id = S._id
            WHEN MATCHED THEN
            UPDATE SET {', '.join(f"T.{c} = S.{c}" for c in df.columns if c != '_id')}
            WHEN NOT MATCHED THEN
            INSERT ({', '.join(df.columns)})
            VALUES ({', '.join(f"S.{c}" for c in df.columns)})
        """

        try:
            print(f"[BIGQUERY] Running MERGE into {self.table_ref}...")
            query_job = self.bq_client.query(merge_sql)
            query_job.result()
            print(f"[BIGQUERY] MERGE succeeded - {len(tickets)} tickets upserted")
            results['success'] = len(tickets)
        except Exception as e:
            if "Not found: Table" in str(e) and self.table_ref.split(":")[-1] in str(e):
                print(f"[BIGQUERY] Target table missing - creating: {self.table_ref}")
                create_sql = f"""
                    CREATE TABLE `{self.table_ref}` AS
                    SELECT * FROM `{temp_table_ref}` LIMIT 0
                """
                self.bq_client.query(create_sql).result()
                print("[BIGQUERY] Target table created")

                # Now run MERGE again
                print("[BIGQUERY] Retrying MERGE...")
                query_job = self.bq_client.query(merge_sql)
                query_job.result()
                print(f"[BIGQUERY] MERGE succeeded - {len(tickets)} tickets upserted")
                results['success'] = len(tickets)
            else:
                print(f"[ERROR] MERGE failed: {e}")
                raise

        # ---- 3. Cleanup ----
        print("[BIGQUERY] Cleaning up temp table...")
        self.bq_client.delete_table(temp_table_ref, not_found_ok=True)
        return results
    
    def sync_all_tickets(self):
        """
        Main method to sync all tickets from API to BigQuery
        """
        start_time = time.time()
        print("="*60)
        print("Starting ticket sync process with upsert logic...")
        print("="*60)

        existing_ids = self.get_existing_ticket_ids()
        print(f"[INFO] Found {len(existing_ids)} existing tickets in BigQuery")

        api_ticket_ids = self.fetch_all_ticket_ids()
        
        if not api_ticket_ids:
            logger.warning("No tickets found to sync")
            return {'success': 0, 'failed': 0, 'errors': ['No tickets found']}
        
        new_ticket_ids = [tid for tid in api_ticket_ids if tid not in existing_ids]
        existing_ticket_ids = [tid for tid in api_ticket_ids if tid in existing_ids]

        print(f"\n[TICKET ANALYSIS]")
        print(f"  - Total tickets from API: {len(api_ticket_ids)}")
        print(f"  - Already in BigQuery: {len(existing_ticket_ids)}")
        print(f"  - New tickets: {len(new_ticket_ids)}")

        tickets_to_fetch = api_ticket_ids

        print(f"\n[FETCHING] Retrieving details for {len(tickets_to_fetch)} tickets...")
        tickets_data = []
        
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        def fetch_and_transform(ticket_id):
            try:
                ticket_details = self.fetch_ticket_details(ticket_id)
                if ticket_details:
                    transformed_ticket = self.transform_ticket_for_bigquery(ticket_details)
                    if transformed_ticket.get('attachments') == []:
                        transformed_ticket['attachments'] = None
                    return transformed_ticket
                return None
            except Exception as e:
                logger.warning(f"Failed to fetch details for ticket {ticket_id}: {e}")
                return None
        
        max_workers = 10
        print(f"[INFO] Using {max_workers} parallel workers...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_id = {
                executor.submit(fetch_and_transform, ticket_id): ticket_id
                for ticket_id in tickets_to_fetch
            }
            completed = 0
            for future in as_completed(future_to_id):
                completed += 1
                ticket_id = future_to_id[future]
                if completed % 10 == 0:
                    print(f"[PROGRESS] {completed}/{len(tickets_to_fetch)} tickets fetched ({(completed/len(tickets_to_fetch)*100):.1f}%)")
                result = future.result()
                if result:
                    tickets_data.append(result)
                    print(f"  [OK] Ticket {result.get('ticketId', ticket_id)} - {result.get('subject', 'No subject')[:50]}")

        time.sleep(0.5)
        print(f"\n[FETCH COMPLETE] Successfully retrieved {len(tickets_data)} tickets")
        
        print(f"\n[UPLOADING] Upserting {len(tickets_data)} tickets to BigQuery...")
        results = self.upsert_tickets_to_bigquery(tickets_data)

        elapsed_time = time.time() - start_time
        print("\n" + "="*60)
        print("[SYNC COMPLETE]")
        print(f"  Total tickets processed: {len(api_ticket_ids)}")
        print(f"  Successfully upserted: {results['success']}")
        print(f"  Failed: {results['failed']}")
        print(f"  Time elapsed: {elapsed_time:.2f} seconds")
        print("="*60)
        
        return results


def main(request=None, credentials_path=None):
    """
    Main function - works for both Cloud Function and local testing
    """
    try:
        API_BASE_URL = "https://api.moosedesk.com/api/v1"
        
        HEADERS = {
                "Authorization": f"Bearer {moosedesk_token}",
                "Content-Type": "application/json"
            }
        
        PROJECT_ID = "dna-staging-test"
        DATASET_ID = "Veries"
        TABLE_ID = "moosedesk_tickets"
        
        syncer = TicketsToBigQuery(
            api_base_url=API_BASE_URL,
            headers=HEADERS,
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            credentials_path=credentials_path
            
        )
        
        results = syncer.sync_all_tickets()
        
        return {
            'status': 'success',
            'tickets_upserted': results['success'],
            'tickets_failed': results['failed'],
            'errors': results['errors'][:10]
        }, 200
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return {
            'status': 'error',
            'message': str(e)
        }, 500


# if __name__ == "__main__":
#     # Local testing entry point
#     result, status_code = main()
#     print(f"Status: {status_code}")
#     print(f"Result: {result}")
