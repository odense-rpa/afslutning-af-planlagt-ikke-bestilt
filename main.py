import argparse
import asyncio
import logging
import sys
import os

from automation_server_client import AutomationServer, Workqueue, WorkItemError, Credential, WorkItemStatus
from kmd_nexus_client import NexusClientManager
from nexus_database_client import NexusDatabaseClient
from odk_tools.tracking import Tracker
from process.config import load_excel_mapping
from process.nexus_service import NexusService

nexus: NexusClientManager
nexus_database_client: NexusDatabaseClient
nexus_service: NexusService
tracker: Tracker
        
proces_navn = "Afslutning af Planlagt, ikke bestilt"

async def populate_queue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)
    max_retries = 3
    retry_delay = 5  # seconds

    borgere = None
    for attempt in range(max_retries):
        try:
            borgere = nexus_database_client.hent_borgere_med_planlagt_ikke_bestilt_indsatser()
            break  # Success - exit retry loop
        except Exception as e:
            logger.warning(f"Fejl ved hentning af borgere (forsøg {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                logger.error(f"Kunne ikke hente borgere efter {max_retries} forsøg")
                return
    
    if borgere is None:
        return

    for borger in borgere:
        eksisterende_kødata = workqueue.get_item_by_reference(borger["Cpr"], WorkItemStatus.NEW)

        if len(eksisterende_kødata) > 0:
            continue

        workqueue.add_item(
            data={"cpr": borger["Cpr"]},
            reference=borger["Cpr"]
        )


async def process_workqueue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)  

    for item in workqueue:
        with item:
            data = item.data  # Item data deserialized from json as dict
 
            try:
                borger = nexus.borgere.hent_borger(borger_cpr=data["cpr"])

                if not borger:
                    raise WorkItemError(f"Borger med CPR {data['cpr']} ikke fundet i Nexus.")
                
                indsatser = nexus_service.hent_indsatser(borger=borger)
                nexus_service.luk_indsatser_og_bestillinger(indsatser)                
                
            except Exception as e:
                # A WorkItemError represents a soft error that indicates the item should be passed to manual processing or a business logic fault
                logger.error(f"Error processing item: {data}. Error: {e}")
                item.fail(str(e))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO        
    )

    ats = AutomationServer.from_environment()
    workqueue = ats.workqueue()

    nexus_credential = Credential.get_credential("KMD Nexus - produktion")
    nexus_database_credential = Credential.get_credential("KMD Nexus - database")    
    tracking_credential = Credential.get_credential("Odense SQL Server")

    tracker = Tracker(
        username=tracking_credential.username, 
        password=tracking_credential.password
    )

    nexus = NexusClientManager(
        client_id=nexus_credential.username,
        client_secret=nexus_credential.password,
        instance=nexus_credential.data["instance"],
    )    
    
    nexus_database_client = NexusDatabaseClient(
        host = nexus_database_credential.data["hostname"],
        port = nexus_database_credential.data["port"],
        user = nexus_database_credential.username,
        password = nexus_database_credential.password,
        database = nexus_database_credential.data["database_name"],
    )

    nexus_service = NexusService(
        nexus=nexus,
        nexus_database_client=nexus_database_client,
        tracker=tracker
    )

    # Parse command line arguments
    parser = argparse.ArgumentParser(description=proces_navn)
    parser.add_argument(
        "--excel-file",
        default="./Regelsæt.xlsx",
        help="Path to the Excel file containing mapping data (default: ./Regelsæt.xlsx)",
    )
    parser.add_argument(
        "--queue",
        action="store_true",
        help="Populate the queue with test data and exit",
    )
    args = parser.parse_args()

    # Validate Excel file exists
    if not os.path.isfile(args.excel_file):
        raise FileNotFoundError(f"Excel file not found: {args.excel_file}")

    # Load excel mapping data once on startup
    load_excel_mapping(args.excel_file)

    # Queue management
    if "--queue" in sys.argv:
        workqueue.clear_workqueue(WorkItemStatus.NEW)
        asyncio.run(populate_queue(workqueue))
        exit(0)

    # Process workqueue
    asyncio.run(process_workqueue(workqueue))
