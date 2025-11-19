import argparse
import asyncio
import logging
import sys
import os

from automation_server_client import AutomationServer, Workqueue, WorkItemError, Credential, WorkItemStatus
from datetime import datetime, timezone
from kmd_nexus_client import NexusClientManager
from kmd_nexus_client.tree_helpers import (
    filter_by_path,
    filter_by_predicate
)
from nexus_database_client import NexusDatabaseClient
from odk_tools.tracking import Tracker
from process.config import get_excel_mapping, load_excel_mapping

nexus: NexusClientManager
nexus_database_client: NexusDatabaseClient
tracker: Tracker

proces_navn = "Afslutning af Planlagt, ikke bestilt"

def hent_indsatser(borger: dict) -> list[dict]:
    regler = get_excel_mapping()
    paragraffer = {item.split("|")[0]: item.split("|")[1] for item in regler.get("Paragraffer", [])}

    relevante_indsatser = []

    pathway = nexus.borgere.hent_visning(borger=borger)

    if pathway is None:
        raise ValueError(
            f"Kunne ikke finde -Alt for borger {borger['patientIdentifier']['identifier']}"
        )

    indsats_referencer = nexus.borgere.hent_referencer(visning=pathway)            

    filtrerede_indsats_referencer = filter_by_path(
        indsats_referencer,
        path_pattern="/*/patientPathwayReference/Indsatser/basketGrantReference",
        active_pathways_only=False,
    )

    indsatser = filter_by_predicate(
        roots=filtrerede_indsats_referencer,
        predicate=lambda x: x["workflowState"]["name"] == "Planlagt, ikke bestilt"
    )

    for indsats_reference in indsatser:
        indsats = nexus.hent_fra_reference(indsats_reference)
        felter = nexus.indsatser.hent_indsats_elementer(indsats=indsats)

        # Check om indsats har enddate og om den er overskredet
        if felter["basketGrantEndDate"] is None or felter["basketGrantEndDate"] >= datetime.now(timezone.utc):
            continue

        # Check om indsatsnavn er relevant
        if indsats_reference["name"] not in regler["Indsatsnavne"]:
            paragraf = felter.get("paragraph")

            # Eller check om paragraf + lovgivning er relevant
            if paragraf is None or paragraf["paragraph"]["name"] not in paragraffer:
                continue
            
            if paragraffer[paragraf["paragraph"]["name"]] != paragraf["paragraph"]["section"]:
                continue

        relevante_indsatser.append(indsats)

    return relevante_indsatser

def luk_indsatser_og_bestillinger(indsatser: list[dict]):
    foretrukne_transitioner = ["Bevilg", "Bestil", "Afslut"]

    for indsats in indsatser:        
        tilgængelige_transitioner = indsats.get("currentWorkflowTransitions")
        felter = nexus.indsatser.hent_indsats_elementer(indsats=indsats)

        if indsats["name"] in ["Aktivitet i Huset", "Aktivitet Ude af Huset"]:
            nexus.indsatser.rediger_indsats(indsats=indsats, ændringer={}, overgang="Fjern")
            tracker.track_task(process_name=proces_navn)
            # continue, fordi der ikke er en leverandør bestilling at håndtere derefter
            continue

        elif tilgængelige_transitioner is not None:
            for transition in tilgængelige_transitioner:

                # Genhent indsats for at sikre frisk data                
                indsats = nexus.hent_fra_reference(indsats)
                tilgængelige_transitioner = indsats.get("currentWorkflowTransitions")

                if transition.get("name") in foretrukne_transitioner:                    
                    planlagt_dato = felter.get("plannedDate" if felter else None)
                    slut_dato = felter.get("basketGrantEndDate" if felter else None)
                    
                    if planlagt_dato is None or slut_dato is None:
                        break

                    ændringer = {
                        "orderedDate": planlagt_dato,
                        "workflowApprovedDate": planlagt_dato,
                        "entryDate": planlagt_dato,
                        "billingStartDate": planlagt_dato,
                        "billingEndDate": slut_dato,
                        "repetition": {                            
                            "pattern": "DAY",
                            "count": 1,
                            "weekdays": 1,
                            "weekenddays": 0,
                            "shifts": [
                                {
                                    "title": "Dag"
                                }
                            ]
                        },
                        "resourceCount": 1
                    }
                    
                    nexus.indsatser.rediger_indsats(indsats=indsats, ændringer=ændringer, overgang=transition.get("name"))
                    tracker.track_task(process_name=proces_navn)
        
        leverandør = (
            felter.get("supplier", {}).get("supplier", {}).get("organization")
            if felter else None
        )

        if leverandør is None:
            continue

        organisation = nexus.hent_fra_reference(leverandør)

        if organisation is None:
            continue

        planlægningskalendere = nexus.kalender.hent_planlægningskalendere(organisation=organisation)

        for kalender in planlægningskalendere:
            pass
            # hent kalender
            # hent bestillingssider
            # loop sider
            # hent bestillinger
            # find bestilling med matching indsats reference
            # hent handlinger
            # find planlagt handling
            # udfør handling


        tracker.track_task(process_name=proces_navn)


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
                
                indsatser = hent_indsatser(borger=borger)
                luk_indsatser_og_bestillinger(indsatser)
                
                
            except WorkItemError as e:
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

    tracker = Tracker(
        username=tracking_credential.username, 
        password=tracking_credential.password
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
