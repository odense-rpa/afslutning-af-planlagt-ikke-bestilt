from datetime import datetime, timezone
from kmd_nexus_client import NexusClientManager
from kmd_nexus_client.tree_helpers import (
    filter_by_path,
    filter_by_predicate
)
from nexus_database_client import NexusDatabaseClient
from odk_tools.tracking import Tracker
from process.config import get_excel_mapping

proces_navn = "Afslutning af Planlagt, ikke bestilt"


class NexusService:
    def __init__(self, nexus: NexusClientManager, nexus_database_client: NexusDatabaseClient, tracker: Tracker):
        self.nexus = nexus
        self.nexus_database_client = nexus_database_client
        self.tracker = tracker


    def hent_indsatser(self, borger: dict) -> list[dict]:
        regler = get_excel_mapping()
        paragraffer = {item.split("|")[0]: item.split("|")[1] for item in regler.get("Paragraffer", [])}

        relevante_indsatser = []

        pathway = self.nexus.borgere.hent_visning(borger=borger)

        if pathway is None:
            raise ValueError(
                f"Kunne ikke finde -Alt for borger {borger['patientIdentifier']['identifier']}"
            )

        indsats_referencer = self.nexus.borgere.hent_referencer(visning=pathway)            

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
            indsats = self.nexus.hent_fra_reference(indsats_reference)
            felter = self.nexus.indsatser.hent_indsats_elementer(indsats=indsats)

            # Check om indsats har enddate og om den er overskredet
            if felter["basketGrantEndDate"] is None or felter["basketGrantEndDate"] >= datetime.now(timezone.utc):
                continue

            # Check om indsatsnavn er relevant
            if indsats_reference["name"] not in regler["Indsatsnavne"]:
                paragraf = felter.get("paragraph")

                # Eller check om paragraf + lovgivning er relevant
                if paragraf is None or paragraf["paragraph"]["section"] not in paragraffer:
                    continue                

                forventet_lovgivning = paragraffer[paragraf["paragraph"]["section"]]
                lovgivning = paragraf["paragraph"]["name"]
                
                # Check if expected section is contained in actual section (case-insensitive)
                if forventet_lovgivning.lower() != lovgivning.lower():
                    continue

            relevante_indsatser.append(indsats)

        return relevante_indsatser
    
    
    def luk_indsatser_og_bestillinger(self, indsatser: list[dict]):
        for indsats in indsatser:
            if indsats["name"] in ["Aktivitet i Huset", "Aktivitet ude af Huset"]:
                self.nexus.indsatser.rediger_indsats(indsats=indsats, ændringer={}, overgang="Fjern")
                self.tracker.track_task(process_name=proces_navn)
                # continue, fordi der ikke er en leverandør bestilling at håndtere derefter
                continue
            
            # Vigtigt at indsats returneres, da orderGrantId opdateres ved bestilling
            afsluttet_indsats = self.afslut_kompleks_indsats(indsats=indsats)
            self.planlæg_bestilling_i_leverandør_kalender(indsats=afsluttet_indsats)


    def afslut_kompleks_indsats(self, indsats: dict):
        foretrukne_transitioner = ["Bevilg", "Bestil", "Afslut"]
        felter = self.nexus.indsatser.hent_indsats_elementer(indsats=indsats)

        for transition in foretrukne_transitioner:
            current_transitions = indsats.get("currentWorkflowTransitions")

            if current_transitions and transition in [t.get("name") for t in current_transitions]:                    
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
                
                self.nexus.indsatser.rediger_indsats(indsats=indsats, ændringer=ændringer, overgang=transition)
                self.tracker.track_task(process_name=proces_navn)
                indsats = self.nexus.hent_fra_reference(indsats)

        return indsats

    def planlæg_bestilling_i_leverandør_kalender(self, indsats: dict):
        felter = self.nexus.indsatser.hent_indsats_elementer(indsats=indsats)

        leverandør = (
            felter.get("supplier", {}).get("supplier", {}).get("organization")
            if felter else None
        )

        if leverandør is None:
            return

        organisation = self.nexus.organisationer.hent_organisation_ved_navn(leverandør)

        if organisation is None:
            return

        planlægningskalendere = self.nexus.kalender.hent_planlægningskalendere(organisation=organisation)

        if planlægningskalendere is None:
            return

        for kalender in planlægningskalendere:
            kalender = self.nexus.hent_fra_reference(kalender)
            bestillingssider = self.nexus.nexus_client.get(kalender["_links"]["orderGrants"]["href"]).json()

            for side in bestillingssider["pages"]:
                bestillinger = self.nexus.nexus_client.get(side["_links"]["orderGrants"]["href"]).json()
                
                fundet_bestilling = filter_by_predicate(
                    bestillinger, 
                    lambda b: b.get("type") == "order-grant" 
                    and b.get("_links", {}).get("self", {}).get("href", "").endswith(f"/{indsats['currentOrderGrantId']}")
                )

                if len(fundet_bestilling) > 0:
                    bestilling = fundet_bestilling[0]
                    handling = [action for action in bestilling["actions"] if action.get("name") == "Planlagt"]

                    if len(handling) == 0:
                        break                    
                    
                    udfør_handling = handling[0]
                    # Put uden content, da empty json object resulterer i 400 bad request
                    self.nexus.nexus_client.client.put(
                        udfør_handling["_links"]["executeAction"]["href"],
                        content=""
                    )
                    self.tracker.track_partial_task(process_name=proces_navn)
                    break
    