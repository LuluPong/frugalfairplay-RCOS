import datetime
import logging

import httpx

import azure.functions as func

class ClubFetcher:
    def __init__(self) -> None:

        pass
    
    def getAllClubs(self):
        page = httpx.get("https://fbref.com/en/comps/Big5/Big-5-European-Leagues-Stats").text
        return page
        


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    allClubs = ClubFetcher().getAllClubs()

    print(allClubs)

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
