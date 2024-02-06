import datetime
import logging

import httpx

import azure.functions as func
from bs4 import BeautifulSoup

class ClubFetcher:
    def __init__(self) -> None:

        pass
    
    def getAllClubs(self):
        page = httpx.get("https://fbref.com/en/comps/Big5/Big-5-European-Leagues-Stats").text.encode("utf-8")
        pageHTML = BeautifulSoup(page, 'html.parser')
        clubTableHTML = pageHTML.find(id="big5_table").tbody

        clubRowsHTML = clubTableHTML.find_all("tr")

        #clubRowHTML = clubRowsHTML[0].find_all(attrs={"data-stat":"team"})
        i = 0
        for clubRowHTML in clubRowsHTML:
            print(i+1)
            clubName = clubRowHTML.find_all(attrs={"data-stat":"team"})[0].a.string.encode("utf-8")
            clubLeagueRank = clubRowHTML.find_all(attrs={"data-stat":"rank"})[0].string.encode("utf-8")
            clubNumGames = clubRowHTML.find_all(attrs={"data-stat":"games"})[0].string.encode("utf-8")
            clubWins = clubRowHTML.find_all(attrs={"data-stat":"wins"})[0].string.encode("utf-8")
            clubTies = clubRowHTML.find_all(attrs={"data-stat":"ties"})[0].string.encode("utf-8")
            clubLies = clubRowHTML.find_all(attrs={"data-stat":"losses"})[0].string.encode("utf-8")
            clubGoalsFor = clubRowHTML.find_all(attrs={"data-stat":"goals_for"})[0].string.encode("utf-8")
            clubGoalsAgainst = clubRowHTML.find_all(attrs={"data-stat":"goals_against"})[0].string.encode("utf-8")
            clubGoalDiff = clubRowHTML.find_all(attrs={"data-stat":"goal_diff"})[0].string.encode("utf-8")
            clubPoints = clubRowHTML.find_all(attrs={"data-stat":"points"})[0].string.encode("utf-8")
            clubPointsAvg = clubRowHTML.find_all(attrs={"data-stat":"points_avg"})[0].string.encode("utf-8")
            clubXgAvg = clubRowHTML.find_all(attrs={"data-stat":"xg_for"})[0].string.encode("utf-8")
            clubXgAgainst = clubRowHTML.find_all(attrs={"data-stat":"xg_against"})[0].string.encode("utf-8")
            clubXgDiff = clubRowHTML.find_all(attrs={"data-stat":"xg_diff"})[0].string.encode("utf-8")
            clubXgDiff90 = clubRowHTML.find_all(attrs={"data-stat":"xg_diff_per90"})[0].string.encode("utf-8")
            clubAttendance90 = clubRowHTML.find_all(attrs={"data-stat":"attendance_per_g"})[0].string.encode("utf-8")
            clubTopScorer = clubRowHTML.find_all(attrs={"data-stat":"top_team_scorers"})[0].a.string.encode("utf-8")
            clubTopScorerNumGoals = clubRowHTML.find_all(attrs={"data-stat":"top_team_scorers"})[0].span.string.encode("utf-8")
            





            print(clubTopScorer)

        

        return "end"
        


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    allClubs = ClubFetcher().getAllClubs()

    print(allClubs)

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
