import datetime
import logging

import httpx
import psycopg2

import azure.functions as func
from bs4 import BeautifulSoup

class ClubFetcher:
    def __init__(self) -> None:
        self.page = httpx.get("https://fbref.com/en/comps/Big5/Big-5-European-Leagues-Stats").text.encode("utf-8")
        self.pageHTML = BeautifulSoup(self.page, 'html.parser')

        self.topLeagues = httpx.get("https://fbref.com/en/comps/Big5/history/Big-5-European-Leagues-Seasons").text.encode("utf-8")
        self.topLeaguesHTML = BeautifulSoup(self.topLeagues, 'html.parser')

        self.dbMainTable = dict()
        pass

    def getTopLeagues(self):
        topLeaguesTableHTML = self.topLeaguesHTML.find(id="seasons").thead.tr

        topLeaguesTableHTML = topLeaguesTableHTML.find_all("th")[1:-1]
        
        leagueKeys = self.dbMainTable.keys()

        for topLeague in topLeaguesTableHTML:
            countryCode = topLeague["data-stat"][:3].encode("utf-8").strip().lower()
            countryLeague = topLeague.get_text()[3:].encode("utf-8").strip()

            if countryCode.lower() not in leagueKeys:
                self.dbMainTable[countryCode] = countryLeague

        return self.dbMainTable
    
    def getAllClubs(self):
        clubTableHTML = self.pageHTML.find(id="big5_table").tbody

        clubRowsHTML = clubTableHTML.find_all("tr")

        #clubRowHTML = clubRowsHTML[0].find_all(attrs={"data-stat":"team"})
        i = 0
        for clubRowHTML in clubRowsHTML:

            clubLeague = clubRowHTML.find_all("span", "f-i")[0].string


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
            





            print(clubLeague)

        

        return "end"
        


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    #allClubs = ClubFetcher().getAllClubs()

    topLeagues = ClubFetcher().getTopLeagues()

    print(topLeagues)

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
