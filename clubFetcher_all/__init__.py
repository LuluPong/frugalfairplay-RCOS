import datetime
import logging

import os

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
        self.dbTeamsTable = dict()


        self.dbName = os.environ["PostgresDBname"]
        self.dbUser = os.environ["PostgresDBuser"]
        self.dbPSWD = os.environ["PostgresDBPWD"]

        pass


    #FROM WEB SCRAPER
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
    
    #FROM WEB SCRAPER
    def getAllClubs(self):
        clubTableHTML = self.pageHTML.find(id="big5_table").tbody

        clubRowsHTML = clubTableHTML.find_all("tr")

        #clubRowHTML = clubRowsHTML[0].find_all(attrs={"data-stat":"team"})
        i = 0
        for clubRowHTML in clubRowsHTML:

            clubLeague = clubRowHTML.find_all("span", "f-i")[0].string


            clubName = clubRowHTML.find_all(attrs={"data-stat":"team"})[0].a.string.encode("utf-8")

            self.dbTeamsTable[clubName] = {
                "LeagueCountry" : clubRowHTML.find_all(attrs={"data-stat":"country"})[0].text[3:],
                "LeagueRank": clubRowHTML.find_all(attrs={"data-stat":"rank"})[0].string.encode("utf-8"),
                "NumberOfGames": clubRowHTML.find_all(attrs={"data-stat":"games"})[0].string.encode("utf-8"),
                "ClubWins" : clubRowHTML.find_all(attrs={"data-stat":"wins"})[0].string.encode("utf-8"),
                "ClubTies" : clubRowHTML.find_all(attrs={"data-stat":"ties"})[0].string.encode("utf-8"),
                "ClubLosses" : clubRowHTML.find_all(attrs={"data-stat":"losses"})[0].string.encode("utf-8"),
                "ClubGoalsFor" : clubRowHTML.find_all(attrs={"data-stat":"goals_for"})[0].string.encode("utf-8"),
                "ClubGoalsAgainst" : clubRowHTML.find_all(attrs={"data-stat":"goals_against"})[0].string.encode("utf-8"),
                "ClubGoalDiff" : clubRowHTML.find_all(attrs={"data-stat":"goal_diff"})[0].string.encode("utf-8"),
                "ClubPoints" : clubRowHTML.find_all(attrs={"data-stat":"points"})[0].string.encode("utf-8"),
                "ClubPointsAvg" : clubRowHTML.find_all(attrs={"data-stat":"points_avg"})[0].string.encode("utf-8"),
                "ClubXgAvg" : clubRowHTML.find_all(attrs={"data-stat":"xg_for"})[0].string.encode("utf-8"),
                "ClubXgAgainst" : clubRowHTML.find_all(attrs={"data-stat":"xg_against"})[0].string.encode("utf-8"),
                "ClubXgDiff" : clubRowHTML.find_all(attrs={"data-stat":"xg_diff"})[0].string.encode("utf-8"),
                "ClubXgDiff90" : clubRowHTML.find_all(attrs={"data-stat":"xg_diff_per90"})[0].string.encode("utf-8"),
                "ClubAttendance90" : clubRowHTML.find_all(attrs={"data-stat":"attendance_per_g"})[0].string.encode("utf-8"),
                "ClubTopScorer" : clubRowHTML.find_all(attrs={"data-stat":"top_team_scorers"})[0].a.string.encode("utf-8"),
                "ClubTopScorerNumGoals" : clubRowHTML.find_all(attrs={"data-stat":"top_team_scorers"})[0].span.string.encode("utf-8")
            }

            #print(clubLeague)

        

        return self.dbTeamsTable
    
    def putLeaguesInDB(self):
        try:
            #CONNECTING TO DB
            conn = psycopg2.connect(dbname=self.dbName, user=self.dbUser, password=self.dbPSWD)
            cur = conn.cursor()


        except psycopg2.Error as e:
            return(e)
        

        primaryColumn = "Country Code"
        cur.execute(f'SELECT "{primaryColumn}" FROM topfiveleagues') #THERE'S AN ISSUE WITH THIS 
        currentLeaguesFromDB = cur.fetchall()

        #FLATTENING REQUIRED TO PUT VALUES INTO A NORMAL LIST INSTEAD OF EACH VALUE BEING IN ITS OWN TUPLE
        flattenedCLFDB = [item for sublist in currentLeaguesFromDB for item in sublist]

        #print(self.dbMainTable)
        #print(flattenedCLFDB)

        for countryCode in self.dbMainTable.keys():
            #TEST FOR NONEXISTENT COUNTRY CODES
            #TEST FOR EXISTING COUNTRY CODES BUT DIFFERENT LEAGUE NAMES (DO NOT FIX; EMAIL OR SEND NOTIFICATION OF POTENTIAL MISMATCH)
            #TEST FOR EXISTING COUNTRY CODES IN DB THAT ARE NOT PICKED UP BY WEB SCRAPER (DO NOT FIX; EMAIL OR SEND NOTIFICATION OF POTENTIAL MISMATCH)

            #DECODE NECESSARY TO COMPARE BYTE STRING TO REGUAR STRING eg (countryCode) b'eng' == 'eng' (DB Country Code)
            #print(countryCode)
            if not (countryCode.decode("utf-8") in flattenedCLFDB):
                #print("test successful")
                try:
                    cur.execute("INSERT INTO topfiveleagues VALUES (%s, %s)", [self.dbMainTable[countryCode].decode("utf-8"), countryCode.decode("utf-8")])
                    conn.commit()
                except psycopg2.Error as e:
                    print(e)
                    conn.rollback()
            else:
                print("Country code found")
                #IF COUNTRY CODE EXISTS BUT VALUE DOES NOT MATCH CURRENT FBREF VALUE, UPDATE VALUE WITH FBREF VALUE

        conn.close()

        return "IT'S WORKING???"
    


    def updateTeamsInDB(self):
        try:
            #CONNECTING TO DB
            conn = psycopg2.connect(dbname=self.dbName, user=self.dbUser, password=self.dbPSWD)
            cur = conn.cursor()


        except psycopg2.Error as e:
            return(e)
        
        primaryColumn = "team"
        cur.execute(f'SELECT "{primaryColumn}" FROM teamstable') #THERE'S AN ISSUE WITH THIS 
        currentTeamsFromDB = cur.fetchall()

        #FLATTENING REQUIRED TO PUT VALUES INTO A NORMAL LIST INSTEAD OF EACH VALUE BEING IN ITS OWN TUPLE
        flattenedCTFDB = [item for sublist in currentTeamsFromDB for item in sublist]

        for team in self.dbTeamsTable:
            #print(team)
            #print(team.keys())
            #print(self.dbTeamsTable[team]["ClubTopScorer"])
            
            print("\n")
            
            if not (team.decode("utf-8") in flattenedCTFDB):
                
                try:
                    a = "TEST"
                    cur.execute("""INSERT INTO teamstable (team, 
                                "League Rank", 
                                "League Country", 
                                "Num of League Games Played",
                                "League Wins",
                                "League Ties",
                                "League Losses",
                                "League Num of Goals For",
                                "League Num of Goals Against",
                                "League Goal Difference",
                                "League Points",
                                "Avg League Points per Match",
                                "League XG Avg",
                                "League XG Against",
                                "League XG Difference",
                                "League XG Difference per 90",
                                "League Attendance per 90",
                                "League Top Scorer",
                                "Top Scorer Num of Goals") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (team) DO UPDATE SET
                                team = EXCLUDED.team, 
                                League Rank = EXCLUDED."League Rank", 
                                League Country = EXCLUDED."League Country", 
                                Num of League Games Played = EXCLUDED."Num of League Games Played",
                                League Wins = EXCLUDED."League Wins",
                                League Ties = EXCLUDED."League Ties",
                                League Losses = EXCLUDED."League Losses",
                                League Num of Goals For = EXCLUDED."League Num of Goals For",
                                League Num of Goals Against = EXCLUDED."League Num of Goals Against",
                                League Goal Difference = EXCLUDED."League Goal Difference",
                                League Points = EXCLUDED."League Points",
                                Avg League Points per Match = EXCLUDED."Avg League Points per Match",
                                League XG Avg = EXCLUDED."League XG Avg",
                                League XG Against = EXCLUDED."League XG Against",
                                League XG Difference = EXCLUDED."League XG Difference",
                                League XG Difference per 90 = EXCLUDED."League XG Difference per 90",
                                League Attendance per 90 = EXCLUDED."League Attendance per 90",
                                League Top Scorer = EXCLUDED."League Top Scorer",
                                Top Scorer Num of Goals = EXCLUDED."Top Scorer Num of Goals" """, \
                                [team.decode('utf-8'), 
                                 self.dbTeamsTable[team]["LeagueRank"].decode('utf-8'),
                                 self.dbTeamsTable[team]["LeagueCountry"].strip(), 
                                 self.dbTeamsTable[team]["NumberOfGames"].decode('utf-8'),
                                 self.dbTeamsTable[team]["ClubWins"].decode('utf-8'),
                                 self.dbTeamsTable[team]["ClubTies"].decode('utf-8'),
                                 self.dbTeamsTable[team]["ClubLosses"].decode('utf-8'),
                                 self.dbTeamsTable[team]["ClubGoalsFor"].decode('utf-8'),
                                 self.dbTeamsTable[team]["ClubGoalsAgainst"].decode('utf-8'),
                                 self.dbTeamsTable[team]["ClubGoalDiff"].decode('utf-8'),
                                 self.dbTeamsTable[team]["ClubPoints"].decode('utf-8'),
                                 self.dbTeamsTable[team]["ClubPointsAvg"].decode('utf-8'),
                                 self.dbTeamsTable[team]["ClubXgAvg"].decode('utf-8'),
                                 self.dbTeamsTable[team]["ClubXgAgainst"].decode('utf-8'),
                                 self.dbTeamsTable[team]["ClubXgDiff"].decode('utf-8'),
                                 self.dbTeamsTable[team]["ClubXgDiff90"].decode('utf-8'), 
                                 self.dbTeamsTable[team]["ClubAttendance90"].decode('utf-8'), 
                                 self.dbTeamsTable[team]["ClubTopScorer"].decode('utf-8'), 
                                 self.dbTeamsTable[team]["ClubTopScorerNumGoals"].decode('utf-8')])
                    conn.commit()
                except psycopg2.Error as e:
                    print(e)
                    conn.rollback()



        conn.close()
        pass
        
    

        


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    aggregateInfo = ClubFetcher()

    topLeagues = aggregateInfo.getTopLeagues()

    insertNewLeagues = aggregateInfo.putLeaguesInDB()

    allTeams = aggregateInfo.getAllClubs()

    aggregateInfo.updateTeamsInDB()

    #print(allTeams)

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
