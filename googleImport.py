import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bson import Decimal128
from re import sub
import datetime
import logging

# logging config
logger = logging.getLogger("mafm_google_import")
logger.setLevel(logging.DEBUG)
# console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatting = logging.Formatter('%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]')
ch.setFormatter(formatting)
logger.addHandler(ch)


import pprint
pp = pprint.PrettyPrinter()

class Googlesheet:

    def __init__(self, credentialFile, sheetName, transactionsWorksheet, gameWorksheet, summaryWorksheet):
        # init does not do anything much other than load the google sheet into the object - but using Decimal128
        # for financial figures that have been loaded as float introduces inaccuracoes.
        try:
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name(credentialFile, scope)
            self.client = gspread.authorize(creds)
        except ValueError:
            logger.critical('ValueError: Unable to access google for download. Check credentials')

        try:
            self.transactions = self.client.open(sheetName).worksheet(transactionsWorksheet).get_all_records(head = 1)
            self.allgames = self.client.open(sheetName).worksheet(gameWorksheet).get_all_records(head = 1)
            self.summaryWorksheet = self.client.open(sheetName).worksheet(summaryWorksheet).get_all_records(head = 1)
        except ValueError:
            logger.critical("ValueError: Unable to open requested worksheets from google")

        # Post processing, cleaning up transactions number figures to Decimal128 for better storage
        # also convert date to ISODate for storage and further querying
        for payment in self.transactions:
            try:
                decimalValue = Decimal128(sub(r'[^\d\-.]', '', payment["Amount"]))
                del payment["Amount"]
                payment["Amount"] = decimalValue
            except ValueError:
                logger.critical ("Unsupported payment record." + str(payment.get("Date")))

            try:
                transactionDate = datetime.datetime.strptime(payment.get("Date"), '%d-%b-%Y').date()
                del payment["Date"]
                payment["Date"] = datetime.datetime(transactionDate.year, transactionDate.month, transactionDate.day)
            except ValueError:
                logger.error("Bad date in transaction record" + payment.get("Date"))

            # make sure each PlayerName is titled with capital for first name and surname letter using title()
            try:
                titledPlayer = payment["Player"].title()
                del payment["Player"]
                payment["Player"] = titledPlayer
            except ValueError:
                logger.error("Document has invalid Player Name and cannot be titled." + payment["Player"])


        # Post processing on games table
        # - convert "Date of Game dd-MON-YYYY" into suitable format for MongoDB
        #    - datetime.date needs converting to datetime.datetime

        for game in self.allgames:
            try:
                gameDate = datetime.datetime.strptime(game.get("Date of Game dd-MON-YYYY"), '%d-%b-%Y').date()
                del game["Date of Game dd-MON-YYYY"]
                game["Date of Game dd-MON-YYYY"] = datetime.datetime(gameDate.year, gameDate.month, gameDate.day)
            except ValueError:
                logger.warning("Bad date in game record" + game.get("Timestamp"))

            # - convert "Cost of Game" and "Cost Each" to Decimal128.
            try:
                gameCost = Decimal128(sub(r'[^\d\-.]', '', game["Cost of Game"]))
                costEach = Decimal128(sub(r'[^\d\-.]', '', game["Cost Each"]))
                del game["Cost of Game"]
                del game["Cost Each"]
                game["Cost of Game"] = gameCost
                game["Cost Each"] = costEach
            except ValueError:
                logger.warning ("Bad costs in game record" +  game.get("Timestamp"))

        self.players = []   # this list has to be explicitly populated.
        self.actualAdjustments = []  # this list has to be explicitly populated

    def derivePlayers(self, rowstart, rowend):
        # uses Summary and specified rows to work out list of players
        playerExtract = self.summaryWorksheet[rowstart:rowend]
        self.players = []
        for player in playerExtract:
            try:
                playerName = player.get("Names")
                if (str(playerName) != str()): self.players.append(player.get("Names"))
            except ValueError:
                logger.warning("Bad player record name encountered:" + player)

        return(self.players)

    def calcPlayerAdjustments(self, rowstart, rowend):
        adjustmentExtract = self.summaryWorksheet[rowstart:rowend]
        self.actualAdjustments = []
        for playerAdjustment in adjustmentExtract:
            try:
                playerName = playerAdjustment.get("Names")

                if (playerName != ""):
                    adjustAmount = Decimal128(sub(r'[^\d\-.]', '', playerAdjustment.get("Money Carry over from 2009")))
                    self.actualAdjustments.append(dict(name = playerName,
                                              adjust = adjustAmount))
            except ValueError:
                logger.warning ("Bad player record found" + playerAdjustment.get("Names"))

        return(self.actualAdjustments)

    def getTransactions(self):
        return(self.transactions)

    def getGames(self):
        return(self.allgames)

    def getSummary(self, rowstart, rowend):
        return(self.summaryWorksheet[rowstart:rowend])

    def calcPlayerListPerGame(self):
        # - add new field containing a coma separated string of players in that game
        validation = ['Win', 'win', 'lose', 'Lose', 'Draw', 'draw', 'no show', 'No Show']

        for game in self.allgames:
            playerList = ""
            try:
                for player in self.players:
                    if game[player] in validation:
                        playerList = playerList + player + ","
            except ValueError:
                logger.error ("Problem calculating player list for game", game.Get("Date of Game dd-MON-YYYY"))
                playerList = "Error!"

            game["PlayerList"] = playerList

        return(True)









