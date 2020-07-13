""" googleImport.py

Logic to import a shared (via a Service Account API user) googlesheet into CFFA memory. Some limited post processing is
execute post import, include:
 a) to handle currency values as Decimal128 ready for DB inserts.
 b) handle dates as datetime.datetime
 c) ensure player names are titled (start with capital for forename/surname letter.)

"""

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


class Googlesheet:
    """ googlesheet class.

    Attributes
    ----------

    client : gspread
        gspread object with the connection to the google sheet post authorisation

    transactions : obj
        representing the transaction worksheet data in the google sheet.

    all_games : obj
        representing the games worksheet data in the google sheet.

    summary_worksheet : obj
        representing the summary worksheet data in the google sheet.

    players : `str` : `list`
        list of player names derived from the summary worksheet.

    actual_adjustments : 'dict' : 'list"
        list of dictionaries with player name and adjustment keys derived from the summary table. Adjustments originated
        from a carry over from a older spreadsheet.

    """

    def __init__(self, credential_file, sheet_name, transactions_worksheet, game_worksheet, summary_worksheet):
        """ Constructor for Googlesheet.

        The constructor will connect to the google sheet and download worksheets into memory.

        Parameters
        ----------

        credential_file : str
            Path/filename to the google sheet key file required to access the google sheet online

        sheet_name : str
            Name of the google sheet

        transactions_worksheet : str
            Transaction worksheet name in the google sheet

        game_worksheet : str
            Game worksheet name in the google sheet

        summary_worksheet : str
            Summary worksheet name in the google sheet.

        Returns
        -------

        Not defined.

        """

        # init does not do anything much other than load the google sheet into the object - but using Decimal128
        # for financial figures that have been loaded as float introduces inaccuracies.
        try:
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name(credential_file, scope)
            self.client = gspread.authorize(creds)
        except ValueError:
            logger.critical('ValueError: Unable to access google for download. Check credentials')

        try:
            self.transactions = self.client.open(sheet_name).worksheet(transactions_worksheet).get_all_records(head=1)
            self.all_games = self.client.open(sheet_name).worksheet(game_worksheet).get_all_records(head=1)
            self.summary_worksheet = self.client.open(sheet_name).worksheet(summary_worksheet).get_all_records(head=1)
        except ValueError:
            logger.critical("ValueError: Unable to open requested worksheets from google")

        # Post processing, cleaning up transactions number figures to Decimal128 for better storage
        # also convert date to ISODate for storage and further querying
        for payment in self.transactions:
            try:
                decimal_value = Decimal128(sub(r'[^\d\-.]', '', payment["Amount"]))
                del payment["Amount"]
                payment["Amount"] = decimal_value
            except ValueError:
                logger.critical("Unsupported payment record." + str(payment.get("Date")))

            try:
                transaction_date = datetime.datetime.strptime(payment.get("Date"), '%d-%b-%Y').date()
                del payment["Date"]
                payment["Date"] = datetime.datetime(transaction_date.year, transaction_date.month, transaction_date.day)
            except ValueError:
                logger.error("Bad date in transaction record" + payment.get("Date"))

            # make sure each PlayerName is titled with capital for first name and surname letter using title()
            try:
                titled_player = payment["Player"].title()
                del payment["Player"]
                payment["Player"] = titled_player
            except ValueError:
                logger.error("Document has invalid Player Name and cannot be titled." + payment["Player"])

        # Post processing on games table
        # - convert "Date of Game dd-MON-YYYY" into suitable format for MongoDB
        #    - datetime.date needs converting to datetime.datetime

        for game in self.all_games:
            try:
                game_date = datetime.datetime.strptime(game.get("Date of Game dd-MON-YYYY"), '%d-%b-%Y').date()
                del game["Date of Game dd-MON-YYYY"]
                game["Date of Game dd-MON-YYYY"] = datetime.datetime(game_date.year, game_date.month, game_date.day)
            except ValueError:
                logger.warning("Bad date in game record" + game.get("Timestamp"))

            # - convert "Cost of Game" and "Cost Each" to Decimal128.
            try:
                game_cost = Decimal128(sub(r'[^\d\-.]', '', game["Cost of Game"]))
                cost_each = Decimal128(sub(r'[^\d\-.]', '', game["Cost Each"]))
                del game["Cost of Game"]
                del game["Cost Each"]
                game["Cost of Game"] = game_cost
                game["Cost Each"] = cost_each
            except ValueError:
                logger.warning("Bad costs in game record" + game.get("Timestamp"))

        self.players = []   # this list has to be explicitly populated.
        self.actual_adjustments = []  # this list has to be explicitly populated

    def derive_players(self, row_start, row_end):
        """ derive_players returns the players on the summary sheet based on specified row range. In the spreadsheet
        there are a number rows with invalid logic and this permits the exclusion of this date.

        Parameters

        row_start : int
            The row in the googlesheet of where to start reading summary data

        row_end : int
            The row in the googlesheet of where to stop reading summary data

        Returns
        -------

        players : `str` : `list`
            List of player names valid in the google sheet.
        """

        # uses Summary and specified rows to work out list of players
        player_extract = self.summary_worksheet[row_start:row_end]
        self.players = []
        for player in player_extract:
            try:
                player_name = player.get("Names")
                if str(player_name) != str():
                    self.players.append(player.get("Names"))
            except ValueError:
                logger.warning("Bad player record name encountered:" + player)

        return self.players

    def calc_player_adjustments(self, row_start, row_end):
        """ calc_player_adjustments extracts the adjustment figure for each player ready for db inserts

        Parameters
        ----------

        row_start : int
            The row in the googlesheet of where to start reading adjustment data

        row_end : int
            The row in the googlesheet of where to stop reading adjustment data

        Returns
        -------

        actual_adjustments : `dict` : `list`
            Dict contains player nae and adjustment value with keys "player_name" and 'adjustAmount'.
        """

        adjustment_extract = self.summary_worksheet[row_start:row_end]
        self.actual_adjustments = []
        for player_adjustment in adjustment_extract:
            try:
                player_name = player_adjustment.get("Names")

                if player_name != "":
                    adjust_amount = Decimal128(sub(r'[^\d\-.]', '',
                                                   player_adjustment.get("Money Carry over from 2009")))
                    self.actual_adjustments.append(dict(name=player_name, adjust=adjust_amount))
            except ValueError:
                logger.warning("Bad player record found" + player_adjustment.get("Names"))

        return self.actual_adjustments

    def get_transactions(self):
        """ Simple function to return  transactions

         returns
         -------

         transactions : `dict` : `list`
            List of all transaction data in DB.

         """

        return self.transactions

    def get_games(self):
        """ Simple function to return all game data

         returns
         -------

         transactions : `dict` : `list`
            List of all games data in DB.

         """
        return self.all_games

    def get_summary(self, row_start, row_end):
        """ Simple function to return all summary data between a range of rows.

        Parameters
        ----------

        row_start : int
            The row in the googlesheet of where to start reading summary data

        row_end : int
            The row in the googlesheet of where to stop reading summary data

        Returns
        -------

        summary_worksheet : `dict` : `list`
            List of  summary data in DB.

        """
        return self.summary_worksheet[row_start:row_end]

    def calc_player_list_per_game(self):
        """" When importing data, each game row has a key for each player name. However it is not easy to determine
        who played the game. This logic creates a new key, PlayerList that concatenates all player Names separated
        by commas into the key value

        Returns
        -------

            Result : Boolean
                Currently always returns True. NB: playerList may be set to "Error" if an exception occurs.

        """
        # - add new field containing a coma separated string of players in that game
        validation = ['Win', 'win', 'lose', 'Lose', 'Draw', 'draw', 'no show', 'No Show']

        for game in self.all_games:
            player_list = ""
            try:
                for player in self.players:
                    if game[player] in validation:
                        player_list = player_list + player + ","
            except ValueError:
                logger.error("Problem calculating player list for game", game.Get("Date of Game dd-MON-YYYY"))
                player_list = "Error!"

            game["PlayerList"] = player_list

        return True









