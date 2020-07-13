""" dbinterface.py

Provides all functionality to manage data with mongoDB

# DB collections are
  MultiTenancy - data relating to user/role and tenancy relationships
  ID_teamSummary  (each player and summary data including balances)
  ID_games  (list of every game detail)
  ID_payment (financial transfers)
  ID_teamPlayers (list of players in the team
  ID_teamSettings  (team name, and list of player names)
  ID_adjustments (adjustment for each player - applies to google sheet imports only)

  where
    ID is a hash (based on time of creation) stored in the MultiTenancy collection for the user

"""

import pymongo
import datetime
from bson import Decimal128
from cffadb import footballClasses
import re
import logging
import pprint

pp = pprint.PrettyPrinter()

# logging config
logger = logging.getLogger("cffa_db")
logger.setLevel(logging.DEBUG)
# console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatting = logging.Formatter('%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]')
ch.setFormatter(formatting)
logger.addHandler(ch)

# config variable
activeDays = 730  # players that haven't played for these days are excluded from default list of players
daysForRecentPayment = 180  # cut off for recent payments/transactions when viewed.

# DB needs to know about each of the above objects to store it but not import
aggCollation = pymongo.collation.Collation(locale='en', strength=1, alternate='shifted')


class FootballDB:
    """ FootballDB class - methods cover all DB transactions

    Attributes
    ----------

    theDB : db_session
        Connection to the database

    tenancy : collection
        Global MultiTenancy Collection handle

    payments : collection
        Payments collection handle for this tenancy.

     games : collection
        Games collection handle for this tenancy.

    adjustments : collection
        Adjustments collection handle for this tenancy.

    teamSummary : collection
        TeamSummary collection handle for this tenancy.

    teamPlayers : collection
        TeamPlayers collection handle for this tenancy.

    """

    def __init__(self, connect_string, db_name):
        """ Constructor for the database connection

        Parameters
        ----------

        connect_string : str
            URI for the MongoDB connection

        db_name : str
            Database name in the db server continuing the football data.

        """
        try:
            db_client = pymongo.MongoClient(connect_string)
        except (pymongo.errors.ConnectionFailure, pymongo.errors.InvalidURI):
            logger.critical("Unable to connect to " + connect_string + db_name)
        except Exception as e:
            logger.critical(
                "Issue connecting to DB: " + connect_string + " " + db_name + " " + getattr(e, 'message', repr(e)))

        try:
            self.theDB = db_client[db_name]
        except pymongo.errors.ConnectionFailure:
            logger.critical("Invalid database name provided:" + db_name + " ")
        except Exception as e:
            logger.critical("Issue with DB on startup:" + db_name + " " + getattr(e, 'message', repr(e)))

        try:
            self.tenancy = self.theDB["MultiTenancy"]

        except pymongo.errors.CollectionInvalid:
            logger.critical("Missing footballDB collections encountered - check.")
        except Exception as e:
            logger.critical("Issue getting collections in footballDB. " + getattr(e, 'message', repr(e)))

        if list(self.tenancy.find()) is None:
            logger.warning("Tenancy collection is empty. Fault or new deployment of service")

    def load_team_tables_for_user_id(self, user_id):
        """ Called during login to set up which tenant collections to use

        Parameters
        ----------

        user_id : str
            Auth0 user ID - must start with auth0|

        Returns
        -------

        Status : boolean
            True: if the tenancy collections exist
            False: if the tenancy collections do not exist or there is a fault

        """

        # look up tenancies for the user_id, and map onto the default tenancyID.
        if user_id is None:
            return False  # no metadata for this user so new session needed. redirect to onboarding

        try:
            team = self.tenancy.find_one({"$and": [{"userID": user_id, "default": True}]}, {"tenancyID": 1})
            if team is None:
                logger.warning("User ID " + str(user_id) + "has no tenancies. May be new user")
                return False

            payments_collection = team.get("tenancyID") + "_payments"

            self.payments = self.theDB[payments_collection]
            self.games = self.theDB[team.get("tenancyID") + "_games"]
            self.adjustments = self.theDB[
                team.get("tenancyID") + "_adjustments"]  # unused for non-google imported accounts if ever supported
            self.teamSummary = self.theDB[team.get("tenancyID") + "_teamSummary"]
            self.teamPlayers = self.theDB[team.get("tenancyID") + "_teamPlayers"]
            self.teamSettings = self.theDB[team.get("tenancyID") + "_teamSettings"]
        except pymongo.errors.PyMongoError as e:
            logger.critical("Unable to load and initialise tenancy data")
            return False

        return True

    def add_team(self, team_name, user_id, user_name):
        """ Logic to add the team name into the tenancy collection from the web form

        Parameters
        ----------

        team_name : str
            Team Name of the tenancy

        user_id : str
            Auth0 user ID

        user_name : str
            User name of manager. If the manager will be a player this should be the same name.

        returns
        -------

        message : str
            Message to display on the web page post action.

        """
        # first check if team exists, it should not as form should have rejected it. Add default key as well as team
        # name to the collection
        if user_id is not None:
            if self.tenancy.find_one({"teamName": team_name}) is not None:
                message = "Team name " + team_name + " already exists. Re-enter tem name from Settings"
                logger.warning(message)
            else:
                self.tenancy.insert(dict(
                    userName=user_name,
                    userID=user_id,
                    tenancyID=hex(int(datetime.datetime.now().timestamp() * 1000))[2:],
                    teamName=team_name,
                    userType="Manager",
                    revoked=False,
                    default=True))

                # load user collections so we can append teamSettings
                self.load_team_tables_for_user_id(user_id)
                first_setting = dict(teamName=team_name)
                settings = []
                settings.append(first_setting)
                self.populate_team_settings(settings)
                message = "Team " + team_name + " configured. Please add new players"
                logger.info("Team " + team_name + " configured on tenancy collection for user" + user_id)
        else:
            logger.warning("add_team did not add team when user_id is not set")
            message = "Internal error. Could not add team"

        return message

    def get_list_of_all_tenant_names(self):
        """ Logic to get all the tenants in the DB

        returns
        -------

        teams : `dict` : `list`
            List of dictionaries of the tenancy collection.

        """

        teams = []
        try:
            teams = list(self.tenancy.find({}, {"_id": 0, "teamName": 1}))
            if len(teams) == 0:
                logger.info("get_list_of_all_tenant_names(): Teams list is empty")
            else:
                logger.info("Teams list is ", teams.join(','))
        except Exception as e:
            logger.critical("Could not execute find on tenancy collection" + getattr(e, 'message', repr(e)))

        return teams

    def populate_payments(self, payment_history):
        """ Logic to add all transactions into the payments collections. This function drops all existing payments.

        Parameters
        ----------

        payment_history : `dict` : `list`
            List of dictionaries containing payment data.

        """
        # payments should be a list of dicts for each record

        self.payments.drop()
        logger.info("Dropping payments collection in populate_payments()")
        try:
            self.payments.insert_many(payment_history)
        except pymongo.errors.OperationFailure as e:
            logger.critical("Unable to insert data into Payments table in populate_payments()")
            logger.critical(e.code + e.details)

    def populate_games(self, played_games):
        """ Logic to add all games into the games collection. This function drops all existing games.

        Parameters
        ----------

        played_games : `dict` : `list`
            List of dictionaries containing game data.

        """
        # games should be a list of dicts for each record. This call replaces existing data.
        self.games.drop()
        logger.info("Dropping games collection in populate_games()")
        try:
            self.games.insert_many(played_games)
        except pymongo.errors.OperationFailure as e:
            logger.critical("Unable to insert data into Games collection")
            logger.critical(e.code + e.details)

    def populate_adjustments(self, new_adjustments):
        """ Logic to add all adjustments into the adjustment collection. This function drops all existing adjustments.

        Parameters
        ----------

        new_adjustments : `dict` : `list`
            List of dictionaries containing adjustment data.

        """

        self.adjustments.drop()
        logger.info("Dropped adjustments collection in populate_adjustments()")
        try:
            self.adjustments.insert_many(new_adjustments)
        except pymongo.errors.OperationFailure as e:
            logger.critical("Unable to insert data into Adjustments")
            logger.critical(e.code, e.details)

    def get_all_adjustments(self):
        """ Logic to get all adjustments. AS adjustments collection obj is not restricted this fn may not have value.

        Returns
        -------

        adjustments : `dict` : `list`
            List of dictionaries containing adjustment data.

        """
        try:
            adjustments = list(self.adjustments.find({}))
            return adjustments
        except pymongo.errors.OperationFailure as e:
            logger.critical("Unable to insert data into Adjustments")
            logger.critical(e.code, e.details)

        return None

    def calc_populate_team_summary(self, players):
        """ Logic to calculate key stats in the summary including cost of all games, balance and aggregated transaction
        values. This function drops all existing team summary data..

        Parameters
        ----------

        players : `str` : `list`
            List of strings for each player name.

        """

        self.teamSummary.drop()
        logger.info("Dropped teamSummary collection in calc_populate_team_summary()")
        team = []
        aggregated_payments = self.get_aggregated_payments()

        for player in players:
            total_cost = 0
            games_played = 0

            try:
                for x in self.games.find({player: {"$in": ["Win", "Lose", "Draw", "No Show"]}}, collation=aggCollation):
                    game_cost = float(x.get("Cost of Game").to_decimal()) / float(x.get("Players"))
                    total_cost = total_cost + game_cost
                    games_played += 1
            except pymongo.errors.OperationFailure as e:
                logger.error("Unable to process game query for player")
                logger.error(e.code + e.details)

            # 8th June 2020 - now check player_guest key games and add up guest costs
            try:
                player_guests = player + "_guests"
                for x in self.games.find({player_guests: {"$exists": 1}}, collation=aggCollation):
                    game_cost = float(x.get("Cost of Game").to_decimal()) / float(x.get("Players"))
                    total_cost = total_cost + (x.get(player_guests) * game_cost)
            except pymongo.errors.OperationFailure as e:
                logger.error("Unable to process player_guests query for player")
                logger.error(e.code + e.details)

            try:
                try:
                    adjust_amount = self.adjustments.find_one({"name": player}, {"_id": 0, "adjust": 1})["adjust"]
                except TypeError:
                    # Player does not have a adjustment listed, so default to 0
                    adjust_amount = Decimal128("0.00")
            except pymongo.errors.OperationFailure as e:
                logger.error("Problem with adjustment amount for player " + player)
                logger.error(e.code + e.details)

            # work out last played date via games "Date of Game dd-MMM-YYYY"
            try:
                last_played_date = list(self.games.find({player: {"$in": ["Win", "Lose", "Draw", "No Show"]}},
                                                        {"_id": 0, "Date of Game dd-MON-YYYY": 1},
                                                        collation=aggCollation)
                                        .sort("Date of Game dd-MON-YYYY", -1).limit(1))
                if len(last_played_date) == 0:
                    last_played_date.append({"Date of Game dd-MON-YYYY": datetime.datetime(1970, 1, 1, 0, 0)})
            except ValueError:
                logger.warning("Unable to find last played date for player" + player)
                last_played_date = []
                last_played_date.append({"Date of Game dd-MON-YYYY": datetime.datetime(1970, 1, 1, 0, 0)})
                # last_played_date will be a list of a single element of datetime.datetime
                logger.warning(pp.pprint(last_played_date))

            # the player may have never made any payments so provide default for aggregated_payments for the player
            # key if it doesn't exist
            try:
                team.append(dict(playerName=player,
                                 gamesAttended=games_played,
                                 lastPlayed=last_played_date[0].get("Date of Game dd-MON-YYYY"),
                                 gamesCost=Decimal128(str(total_cost)),
                                 moniespaid=aggregated_payments.get(player, Decimal128("0.00")),
                                 balance=Decimal128(str(float(aggregated_payments.get(player,
                                                                                      Decimal128(
                                                                                          "0.00")).to_decimal()) -
                                                        total_cost + float(adjust_amount.to_decimal())))
                                 )
                            )
            except pymongo.errors.OperationFailure as e:
                logger.error("Problem with a player when adding their summary:")
                logger.error(e.code + e.details)
        try:
            self.teamSummary.insert_many(team)
        except pymongo.errors.OperationFailure as e:
            logger.error("Problem with inserting teamSummary in DB")
            logger.error(e.code + e.details)

    def populate_team_players(self, players):
        """ Logic to write team player names into the DB . This function drops all existing team player data..

        Parameters
        ----------

        players : `dict` : `list`
            Player details : key : value -> (playerName : str , retiree : boolean, comment : str)

        """
        # playerName, comment
        self.teamPlayers.drop()
        try:
            self.teamPlayers.insert_many(players)
        except pymongo.errors.OperationFailure as e:
            logger.critical("Unable to insert players into teamPlayers collection")
            logger.critical(e.code + e.details)

    def populate_team_settings(self, settings):
        """ Logic to write CFFA settings into the DB . This function drops all existing setting data.

        Parameters
        ----------

        settings : `dict` : `list`
            Currently only teamName key is implemented.

        """
        # TeamName
        self.teamSettings.drop()
        try:
            self.teamSettings.insert_many(settings)
        except pymongo.errors.OperationFailure as e:
            logger.critical("Unable to insert settings into teamSettings collection")
            logger.critical(e.code + e.details)

    def player_exists(self, player_name):
        """ Logic to check if player exists in teamSummary.

          Parameters
          ----------

          player_name : str
              player_name identifier.

          Returns
          -------

          result : boolean
            True if player exists, False if not.

          """
        # check if player name exists, return true/false
        player = self.teamSummary.find_one({"playerName": player_name})
        if player is not None:
            if player.get("playerName", None) == player_name:
                return True
        return False

    def add_player(self, player):
        """ Logic to write add a player into the DB if unique name. This is a bit ugly has both TeamPlayers and
        TeamSummary require updates (but MongoDB isn't relational...).

          Parameters
          ----------

          player : footballClasses.Player

          Returns
          -------

          message : str
            Message to show if action succeeded or not.

          """
        # append new document to summary table if player.name is unique
        all_players = self.get_player_labels()
        if player.playername in all_players:
            # player already exists
            message = "Player " + str(player.playername) + " already exists!"
            return message

        self.teamPlayers.insert(dict(
            playerName=player.playername,
            comment=player.comment,
            retiree=player.retiree))

        self.teamSummary.insert(dict(
            playerName=player.playername,
            gamesAttended=0,
            lastPlayed=datetime.datetime(1970, 1, 1, 0, 0),
            gamesCost=Decimal128("0.00"),
            moniespaid=Decimal128("0.00"),
            balance=Decimal128("0.00")))

        message = "Player " + str(player.playername) + " added to System!"
        logger.info(message)
        return message

    def edit_player(self, old_player_name, player):
        """ Logic to edit a player into the DB. This is a bit ugly has both TeamPlayers and TeamSummary require
        updates (but MongoDB isn't relational..). If the name is changed we need to update all collections.

          Parameters
          ----------

          old_player_name : str
            This is the key to the existing player that has been edited. As edit supports name change we need to know
            if the name has changed.

          player : footballClasses.Player
            Player is the new player details from the edit player form.

          Returns
          -------

          message : str
            Message to show if action succeeded or not.
          """
        #  player is a footballClass.
        # if name change go through every Game + TeamSummary + TeamPlayers + transactions + adjustments and
        # update every key. Not nice.

        # title the new player name to make sure we are consistent.
        titled_player_name = player.playername.title()
        player.playername = titled_player_name

        if old_player_name != player.playername:
            our_games = self.games.find({})
            for game in our_games:
                if old_player_name in game:
                    value = game.get(old_player_name)
                    self.games.update_one({"_id": game.get("_id")},
                                          {"$unset": {old_player_name: ""}})
                    logger.debug("Removed player" + old_player_name + " from game " + str(game.get("_id")))
                    self.games.update_one({"_id": game.get("_id")},
                                          {"$set": {player.playername: value}})
                    logger.debug("Added player" + player.playername + " to game " + str(game.get("_id")))

                guest = old_player_name + "_guests"
                if guest in game:
                    new_guest = player.playername + "_guests"
                    value = game.get(guest)
                    self.games.update_one({"_id": game.get("_id")},
                                          {"$unset": {guest: ""}})
                    logger.debug("Removed guests for " + guest + " with id" + str(game.get("_id")))
                    self.games.update_one({"_id": game.get("_id")},
                                          {"$set": {new_guest: value}})
                    logger.debug("Added guest for " + new_guest + "with game id " + str(game.get("_id")))

                # done - need to separate into comma separate values, then check and rebuild as Mark and Mark D
                # will clash
                new_player_list = game.get("PlayerList").replace(old_player_name, player.playername)
                if new_player_list != game.get("PlayerList"):
                    player_name_list = game.get("PlayerList").split(',')
                    player_name_list = [player.playername if w == old_player_name else w for w in player_name_list]

                    guest = old_player_name + "_guests"
                    new_guest = player.playername + "_guests"
                    player_name_list = [new_guest if w == guest else w for w in player_name_list]

                    self.games.update_one({"_id": game.get("_id")},
                                          {"$set": {"PlayerList": ','.join(player_name_list)}})
                    logger.info("PlayerList modded for old_player_name" + old_player_name + str(game.get("_id")))

            # now teamSummary
            team = self.teamSummary.find({})
            for our_player in team:
                if old_player_name == our_player.get("playerName", "None"):
                    self.teamSummary.update_one({"_id": our_player.get("_id")},
                                                {"$set": {"playerName": player.playername}})
                    logger.debug("Updated teamSummary" + str(our_player.get(
                        "_id")) + "for player " + old_player_name + " and changed name to " + player.playername)

            # now transactions
            transactions = self.payments.find({})
            for transaction in transactions:
                if old_player_name == transaction.get("Player", "None"):
                    self.payments.update_one({"_id": transaction.get("_id")}, {"$set": {"Player": player.playername}})
                    logger.debug("Updated transaction " + str(transaction.get(
                        "_id")) + " for player " + old_player_name + " and changed name to " + player.playername)

            # now teamPlayers
            team = self.teamPlayers.find({})
            for our_player in team:
                if old_player_name == our_player.get("playerName", "None"):
                    self.teamPlayers.update_one({"_id": our_player.get("_id")},
                                                {"$set": {"playerName": player.playername}})
                    logger.debug("Updated teamPlayer " + str(our_player.get(
                        "_id")) + " for player " + old_player_name + " and changed name to " + player.playername)

            message = "Updated CFFA database from " + old_player_name + " to " + player.playername + "!"
        else:
            message = "Updated player " + player.playername + " details"

        # in all cases we update retiree and comment with whatever is passed - doesn't matter to check if they have
        # actually changed.
        self.teamPlayers.update_one({"playerName": player.playername}, {"$set":
                                                                            {"retiree": player.retiree,
                                                                             "comment": player.comment
                                                                             }})

        logger.info(message)
        return message

    def retire_player(self, player_name):
        """ Logic to edit a retire a player

          Parameters
          ----------

          player_name : str
            This is the key to the  player that will be retired.. A

          Returns
          -------

          message : str
            Message to show if action succeeded or not.
          """
        try:
            self.teamPlayers.update_one({"playerName": player_name}, {"$set": {"retiree": True}})
        except pymongo.errors.OperationFailure as e:
            logger.critical("Unable to reactivate player in teamPlayers " + player_name)
            logger.critical(e.code + e.details)
            message = "Could not retire player " + player_name
            logger.info(message)
            return message

        message = "Retired player " + player_name
        logger.info(message)
        return message

    def reactivate_player(self, player_name):
        """ Logic to edit a reactivate a player

          Parameters
          ----------

          player_name : str
            This is the key to the player that will be reactivated..

          Returns
          -------

          message : str
            Message to show if action succeeded or not.
          """
        try:
            self.teamPlayers.update_one({"playerName": player_name}, {"$set": {"retiree": False}})
        except pymongo.errors.OperationFailure as e:
            logger.critical("Unable to reactivate player in teamPlayers " + player_name)
            logger.critical(e.code + e.details)
            message = "Could not reactivate player " + player_name
            logger.info(message)
            return message

        message = "Reactivated player " + player_name
        logger.info(message)
        return message

    def add_game(self, new_game):
        """ Logic to edit a insert a new game and update each player/booker TeamSummary data.

          Parameters
          ----------

          new_game : footballClasses.Game
            Game details to be inserted.

          Returns
          -------

          result : boolean
            True if successfully, false if fails.
          """
        # adds new game to DB
        logger.debug("We got to add_game()!")
        # new_game is a footballClasses game

        # append new game into game collection. treat everything as a 1-1 draw. no goals for players. need to populate
        # draw against the player name field. No need to populate any keys for players who didn't attend.
        # will log a playerNameGuests key where != 0.
        # once game is appended, recalculate summary table for impacted players.

        game_record = {"Timestamp": datetime.datetime.now(), "Winning Team Score": 1, "Losing Team Score": 1,
                       "Date of Game dd-MON-YYYY": datetime.datetime(new_game.gamedate.year, new_game.gamedate.month,
                                                                     new_game.gamedate.day),
                       "Cost of Game": Decimal128(str(new_game.gamecost))}

        #  game_record["Players"] = new_game.currentactiveplayers
        # cannot use the above as this does not include number of guests - need to set this later on

        team_string = []
        total_players_this_game = 0
        for player in new_game.playerlist:
            if player.playedlastgame:
                game_record[player.playername] = "Draw"
                team_string.append(player.playername)
                total_players_this_game += 1
            if player.guests > 0:
                guest_key = player.playername + "_guests"
                game_record[guest_key] = player.guests
                team_string.append(player.playername + "_has_" + str(player.guests) + "_guests")
                total_players_this_game += player.guests

        game_record["PlayerList"] = ",".join(team_string)

        game_record["Players"] = total_players_this_game
        cost_each = float(new_game.gamecost) / float(total_players_this_game)
        game_record["Cost Each"] = Decimal128(str(cost_each))
        game_record["Booker"] = new_game.booker

        game_record["CFFA"] = "Record submitted by CFFA user"

        self.games.insert(game_record)

        # now update summary collection for each player that played and/or has guests in new_game.playerlist
        # then handle booker and cost of game

        for player in new_game.playerlist:
            player_document = self.teamSummary.find_one({"playerName": player.playername}, collation=aggCollation)
            if player_document is None and player.playername != "":
                # ok we didn't find this player so hopefully will be new! let's set the record to zeros
                new_player = footballClasses.TeamPlayer(player.playername, False, "Created from a New Game")
                self.add_player(new_player)
                player_document = self.teamSummary.find_one({"playerName": player.playername}, collation=aggCollation)
                if player_document is None:
                    logger.critical("add_game(): After adding player, player does not exist in teamSummary")
                    return False

            if player.playedlastgame:
                logger.info("Played Player:" + player.playername + " gamesCost is " + str(
                    player_document.get("gamesCost").to_decimal()))
                previous_cost = player_document.get("gamesCost")
                del player_document["gamesCost"]
                player_document["gamesCost"] = Decimal128(str(float(previous_cost.to_decimal()) + cost_each))
                logger.info(player.playername + " cost of all games:" + str(previous_cost.to_decimal()) + " to " + str(
                    player_document.get("gamesCost").to_decimal()))

                previous_balance = player_document.get("balance")
                del player_document["balance"]
                player_document["balance"] = Decimal128(str(float(previous_balance.to_decimal()) - cost_each))
                logger.info(player.playername + " balance:" + str(previous_balance.to_decimal()) + " to " + str(
                    player_document.get("balance").to_decimal()))

                player_document["lastPlayed"] = datetime.datetime(new_game.gamedate.year, new_game.gamedate.month,
                                                                  new_game.gamedate.day)
                player_document["gamesAttended"] = player_document.get("gamesAttended") + 1
                # now update DB player summary record
                self.teamSummary.update_one({"playerName": player.playername},
                                            {"$set": {
                                                "gamesCost": player_document.get("gamesCost"),
                                                "balance": player_document.get("balance"),
                                                "lastPlayed": player_document.get("lastPlayed"),
                                                "gamesAttended": player_document.get("gamesAttended")}})

            if player.pitchbooker:
                # add booking credit to transactions list as well
                previous_balance = player_document.get("balance")
                del player_document["balance"]
                player_document["balance"] = Decimal128(
                    str(float(previous_balance.to_decimal()) + float(new_game.gamecost)))
                logger.info("Booker Player:" + player.playername + " balance is " + str(
                    player_document.get("balance").to_decimal()))

                self.teamSummary.update_one({"playerName": player.playername},
                                            {"$set": {
                                                "balance": player_document.get("balance")
                                            }})
                transaction_document = {}
                transaction_document["Player"] = player.playername
                transaction_document["Type"] = "CFFA Booking Credit"
                transaction_document["Amount"] = Decimal128(str(float(new_game.gamecost)))
                transaction_document["Date"] = datetime.datetime(new_game.gamedate.year, new_game.gamedate.month,
                                                                 new_game.gamedate.day)
                self.payments.insert(transaction_document)
                logger.info("Booker " + player.playername + "transaction added for booking credit of " + str(
                    float(new_game.gamecost)))

            if player.guests > 0:
                previous_balance = player_document.get("balance")
                del player_document["balance"]
                player_document["balance"] = Decimal128(str(float(previous_balance.to_decimal()) -
                                                            (cost_each * player.guests)))
                logger.info("Guests for player:" + player.playername)

                self.teamSummary.update_one({"playerName": player.playername},
                                            {"$set": {
                                                "balance": player_document.get("balance")
                                            }})
        return True

    def edit_game(self, db_id, edit_game_form):
        """ Logic to edit a edit an existing game. Updates each player's summary figures (ie: balance)

          Parameters
          ----------

          db_id : ObjectId
            Unique ID in the game collection

          edit_game_form : footballClasses.Game
            Populated game object from input form.

          Returns
          -------

          message : str
            Message to show if action succeeded or not.
          """
        # db_id must be set and exists
        logger.debug("We got to edit game")

        game_record = self.games.find_one({"_id": db_id})

        # start updating each old record with content in edit_game_form
        game_record["Timestamp"] = datetime.datetime.now()
        game_record["Date of Game dd-MON-YYYY"] = datetime.datetime(edit_game_form.gamedate.year,
                                                                    edit_game_form.gamedate.month,
                                                                    edit_game_form.gamedate.day)
        original_cost_game = game_record.get("Cost of Game")
        game_record["Cost of Game"] = Decimal128(str(edit_game_form.gamecost))

        team_string = []
        total_players_this_game = 0
        for player in edit_game_form.playerlist:
            # first check if any player is new
            player_document = self.teamSummary.find_one({"playerName": player.playername}, collation=aggCollation)
            if player_document is None and player.playername != "":
                # ok we didn't find this player so hopefully will be new! let's set the record to zeros
                new_player = footballClasses.TeamPlayer(player.playername, False, "Created from an Edited Game")
                self.add_player(new_player)
                player_document = self.teamSummary.find_one({"playerName": player.playername}, collation=aggCollation)
                if player_document is None:
                    logger.critical("add_game(): After adding player, player does not exist in teamSummary")
                    return False

            if player.playedlastgame:
                game_record[player.playername] = "Draw"
                team_string.append(player.playername)
                total_players_this_game += 1
            else:
                game_record.pop(player.playername, None)

            guest_key = player.playername + "_guests"
            if player.guests > 0:
                team_string.append(player.playername + "_has_" + str(player.guests) + "_guests")
                total_players_this_game += player.guests
                game_record[guest_key] = player.guests
            else:
                game_record.pop(guest_key, None)

            # check for guest changes.

        game_record["PlayerList"] = ",".join(team_string)

        # TO DO: Go through team_string again, and if any key has a value (that is set to Draw, Win, Lose, No Show,
        # No Play)
        # but is not in the team_string list we need to pop. This is bad as someones name could be Cost of Game and this
        # would cause lots of issues. Players should be a list in the dict!
        keys_to_pop = []
        for key in game_record.keys():
            if game_record.get(key, None) in ["Win", "win", "Draw", "draw", "Lose", "lose", "no show", "No Show",
                                              "no play", "No Play"]:
                # now check if in team_string
                if key not in team_string:
                    keys_to_pop.append(key)
                    logger.info("Removing player " + key + " from game on " +
                                str(game_record.get("Date of Game dd-MON-YYYY").year) + "/" +
                                str(game_record.get("Date of Game dd-MON-YYYY").month) + "/" +
                                str(game_record.get("Date of Game dd-MON-YYYY").day))

        for key in keys_to_pop:
            game_record.pop(key, None)

        game_record["Players"] = total_players_this_game
        cost_each = float(edit_game_form.gamecost) / float(total_players_this_game)
        game_record["Cost Each"] = Decimal128(str(cost_each))
        original_booker = game_record.get("Booker")
        game_record["Booker"] = edit_game_form.booker

        game_record["CFFA"] = "Record edited by CFFA user"

        # if we update then we need to add every player status and guests, long document.
        # Instead delete the game document and insert of a new one for now.
        # self.games.update( { "_id": db_id}, {"$set" : game_record}, upsert=False)
        self.games.delete_one({"_id": db_id})
        logger.debug("Deleted game during edit with db_id" + str(db_id))
        self.games.insert(game_record)
        logger.debug("Inserted new edited game record:" + " ".join(team_string))
        # sort out impact on costs.
        # easiest to resync all costs on all historical games instead of add/removing costs on a per player  basis
        # added guests field into games to capture guests
        # but need to update transactions for booker if that has changed.

        date_string = str(edit_game_form.gamedate.year) + "/" + str(edit_game_form.gamedate.month) + "/" + str(
            edit_game_form.gamedate.day)

        if original_booker != game_record.get("Booker") or original_cost_game != game_record.get("Cost of Game"):
            # add transaction to remove original booker credit with original cost of game then
            # add transaction to add new cost of booking with new (or same) booker)
            logger.debug("edit_game(): Cost of game or change of booker")

            transaction_document = {"Player": original_booker,
                                    "Type": "CFFA Game Edit for " + date_string +
                                            ". Booker change - remove original game credit",
                                    "Amount": Decimal128(str(float(0 - original_cost_game.to_decimal()))),
                                    "Date": datetime.datetime.now()}
            self.payments.insert(transaction_document)
            logger.debug("inserted new transaction for " + original_booker + " to remove credit for this player")

            transaction_document = {"Player": game_record.get("Booker"),
                                    "Type": "CFFA Game Edit for " +
                                            date_string + ". Booker change - add new game credit",
                                    "Amount": Decimal128(str(float(edit_game_form.gamecost))),
                                    "Date": datetime.datetime.now()}
            self.payments.insert(transaction_document)
            logger.debug(
                "inserted new transaction for " + game_record.get("Booker") + " to add booking credit for this player")

        player_dict = list(self.teamSummary.find({}, {"playerName": 1}))
        player_list = []
        for player in player_dict:
            player_list.append(player.get("playerName"))

        self.calc_populate_team_summary(player_list)

        return True

    def delete_game(self, db_id):
        """ Logic to delete a game

          Parameters
          ----------

          db_id : ObjectId
            MongoDB ID to game document.

          Returns
          -------

          message : str
            Message to show if action succeeded or not.
          """

        # not just delete Game record (db_id), but also refund transaction (log in transaction) for booker.
        # Then recalculate summary table.
        game_document = self.games.find_one({"_id": db_id})

        transaction_document = {}
        game_date = game_document.get("Date of Game dd-MON-YYYY")
        date_string = str(game_date.year) + "/" + str(game_date.month) + "/" + str(game_date.day)

        transaction_document["Player"] = game_document.get("Booker")
        transaction_document["Date"] = datetime.datetime.now()

        if "Booker" in game_document:

            transaction_document["Type"] = "CFFA Game Deletion for " + date_string + ". Booker removal - game credit"
            transaction_document["Amount"] = Decimal128(str(0 - float(game_document.get("Cost of Game").to_decimal())))
            delete_message = "Game " + date_string + " deleted and transactions adjusted."
            logging.debug(
                "Booking credit for booker" + game_document.get("Booker") + " removed as game is being deleted")

        else:
            delete_message = "Warning: Game had no booker - will need manual review of past transactions to remove " \
                             "booker credit"
            transaction_document[
                "Type"] = "CFFA Game Deletion for " + date_string + ". No booker set: no booker credit."
            transaction_document["Amount"] = Decimal128("0.00")
            logger.warning("There was no booker for deleted game. Maybe imported game.")

        self.payments.insert(transaction_document)
        logger.debug("Inserted new transaction to remove booking credit")

        self.games.delete_one({"_id": db_id})

        player_dict = list(self.teamSummary.find({}, {"playerName": 1}))
        player_list = []
        for player in player_dict:
            player_list.append(player.get("playerName"))

        self.calc_populate_team_summary(player_list)

        return delete_message

    def get_aggregated_payments(self):
        """ Logic to aggregate each player payments together (sum up their transactions)


          Returns
          -------

          aggregated_payments : `dict` : `list`
            dict is : key: PlayerName, value: sum
          """
        aggregated_payments = {}
        try:
            agg_cursor = self.payments.aggregate([{"$group": {"_id": "$Player", "sum": {"$sum": "$Amount"}}}
                                                  ], collation=aggCollation)
            for x in list(agg_cursor):
                aggregated_payments[x.get("_id")] = x.get("sum")
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not aggregate payments")
            logger.critical(e.code + e.details)

        return aggregated_payments

    def get_active_player_summary(self):
        """ Obtain players summary date within a recent timeframe (hardcoded active days value)

          Returns
          -------

          active_players : `dict` : `list`
            list of recently played player data from teamSummary

          """

        active_players = []
        cur_off_date = datetime.date.today() - datetime.timedelta(days=activeDays)
        cur_off_datetime = datetime.datetime(cur_off_date.year, cur_off_date.month, cur_off_date.day)
        try:
            active_players = list(self.teamSummary.find({"lastPlayed": {"$gte": cur_off_datetime}}, {"_id": 0}))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not return summary")
            logger.critical(e.code, e.details)

        # for checkPlayer in playersInDB:
        #    daysSincePlayed = datetime.date.today() - checkPlayer.get("lastPlayed").date()

        #    if daysSincePlayed.days < activeDays:
        #        active_players.append(checkPlayer)

        return active_players

    def get_full_summary(self):
        """ Obtain all players summary date

          Returns
          -------

          all_players : `dict` : `list`
            list of all player data from teamSummary

          """

        all_players = []
        try:
            # all_players = list(self.teamSummary.find({}, { "_id": 0}))
            all_players = list(self.teamSummary.find({}))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not return summary")
            logger.critical(e.code + e.details)

        return all_players

    def get_recent_games(self):
        """ Obtain game summary date within a recent timeframe (hardcoded active days value)

          Returns
          -------

          games_in_db : `dict` : `list`
            list of recently played game data from game collection

          """
        # should convert the string in the date field during population to date or ISODate then find() can do the
        # filter. instead we get all games then then loop through them, slower and not efficient.

        games_in_db = []
        # work out datetime for cutoff.
        cur_off_date = datetime.date.today() - datetime.timedelta(days=activeDays)
        cur_off_datetime = datetime.datetime(cur_off_date.year, cur_off_date.month, cur_off_date.day)
        try:
            games_in_db = list(self.games.find({"Date of Game dd-MON-YYYY": {"$gte": cur_off_datetime}}, {"_id": 0}))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get list of games in get_recent_games()")
            logger.critical(e.code + e.details)

        return games_in_db

    def get_games_for_player(self, player_name):
        """ Obtain game summary data for the specified player

          Parameters
          ----------

          player_name : str
            Name of player.

          Returns
          -------

          games_in_db : `dict` : `list`
            list of recently played game data from game collection

          """

        games_in_db = []
        try:
            games_in_db = list(
                self.games.find({player_name: {"$in": ["Win", "Lose", "Draw", "No Show"]}}, collation=aggCollation))
        except Exception as e:
            logger.critical("Could not get list of games in get_games_for_player() with name " + player_name)
            logger.critical(e.code + e.details)

        return games_in_db

    def get_all_games(self):
        """ Obtain all game summary date

          Returns
          -------

          games_in_db : `dict` : `list`
            list of all played game data from game collection

          """
        # sort on date, latest first
        games_in_db = []
        try:
            games_in_db = list(self.games.find({}).sort("Date of Game dd-MON-YYYY", -1))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get list of games in get_all_games()")
            logger.critical(e.code + e.details)

        return games_in_db

    def get_recent_transactions(self):
        """ Obtain transaction data within a recent timeframe (hardcoded daysForRecentPayment value)

          Returns
          -------

          recent_transactions : `dict` : `list`
            list of recently played transaction data from transaction collection

          """
        recent_transactions = []
        cur_off_date = datetime.date.today() - datetime.timedelta(days=daysForRecentPayment)
        cur_off_datetime = datetime.datetime(cur_off_date.year, cur_off_date.month, cur_off_date.day)

        try:
            recent_transactions = list(self.payments.find({"Date": {"$gte": cur_off_datetime}}, {"_id": 0}))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get list of transactions in get_recent_transactions()")
            logger.critical(e.code + e.details)

        return recent_transactions

    def get_all_transactions(self):
        """ Obtain all transaction data

          Returns
          -------

          all_transactions : `dict` : `list`
            list of all transaction data from transaction collection

          """
        all_transactions = []
        try:
            all_transactions = list(self.payments.find({}).sort("Date", -1))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get list of transactions in get_all_transactions()")
            logger.critical(e.code + e.details)

        return all_transactions

    def get_active_players_for_new_game(self):
        """ Returns a list of player dicts (who have played since activeDays, with the lastGamePlayed key value set to
        False/True according to whether they played the last game or not.

          Returns
          -------

          active_players : `dict` : `list`
            list of recent played players with 'lastPlayed' key set to whether they played the last gam e o not.

          """
        # return list dict of players containing keys of "name" and
        # "lastGamePlayed" value 1 (default checked) or 0 (not checked)
        active_players = []
        # work out datetime for cutoff.
        cur_off_date = datetime.date.today() - datetime.timedelta(days=activeDays)
        cur_off_datetime = datetime.datetime(cur_off_date.year, cur_off_date.month, cur_off_date.day)
        try:
            active_players = list(
                self.teamSummary.find({"lastPlayed": {"$gte": cur_off_datetime}}, {"playerName": 1, "lastPlayed": 1}))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get active Players from Summary in get_active_players_for_new_game()")
            logger.critical(e.code + e.details)

        last_game = self.get_last_game_details()

        for player in active_players:
            if player["lastPlayed"] == last_game[0].get("Date of Game dd-MON-YYYY"):
                player["lastGamePlayed"] = True  # checked
            else:
                player["lastGamePlayed"] = False  # not checked

        return active_players

    def get_inactive_players_for_new_game(self):
        """ Returns a list of player dicts (who have not played  more than activeDays static)

          Returns
          -------

          inactive_players : `dict` : `list`
            list of inactive players.

          """
        # return list dict of players containing keys of "name"
        inactive_players = []
        # work out datetime for cutoff.
        cur_off_date = datetime.date.today() - datetime.timedelta(days=activeDays)
        cur_off_datetime = datetime.datetime(cur_off_date.year, cur_off_date.month, cur_off_date.day)
        try:
            inactive_players = list(self.teamSummary.find({"lastPlayed": {"$lt": cur_off_datetime}},
                                                          {"playerName": 1}))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get inactive Players from Summary in get_inactive_players_for_new_game()")
            logger.critical(e.code + e.details)

        return inactive_players

    def get_last_game_details(self):
        """ Returns a dictionary containing details of the last played game.

          Returns
          -------

          last_played : `dict``
            Keys are "Date of Game dd-MON-YYYY" : datetime.datetime , "Cost of Game" : decimal128

          """
        # return the last game cost in Decimal128 in a single element list of dict with date and cost.
        last_played = []
        try:
            last_played = list(self.games.find({}, {"_id": 0, "Date of Game dd-MON-YYYY": 1, "Cost of Game": 1},
                                               collation=aggCollation)
                               .sort("Date of Game dd-MON-YYYY", -1).limit(1))
            if len(last_played) == 0:
                last_played.append({"Date of Game dd-MON-YYYY": datetime.datetime(1970, 1, 1, 0, 0),
                                    "Cost of Game": Decimal128("0.00")})
        except ValueError:
            logger.warning("Unable to find last played game")
            last_played = []
            last_played.append({"Date of Game dd-MON-YYYY": datetime.datetime(1970, 1, 1, 0, 0),
                                "Cost of Game": Decimal128("0.00")})
            # last_played will be a list of a single element of datetime.datetime
            logger.warning(last_played)

        return last_played

    def get_last_game_db_id(self):
        """ Returns the DB id (_id) of the last game played.

          Returns
          -------

          last_played : ObjectID
            ID is from MongoDB from the last game in the games collection.

          """

        last_played = list(
            self.games.find({}, {"_id": 1, "Date of Game dd-MON-YYYY": 1}).sort("Date of Game dd-MON-YYYY", -1).limit(
                1))
        if len(last_played) == 0:
            logger.warning("getLastGameDBIB(): no games found")
            return 0

        return last_played[0].get("_id")

    def get_defaults_for_new_game(self, logged_in_user):
        """ Returns a list of player objects with defaults for new game form. .

            Parameters
            ----------

            logged_in_user : str
                PlayerName of the logged in user. This is to make this player the default booker.

           Returns
           -------

           game : `footballClasses.Game`
             Game object containing a list of players details for defaults in new game form.

           """
        new_game_players = []

        active_players = self.get_active_players_for_new_game()

        count = 0
        for active_player in active_players:
            if logged_in_user == active_player.get("playerName"):
                likely_booker = True
            else:
                likely_booker = False

            new_player = footballClasses.Player(active_player.get("_id"),
                                                active_player.get("playerName"),
                                                active_player.get("lastGamePlayed"), likely_booker, 0)
            new_game_players.append(new_player)
            count += 1

        # if supplied players are less than 10, append blank defaults for the new game form.
        if count < 10:
            for x in range(count, 10):
                new_player = footballClasses.Player("empty", "", False, False, 0)
                new_game_players.append(new_player)

        last_game = self.get_last_game_details()

        new_game = footballClasses.Game(last_game[0].get("Cost of Game"),
                                        datetime.datetime.date(datetime.datetime.now()),
                                        new_game_players, "")

        return new_game

    def date_of_game(self, game_db_id):
        """ Returns a datetime.datetime date of the game based on the DB _id.. .

            Parameters
            ----------

            game_db_id : ObjectID
                Unique object ID for the game document.

           Returns
           -------

           ourDate : datetime.datetime
             Date of game..

           """
        game_date = self.games.find_one({"_id": game_db_id}, {"Date of Game dd-MON-YYYY": 1})
        our_date = ""
        if game_date is not None:
            our_date = game_date.get("Date of Game dd-MON-YYYY")
        return our_date

    def get_game_details_for_edit_delete_form(self, game_db_id, long):
        """ Returns a list of player objects with defaults for new game form. .

            Parameters
            ----------

            game_db_id : ObjectID
                Unique object ID for the game document.

            long : Boolean
                If this is true add a player obj for every player in the DB.

           Returns
           -------

           game : `footballClasses.Game`
             Game object containing a list of players details for the game selected for populate in edit/delete game
             forms.
        """

        game_players = []
        all_players = self.get_all_players()  # from summary table
        count = 0
        for player in all_players:
            if player.get("playerName") == self.check_game_for_booker(game_db_id):
                booker = True
            else:
                booker = False

            if self.did_player_play_this_game(game_db_id, player.get("playerName")):
                played_game = True
            else:
                played_game = False

            guests = self.check_game_for_guests(game_db_id, player.get("playerName"))
            if long or booker or played_game or guests > 0:
                add_player = footballClasses.Player(player.get("_id"),
                                                    player.get("playerName"),
                                                    played_game,
                                                    booker,
                                                    guests)
                game_players.append(add_player)
            count += 1

        # if supplied players are less than 10, append blank defaults for the new game form.
        if count < 10:
            for x in range(count, 10):
                blank_player = footballClasses.Player("empty", "", False, False, 0)
                game_players.append(blank_player)

        return self.get_game_from_db(game_db_id, game_players)

    def new_manager(self):
        """ If this is a new manager session (played less than 3 games, keep a banner popping up. .

           Returns
           -------

           status : boolean
             Return true if the manager has only submitted 3 games or less, else False.

           """
        all_games = list(self.games.find())
        if len(all_games) <= 3:
            return True

        return False

    def get_all_players(self):
        """ Returns a list of all player details - requires an effective join between teamSummary and teamPlayers, not
         efficient in mongoDB as it is not relational..

           Returns
           -------

           all_players : `dict` : `list`
             List of dict of players with keys playerName:str, retiree: bool, comment: str

        """
        all_players = []
        try:
            all_players = list(self.teamSummary.find({}, {"_id": 1, "playerName": 1, "lastGamePlayed": 1}))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get All Players from Summary in get_all_players()")
            logger.critical(e.code + e.details)

        try:
            # inefficient - mongoDB is not relational!
            all_player_details = list(self.teamPlayers.find({}))
            for player in all_players:
                for playerX in all_player_details:
                    if player.get("playerName", None) == playerX.get("playerName", "Not Set"):
                        player["retiree"] = playerX.get("retiree", False)
                        player["comment"] = playerX.get("comment", "No comment set")
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get/process All Players from teamPlayers in get_all_players()")
            logger.critical(e.code + e.details)

        return all_players

    def get_game_from_db(self, game_db_id, player_list):

        """ Returns a game object from game_db_id key.

            Parameters
            ----------

            game_db_id : ObjectID
                Unique object ID for the game document.

            player_list : `footballClasses.Player` : `list`
                A list of Player objects.

           Returns
           -------

           game : `footballClasses.Game`
             Game object containing a list of players details for the game.

        """

        game = self.games.find_one({"_id": game_db_id}, {"Date of Game dd-MON-YYYY": 1, "Cost of Game": 1,
                                                         "PlayerList": 1, "Players": 1, "Booker": 1})

        our_game = None
        booker = ""
        if game is not None:
            if "Booker" in game:
                booker = game["Booker"]

            game_date = datetime.datetime.date(game.get("Date of Game dd-MON-YYYY"))
            our_game = footballClasses.Game(game.get("Cost of Game"),
                                            game_date,
                                            player_list, booker)

        return our_game

    def check_game_for_booker(self, game_db_id):
        """ returns the booker playerName for requested game ID

        Parameters
        ----------

            game_db_id : ObjectID
                Unique object ID for the game document.

        Returns
        -------

        booker : str
            The booker name string (playerName)
        """

        game = self.games.find_one({"_id": game_db_id}, {"Booker": 1})
        booker = ""
        if game is not None:
            if "Booker" in game:
                booker = game.get("Booker")

        return booker

    def check_game_for_guests(self, game_db_id, player_name):
        """ checks the player_list value for the specified game and player for number of guests using regex

        Parameters
        ----------

        game_db_id : ObjectID
            Unique object ID for the game document.

        player_name : str
            player to check for guests

        Returns
        -------

            guests : int
                The number of guests for requested player and game
        """

        # need to traverse the playerList string for "<name>_has_X_guests"
        game = self.games.find_one({"_id": game_db_id}, {"PlayerList": 1})
        guests = 0
        if game is not None:
            must_match = player_name + "_has_(\d)+_guests"
            regex = re.compile(must_match)
            r = regex.search(game.get("PlayerList"))
            if r is not None:
                guests = r.group(1)

        return guests

    def did_player_play_this_game(self, game_db_id, name):
        """ checks if a player played the specified game

        Parameters
        ----------

        game_db_id : ObjectID
            Unique object ID for the game document.

        name : str
            player to check for whether they played game

        Returns
        -------

            played : boolean
                True if game played else false.
        """
        game = self.games.find_one({"_id": game_db_id}, {name: 1})
        if game is not None:
            if game.get(name) in ["Win", "win", "Draw", "draw", "Lose", "lose", "no show", "No Show"]:
                return True

        return False

    def get_all_player_details_for_player_edit(self):
        """ gets all players information to prepopulate a player edit form

        Returns
        -------

            all_players : `footballClasses.player` : `list`
                All player details from teamSummary in a list.
        """
        # returns a list of teamPlayer objects
        all_players = []
        try:
            our_players = list(self.teamSummary.find({}, {"_id": 1, "playerName": 1, "comment": 1, "retiree": 1}))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get All Players from Summary in get_all_player_details_for_player_edit()")
            logger.critical(e.code + e.details)

        for player in our_players:
            team_player = footballClasses.TeamPlayer(player.get("playerName"),
                                                     player.get("retiree", False),
                                                     player.get("comment", "Blank"))
            all_players.append(team_player)

        return all_players

    def get_player_defaults_for_edit(self, player_name):
        """ for specified player get defaults for edit form

        Parameters
        ----------

        player_name : Str
            player name.

        Returns
        -------

            player : footballClasses.Player

        """
        try:
            this_player = self.teamPlayers.find_one({"playerName": player_name})
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get the player in teamPlayers in get_player_defaults_for_edit()")
            logger.critical(e.code + e.details)

        player = footballClasses.TeamPlayer(player_name, this_player.get("retiree", False),
                                            this_player.get("comment", "Not set"))
        return player

    def get_player_labels(self):
        """ gets a list of labels for the selectField when editing players.

        Returns
        -------

            all_players : `str` : `list`
                List of player name strings.
        """
        all_players = []
        try:
            our_players = list(self.teamSummary.find({}, {"_id": 1, "playerName": 1}))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get All Players from Summary in get_all_player_details_for_player_edit()")
            logger.critical(e.code + e.details)

        for player in our_players:
            all_players.append(player.get("playerName"))

        return all_players

    def should_player_be_retired(self, player_name):
        """ after data import, assess if a player should be treated as a retiree based on the static activeDays variable

        Parameters
        ----------

        player_name : str
            player to assess

        Returns
        -------

            status : boolean
                True if should be retired, else false.
        """
        cur_off_date = datetime.date.today() - datetime.timedelta(days=activeDays)
        cur_off_datetime = datetime.datetime(cur_off_date.year, cur_off_date.month, cur_off_date.day)

        player_last_played = self.teamSummary.find_one({"playerName": player_name}, {"lastPlayed": 1})

        if player_last_played.get("lastPlayed", datetime.datetime(1970, 1, 1, 0, 0)) < cur_off_datetime:
            return True

        return False

    def add_transaction(self, transaction):
        """ adds the transaction into the DB

        Parameters
        ----------

        transaction : Footballclasses.Transaction
            Transaction details

        Returns
        -------

            message : string
                Message to be sent back to the user on whether action succeeded or not

        """

        # assumes transaction.transactiondate is a datetime.date object, not datetime.datetime
        # assumes Amount is a float

        if self.player_exists(transaction.player):
            payment = {"Player": transaction.player, "Type": transaction.description,
                       "Amount": Decimal128(str(transaction.amount)),
                       "Date": datetime.datetime(transaction.transactiondate.year,
                                                 transaction.transactiondate.month,
                                                 transaction.transactiondate.day)}

            try:
                self.payments.insert(payment)
                message = "Added transaction £" + str(transaction.amount) + " against " + transaction.player
            except pymongo.errors.OperationFailure as e:
                logger.critical("Could not add transaction in add_transaction()")
                logger.critical(e.code + e.details)
                message = "Internal error when adding transaction " + str(
                    transaction.amount) + " against " + transaction.player
                logger.error(message)
                return message
        else:
            message = "Player " + transaction.player + " does not exist in system. Transaction not added"
            logger.error(message)
            return message

        # TO DO - update Summary table. ALso check if AutoPay has a duplicate
        player_document = self.teamSummary.find_one({"playerName": transaction.player}, collation=aggCollation)
        if player_document is None:
            # no record for player, this should not happen as it was in a select list. lets abort
            message = "Selected player" + transaction.player + " is not in teamSummary table. Did not adjust Summary"
            logger.error(message)
            return message

        # only balance and moniespaid needs to be adjusted - add transaction amount to both values
        current_balance = float(player_document.get("balance", Decimal128("0.00")).to_decimal())
        current_payments = float(player_document.get("moniespaid", Decimal128("0.00")).to_decimal())
        current_balance += transaction.amount
        current_payments += transaction.amount  # transaction.amount is a float

        try:
            self.teamSummary.update_one({"playerName": transaction.player},
                                        {"$set": {
                                            "balance": Decimal128(str(current_balance)),
                                            "moniespaid": Decimal128(str(current_payments))}})
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not update summary table in  add_transaction()")
            logger.critical(e.code + e.details)
            message = "Internal error when updating summary after adding transaction " + str(
                transaction.amount) + " against " + transaction.player
            logger.critical(message)

        logger.info(message)
        return message

    def get_autopay_details(self, user):
        """ works out what the autopay settings should be - based on the last player game - this auto transaction is
        against the manager.

        Parameters
        ----------

        user : str
            playerName of logged in CFFA user..

        Returns
        -------

            payment : footballClasses.Transaction
                Expected transaction details.
        """
        # return a transaction object with logged in user, amount and todays date set. description is AutoPay
        # however return a £0 credit if the transaction already exists

        last_game = list(self.games.find({}).sort("Date of Game dd-MON-YYYY", -1).limit(1))
        if len(last_game) == 1:
            rounded_cost_each = round(last_game[0].get("Cost Each").to_decimal(), 2)
        else:
            rounded_cost_each = "0.00"
        payment = footballClasses.Transaction(user, "CFFA AutoPay", float(rounded_cost_each), datetime.date.today())

        return payment

    def get_defaults_for_transaction_form(self, player):
        """ Most likely a transaction will be the value of the last game / number of players. This call populates the
        transaction form with this data.

        Parameters
        ----------

        player : str
            PlayerName for the transaction.

        Returns
        -------

            transaction : footballClasses.transaction
                Populated transaction details.
        """
        #     player  description amount transaction_date
        last_game = list(self.games.find({}).sort("Date of Game dd-MON-YYYY", -1).limit(1))
        if len(last_game) == 1:
            transaction = footballClasses.Transaction(player, "Transfer",
                                                      float(round(last_game[0].get("Cost Each").to_decimal(), 2)),
                                                      datetime.date.today())
        else:
            transaction = footballClasses.Transaction(player, "Transfer", float("0.00"), datetime.date.today())

        return transaction

    def update_team_name(self, new_name, user_id):
        """ update Team name across tenancy and teamSettings collections

        Parameters
        ----------

        new_name : str
            New team name.

        user_id : str
            Auth0 ID of logged in user.

        Returns
        -------

            message : str
                Message for web user if the action succeeded or not.
        """
        settings = list(self.teamSettings.find({}))
        our_id = None
        message = ""

        for setting in settings:
            if setting.get("teamName", None) is not None:
                our_id = setting.get("_id")
                current_team = setting.get("teamName", None)

        if our_id is not None:
            try:
                self.teamSettings.update({"_id": our_id}, {"$set": {"teamName": new_name}})
                message = "Successfully updated teamName from " + current_team + " to " + new_name
            except pymongo.errors.OperationFailure as e:
                logger.critical("Could not update teamSettings in  update_team_name()")
                logger.critical(e.code + e.details)
                message = "Internal error when updating teamSettings name " + new_name

            try:
                self.tenancy.update({"userID": user_id, "teamName": current_team}, {"$set": {"teamName": new_name}})
                # TO DO:  find all records on tenancyID and update team name
            except pymongo.errors.PyMongoError:
                logger.critical("Could not update tenancy for teamName " + current_team + " to team " + new_name)
                message = "Internal error when updating database"

        logger.info(message)
        return message

    def get_app_settings(self):
        """ get all settings from TeamSettings.

        Returns
        -------

            ourSettings : footballClasses.CFFASettings
                Single obj returned.
        """
        # TO DO: this is ugly, must be a better way to populate object
        settings = list(self.teamSettings.find({}))
        team_name = None
        for setting in settings:
            if setting.get("teamName", None) is not None:
                team_name = setting.get("teamName", None)

        our_settings = footballClasses.CFFASettings(team_name)

        return our_settings

    def get_team_settings(self):
        """ get all settings and returned in dict list.

        Returns
        -------

            allSettings : 'dict' : 'list'
                dictionary only contains TeamName currently.
        """
        all_settings = []
        try:
            all_settings = list(self.teamSettings.find())
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get/process settings from teamSettings in get_team_settings()")
            logger.critical(e.code + e.details)

        return all_settings

    def get_team_players(self):
        """ returns all players in team in dict list.

        Returns
        -------

            teamPlayers : `dict` : `list`
                keys as per db - name, comment and retiree keys.
        """
        team_players = []
        try:
            team_players = list(self.teamPlayers.find())
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get/process settings from team_players in get_team_players()")
            logger.critical(e.code + e.details)

        return team_players

    def get_user_access_data(self, user_id):
        """ works across tenancies. Given the user ID return all users with that tenancy.

        Parameters
        ----------

        user_id : str
            Auth0 user id that starts with auth0|.

        Returns
        -------

            users : footballClasses.User : `list`
                List of User objs.
        """
        # get tenancy ID from user_id, then return all users in tenancy with that tenancyID
        users = []
        try:
            tenancy_id = self.get_tenancy_id(user_id)
            db_users = list(self.tenancy.find({"tenancyID": tenancy_id}))

            for user in db_users:
                a_user = footballClasses.CFFAUser(user.get("userName", None),
                                                  user.get('userID', None),
                                                  user.get('userType', None),
                                                  user.get('revoked', None))
                users.append(a_user)

        except Exception as e:
            logger.critical("Unable to find tenancyID or users in get_user_access_data()")
            logger.critical(e.code + e.details)

        return users

    def get_tenancy_id(self, user_id):
        """ returns the tenancy ID (used as part of a collection name) from a user ID.

        Parameters
        ----------

        user_id : str
            Auth0 user id that starts with auth0|.

        Returns
        -------

            tenancyID : str
                Tenancy prefix for collections
        """
        try:
            tenancy_document = self.tenancy.find_one({"$and": [{"userID": user_id, "default": True}]},
                                                     {"tenancyID": 1})
        except Exception as e:
            logger.critical("Unable to find tenancyID when get_tenancy_id()")
            logger.critical(e.code + e.details)
            return None

        return tenancy_document.get("tenancyID", None)

    def get_team_name(self, tenancy_id):
        """ returns the team name from the tenancy ID.

        Parameters
        ----------

        tenancy_id : str
            Tenancy prefix for collections

        Returns
        -------

            team_name : str
                tenancy's team name.
        """
        team_name = None
        try:
            cffa_settings = list(self.teamSettings.find())
            for setting in cffa_settings:
                if setting.get("teamName", None) is not None:
                    team_name = setting.get("teamName", None)

        except Exception as e:
            logger.critical("Unable to find teamName in settings in get_team_name()")
            logger.critical(e.code + e.details)
            return None

        return team_name

    def add_user_access(self, name, auth_id, role, this_user_id):

        """ add user document to tenancy collection.

        Parameters
        ----------

        name : str
            Name of new user, should match player name

        auth_id : str
            Auth0 unique ID for new user. Starts with auth0|

        role : str
            Either "Manager" or "Player"

        this_user_id : str
            Auth0 unique ID of the user adding this new user.

        Returns
        -------

            message : str
                Message contains if action was successful.
        """

        tenancy_id = self.get_tenancy_id(this_user_id)
        team_name = self.get_team_name(tenancy_id)

        if tenancy_id is not None and team_name is not None:
            try:
                self.tenancy.insert(dict(
                    userName=name,
                    userID=auth_id,
                    tenancyID=tenancy_id,
                    teamName=team_name,
                    userType=role,
                    revoked=False,
                    default=True))
                message = "Added user to CFFA"
            except Exception as e:
                logger.critical("Unable to insert user into tenancy collection in add_user_access()")
                logger.critical(e.code + e.details)
                message = "Internal Error. Unable to add user"
        else:
            message = "Internal Error, unable to add user due to tenancy or team name setting"

        return message

    def edit_user_access(self, old_user_name, cffa_user):
        """ Modifies an existing user, could be name or role.

        Parameters
        ----------

        old_user_name : str
            Existing username

        cffa_user : footballClasses.User
            New user details.

        Returns
        -------

            message : str
                Message contains if action was successful.
        """
        # replace document
        titled_user_name = cffa_user.name.title()
        if old_user_name != titled_user_name:
            # remove old document, the insert new. Hmm or does the below work?
            self.tenancy.update_one({"userName": old_user_name}, {"$set":
                                                                      {"userName": titled_user_name,
                                                                       "userID": cffa_user.authid,
                                                                       "userType": cffa_user.role,
                                                                       "revoked": cffa_user.revoked
                                                                       }})
            message = "Updated user " + old_user_name + " to " + titled_user_name + " and their access details"

        else:
            self.tenancy.update_one({"userName": titled_user_name}, {"$set":
                                                                         {"userID": cffa_user.authid,
                                                                          "userType": cffa_user.role,
                                                                          "revoked": cffa_user.revoked
                                                                          }})
            message = "Updated user " + titled_user_name + " access details"

        logger.info(message)
        return message

    def validate_user_as_player_role(self, user_id):
        """ Checks if the user [that has just logged in] is a player or not [manager]. If a user logs in and does not
        have a tenancy document, we assume this user is a new manager and assumes that role.

        Parameters
        ----------

        user_id : str
            Auth0 user ID that starts with auth0|

        Returns
        -------

            result : boolean
                If the user is confirmed via the tenancy collection that it is a pLayer this returns True, else False.
        """
        if user_id is None:
            logger.warning("Unable to validate player user ID as it not set in validatePlayerRole()")
            return False  # AUth0 has no user ID, deny any manager admin access to system
            # TO DO: should handle this somewhere else but might not be a use valid case as Auth0 would not permit
            # access without a user_id.

        try:
            user = self.tenancy.find_one({"$and": [{"userID": user_id, "default": True}]},
                                         {"tenancyID": 1, "userType": 1, "name": 1})
            if user is None:
                logger.warning("User ID " + str(user_id) + "has no tenancies set. Will be new manager")
                # TO DO: Needs to be redesigned to tighten up access.
                return False

            if user.get('userType', None) == "Player":
                logger.debug("User " + user_id + " validated as a player")
                return True
            else:
                return False

        except pymongo.errors.PyMongoError:
            logger.critical("Unable to validate Player Role in validatePlayerRole()")
            return False

    def get_summary_for_player(self, player_name):
        """ Given the player name, gets their summary data. If the user is a new player there will be no data
        so empty data is returned..

        Parameters
        ----------

        player_name : str
            Player name identifier.

        Returns
        -------

            this_player : footballClasses.PlayerSummary
                object containing summary data for player.

        """
        try:
            player_summary = self.teamSummary.find_one({"playerName": player_name}, {"_id": 0})
        except Exception as e:
            logger.critical("Could not return summary in get_summary_for_player() for player " + player_name)
            logger.critical(e.code, e.details)

        if player_summary is None:
            this_player = footballClasses.PlayerSummary(Decimal128("0.00"),
                                                        Decimal128("0.00"),
                                                        Decimal128("0.00"),
                                                        0,
                                                        datetime.datetime(1970, 1, 1, 0, 0))
        else:
            this_player = footballClasses.PlayerSummary(player_summary.get("balance", Decimal128("0.00")),
                                                        player_summary.get("gamesCost", Decimal128("0.00")),
                                                        player_summary.get("moniespaid", Decimal128("0.00")),
                                                        player_summary.get("gamesAttended", 0),
                                                        player_summary.get("lastPlayed",
                                                                           datetime.datetime(1970, 1, 1, 0, 0))
                                                        )

        return this_player

    def calc_ledger_for_player(self, player_name):
        """ Method builds a ledger for all transactions and game costs in reverse chronological order (since their
         first transaction/game) in the form of a bank-like statement. Merges data across both transactions and
         game collections.

        Parameters
        ----------

        player_name : str
            Player to build the ledger for.

        Returns
        -------

            sortedLedger : `footballClasses.LedgerEntry` : `list`
                Latest first list of transactions and game costs showing financial activity since player started.
        """

        # create a list from games that the player played, and append a list of their transactions, then sort on date,
        # latest first.
        ledger = []
        try:
            # first append the adjustment, if any
            adjustment = self.adjustments.find_one({"name": player_name})
            if adjustment is not None:
                if adjustment.get("adjust").to_decimal() > 0:
                    ledger_record = footballClasses.LedgerEntry(datetime.datetime(2010, 1, 1, 0, 0),
                                                                adjustment.get("adjust"),
                                                                "",
                                                                "",
                                                                "Initial balance adjustment")
                else:
                    ledger_record = footballClasses.LedgerEntry(datetime.datetime(2010, 1, 1, 0, 0),
                                                                "",
                                                                Decimal128(
                                                                    str(abs(adjustment.get("adjust").to_decimal()))),
                                                                "",
                                                                "Initial balance adjustment")
                ledger.append(ledger_record)
            for x in self.games.find({player_name: {"$in": ["Win", "Lose", "Draw", "No Show"]}},
                                     collation=aggCollation):
                actual_cost_each = Decimal128(str(x.get("Cost of Game").to_decimal() / x.get('Players')))
                ledger_record = footballClasses.LedgerEntry(x.get("Date of Game dd-MON-YYYY"),
                                                            "",
                                                            actual_cost_each,
                                                            "",
                                                            "Game")
                ledger.append(ledger_record)

            for x in self.payments.find({"Player": player_name}):
                if x.get("Amount").to_decimal() >= 0:
                    credit = x.get("Amount")
                    debit = ""
                else:
                    debit = Decimal128(str(abs(x.get('Amount').to_decimal())))
                    credit = ""

                ledger_record = footballClasses.LedgerEntry(x.get("Date"),
                                                            credit,
                                                            debit,
                                                            "",
                                                            x.get("Type"))
                ledger.append(ledger_record)

            # now sort on date then calc balance on each row assuming initial balance is 0
            sorted_ledger = sorted(ledger, key=lambda k: k.date)

            rolling_balance = 0
            for record in sorted_ledger:
                if record.credit == "":
                    credit = float()  # 0.0
                else:
                    credit = float(record.credit.to_decimal())
                    record.credit = Decimal128(str(round(credit, 2)))  # rounded for presentation

                if record.debit == "":
                    debit = float()
                else:
                    debit = float(record.debit.to_decimal())
                    record.debit = Decimal128(str(round(debit, 2)))  # rounded for presentation

                rolling_balance = rolling_balance + credit - debit
                record.balance = Decimal128(str(round(rolling_balance, 2)))

            if len(sorted_ledger) == 0:
                # if there are is no activity, at least show something when rendering table
                sorted_ledger.append(
                    footballClasses.LedgerEntry(datetime.datetime(1970, 1, 1, 0, 0), "", "", Decimal128("0.00"),
                                                "Initial Balance"))

            # reverse list so latest dates are first
            sorted_ledger = sorted(sorted_ledger, key=lambda k: k.date, reverse=True)

        except Exception as e:
            logger.error("Internal Error: Unable to process ledger logic for player " + player_name)
            logger.error(e.code + e.details)

        return sorted_ledger

    def drop_all_collections(self, user_id):
        """ Drops all tenancy collections for the user ID and tenancy collection..
        TO DO: implement only removal of user tenancies from tenancy collection.

        Parameters
        ----------

        user_id : str
            Auth0 ID for the logged in manager user.

        Returns
        -------

            message : str
                Message contains whether the method succeeded or failed.

        """

        # method will drop all collections for this user and the tenancy collection
        # TO DO: remove other tenancy collections!
        try:
            self.payments.drop()
            self.games.drop()
            self.teamPlayers.drop()
            self.adjustments.drop()
            self.teamSettings.drop()
            self.teamSummary.drop()
            self.tenancy.drop()
            message = "Dropped all data for user ID: " + user_id
        except Exception as e:
            logger.error("Internal Error: Unable to process drop database for player " + user_id)
            logger.error(e.code + e.details)
            message = "Internal Error: Unable to process drop database for player " + user_id

        return message
