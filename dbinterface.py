import pymongo
import datetime
from bson import Decimal128
from re import sub
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
activeDays = 730   # players that haven't played for these days are excluded from default list of players
daysForRecentPayment = 180 # cut off for recent payments/transactions when viewed.

# DB Tables are
#  playerSummary  (list of players and their details)
#  games  (list of every game and each game detail
#  transactions (all financial transfers)
#  team  (team name, and list of player names)
#  user (for access : username, credentials, role, player name
#  role (admin | manager | player  - read only )

# DB needs to know about each of the above objects to store it but not import

aggCollation = pymongo.collation.Collation(locale = 'en' , strength = 1, alternate = 'shifted')

class FootballDB:


    def __init__(self, connectString, dbname):
        try:
            dbclient = pymongo.MongoClient(connectString)
        except (pymongo.errors.ConnectionFailure, pymongo.errors.InvalidURI):
            logger.critical("Unable to connect to " + connectString + dbname)
        except Exception as e:
            logger.critical("Issue connecting to DB: " + connectString + " " + dbname + " " + getattr(e, 'message', repr(e)))

        try:
            self.theDB = dbclient[dbname]
        except pymongo.errors.ConnectionFailure:
            logger.critical("Invalid database name provided:" + dbname + " ")
        except Exception as e:
            logger.critical("Issue with DB on startup:" + dbname + " " + getattr(e, 'message', repr(e)))

        try:
            self.tenancy = self.theDB["MultiTenancy"]
            #self.payments = self.theDB["STUB_PaymentsAndTransfersX"]
            #self.games = self.theDB["STUB_GamesX"]
            #self.adjustments = self.theDB["STUB_BasePlayerAdjustX"]
            #self.teamSummary = self.theDB["STUB_TeamSummaryX"]
            #self.teamPlayers = self.theDB["STUB_TeamPlayersX"]
            #self.teamSettings = self.theDB["STUB_TeamSettingsX"]
        except pymongo.errors.CollectionInvalid:
            logger.critical("Missing footballDB collections encountered - check.")
        except Exception as e:
            logger.critical("Issue getting collections in footballDB. " + getattr(e, 'message', repr(e)))

        if list(self.tenancy.find()) == None:
            logger.warning("Tenancy collection is empty. Fault or new deployment of service")


    def loadTeamTablesForUserId(self, userID):
        # look up tenancies for the userID, and map onto the default tenancyID.
        if userID == None: return(False)   # no metadata for this user so new session needed. redirect to onboarding

        try:
            team = self.tenancy.find_one( { "$and" : [ {"userID" : userID , "default" : True} ]},
                                   { "tenancyID" : 1})
            if (team == None):
                logger.warning("User ID " + str(userID) + "has no tenancies. May be new user")
                return(False)

            paymentsCollection = team.get("tenancyID") + "_payments"

            self.payments = self.theDB[paymentsCollection]
            self.games = self.theDB[team.get("tenancyID") + "_games"]
            self.adjustments = self.theDB[team.get("tenancyID") + "_adjustments"]  # unused for non-google imported accounts if ever supported
            self.teamSummary = self.theDB[team.get("tenancyID") + "_teamSummary"]
            self.teamPlayers = self.theDB[team.get("tenancyID") + "_teamPlayers"]
            self.teamSettings = self.theDB[team.get("tenancyID") + "_teamSettings"]
        except Exception as e:
            logger.critical("Unable to load and initialise tenancy data")
            return(False)

        return(True)

    def addTeam(self, teamName, userID):
        # first check if team exists, it should not as form should have rejected it. Add default key as well as team
        # name to the collection
        if userID != None:
            if self.tenancy.find_one( { "teamName" : teamName}) != None:
                message = "Team name " + teamName + " already exists. Re-enter tem name from Settings"
                logger.warning(message)
            else:
                self.tenancy.insert(dict(
                    userID=userID,
                    tenancyID=hex(int(datetime.datetime.now().timestamp() * 1000)),
                    teamName=teamName,
                    default=True))

                # load user collections so we can append teamSettings
                self.loadTeamTablesForUserId(userID)
                firstSetting=dict(teamName=teamName)
                settings = []
                settings.append(firstSetting)
                self.populateTeamSettings(settings)
                message = "Team " + teamName + " configured. Please add new players"
                logger.info("Team " + teamName + " configured on tenancy collection for user" + userID)
        else:
            logger.warning("addTeam did not add team when userID is not set")
            message="Internal error. Could not add team"

        return(message)


    def getListofAllTenantNames(self):

        teams = []
        try:
            teams = list(self.tenancy.find({}, { "_id" : 0, "teamName" : 1}))
            if len(teams) == 0:
                logger.info("getListofAllTenantNames(): Teams list is empty")
            else:
                logger.info("Teams list is ", teams.join(','))
        except Exception as e:
            logger.critical("Could not execute find on tenancy collection" + getattr(e, 'message', repr(e)))

        return(teams)


    def populatePayments(self, paymentHistory):
        # payments should be a list of dicts for each record

        self.payments.drop()
        logger.info("Dropping payments collection in populatePayments()")
        try:
            insertedIDs = self.payments.insert_many(paymentHistory)
        except pymongo.errors.OperationFailure as e:
            logger.critical("Unable to insert data into Payments table in populatePayments()")
            logger.critical(e.code + e.details)

    def populateGames(self, playedGames):
        # games should be a list of dicts for each record. This call replaces existing data.
        self.games.drop()
        logger.info("Dropping games collection in populateGames()")
        try:
            insertedIDs = self.games.insert_many(playedGames)
        except pymongo.errors.OperationFailure as e:
            logger.critical("Unable to insert data into Games collection")
            logger.critical(e.code +  e.details)

    def populateAdjustments(self, newAdjustments):

        self.adjustments.drop()
        logger.info("Dropped adjustments collection in populateAdjustments()")
        try:
            insertedIDs = self.adjustments.insert_many(newAdjustments)
        except pymongo.errors.OperationFailure as e:
            logger.critical ("Unable to insert data into Adjustments")
            logger.critical(e.code, e.details)

    def getAllAdjustments(self):
        try:
            adjustments = list(self.adjustments.find({}))
        except pymongo.errors.OperationFailure as e:
            logger.critical ("Unable to insert data into Adjustments")
            logger.critical(e.code, e.details)

        return(adjustments)

    def calcPopulateTeamSummary(self, players):

        self.teamSummary.drop()
        logger.info("Dropped teamSummary collection in calcPopulateTeamSummary()")
        team = []
        aggregatedPayments = self.getAggregatedPayments()

        for player in players:
            totalCost = 0
            gamesPlayed = 0

            try:
                for x in self.games.find({player: {"$in": ["Win", "Lose", "Draw", "No Show"]}}, collation=aggCollation):
                    gameCost = float(x.get("Cost of Game").to_decimal()) / float(x.get("Players"))
                    totalCost = totalCost + gameCost
                    gamesPlayed += 1
            except pymongo.errors.OperationFailure as e:
                logger.error("Unable to process game query for player")
                logger.error(e.code + e.details)

            # 8th June 2020 - now check player_guest key games and add up guest costs
            try:
                playerGuests = player + "_guests"
                for x in self.games.find({playerGuests : { "$exists" : 1 }}, collation=aggCollation):
                    gameCost = float(x.get("Cost of Game").to_decimal()) / float(x.get("Players"))
                    totalCost = totalCost + (x.get(playerGuests) * gameCost)
            except pymongo.errors.OperationFailure as e:
                logger.error("Unable to process playerGuests query for player")
                logger.error(e.code + e.details)

            try:
                try:
                    adjustAmount = self.adjustments.find_one({"name": player}, {"_id": 0, "adjust": 1})["adjust"]
                except TypeError:
                    # Player does not have a adjustment listed, so default to 0
                    adjustAmount = Decimal128("0.00")
            except pymongo.errors.OperationFailure as e:
                logger.error("Problem with adjustment amount for player " + player)
                logger.error(e.code + e.details)

            # work out last played date via games "Date of Game dd-MMM-YYYY"
            try:
                lastPlayedDate = list(self.games.find({player: {"$in": ["Win", "Lose", "Draw", "No Show"]}},
                                                     { "_id" : 0, "Date of Game dd-MON-YYYY": 1},
                                                     collation=aggCollation)\
                    .sort("Date of Game dd-MON-YYYY", -1).limit(1))
                if len(lastPlayedDate) == 0:
                    lastPlayedDate.append({"Date of Game dd-MON-YYYY": datetime.datetime(1970, 1, 1, 0, 0)})
            except ValueError:
                logger.warning("Unable to find last played date for player" + player)
                lastPlayedDate = [];
                lastPlayedDate.append({ "Date of Game dd-MON-YYYY" : datetime.datetime(1970,1,1,0,0)})
                #lastPlayedDate will be a list of a single element of datetime.datetime
                logger.warning(pp.pprint(lastPlayedDate))

            # the player may have never made any payments so provide default for aggregatedPayments for the player key if it doesn't exist
            try:
                team.append(dict(playerName=player,
                             gamesAttended=gamesPlayed,
                             lastPlayed=lastPlayedDate[0].get("Date of Game dd-MON-YYYY"),
                             gamesCost=Decimal128(str(totalCost)),
                             moniespaid=aggregatedPayments.get(player, Decimal128("0.00")),
                             balance=Decimal128(str(float(aggregatedPayments.get(player, Decimal128("0.00")).to_decimal()) -
                                                             totalCost + float(adjustAmount.to_decimal())))
                                      )
                                 )
            except pymongo.errors.OperationFailure as e:
                logger.error("Problem with a player when adding their summary:")
                logger.error(e.code + e.details)
        try:
            insertedIDs = self.teamSummary.insert_many(team)
        except pymongo.errors.OperationFailure as e:
            logger.error("Problem with inserting teamSummary in DB")
            logger.error(e.code +  e.details)

    def populateTeamPlayers(self, players):
        # playerName, comment
        self.teamPlayers.drop()
        try:
            insertedIDs = self.teamPlayers.insert_many(players)
        except pymongo.errors.OperationFailure as e:
            logger.critical ("Unable to insert players into teamPlayers collection")
            logger.critical(e.code + e.details)

    def populateTeamSettings(self, settings):
        # TeamName
        self.teamSettings.drop()
        try:
            insertedIDs = self.teamSettings.insert_many(settings)
        except pymongo.errors.OperationFailure as e:
            logger.critical ("Unable to insert settings into teamSettings collection")
            logger.critical(e.code + e.details)

    def playerExists(self, playerName):
        # check if player name exists, return true/false
        player = self.teamSummary.find_one( { "playerName" : playerName } )
        if player != None:
            if player.get("playerName", None) == playerName:
                return (True)
        return (False)

    def addPlayer(self, player):
        # append new document to summary table if player.name is unique
        allPlayers = self.getPlayerLabels()
        if player.playerName in allPlayers:
            # player already exists
            message = "Player " + str(player.playerName) + " already exists!"
            return(message)

        self.teamPlayers.insert(dict(
            playerName=player.playerName,
            comment=player.comment,
            retiree=player.retiree))

        self.teamSummary.insert(dict(
            playerName=player.playerName,
            gamesAttended=0,
            lastPlayed=datetime.datetime(1970, 1, 1, 0, 0),
            gamesCost=Decimal128("0.00"),
            moniespaid=Decimal128("0.00"),
            balance=Decimal128("0.00")))

        message = "Player " + str(player.playerName) + " added to System!"
        logger.info(message)
        return (message)

    def editPlayer(self, oldplayerName, player):
        #  player is a footballClass.
        # if name change go through every Game + TeamSummary + TeamPlayers + transactions + adjustments and update every key. Not nice.

        # title the new player name to make sure we are consistent.
        titledPlayerName = player.playerName.title()
        player.playerName = titledPlayerName

        if oldplayerName != player.playerName:
            ourgames = self.games.find({})
            for game in ourgames:
                if oldplayerName in game:
                    value = game.get(oldplayerName)
                    self.games.update_one({"_id": game.get("_id")},
                                                {"$unset": { oldplayerName : ""}})
                    logger.debug("Removed player" + oldplayerName + " from game " + str(game.get("_id")))
                    self.games.update_one({"_id": game.get("_id")},
                                          {"$set": {player.playerName : value}})
                    logger.debug("Added player" + player.playerName + " to game " + str(game.get("_id")))

                guest = oldplayerName + "_guests"
                if guest in game:
                    newguest = player.playerName + "_guests"
                    value = game.get(guest)
                    self.games.update_one({"_id": game.get("_id")},
                                                {"$unset": { guest : ""}})
                    logger.debug("Removed guests for " + guest + " with id" + str(game.get("_id")))
                    self.games.update_one({"_id": game.get("_id")},
                                          {"$set": { newguest : value}})
                    logger.debug("Added guest for " + newguest + "with game id " + str(game.get("_id")))

                # done - need to separate into comma separate values, then check and rebuild as Mark and Mark D will clash
                newplayerList = game.get("PlayerList").replace(oldplayerName, player.playerName)
                if newplayerList != game.get("PlayerList"):
                    playerNameList = game.get("PlayerList").split(',')
                    playerNameList = [player.playerName if w == oldplayerName else w for w in playerNameList]

                    guest = oldplayerName + "_guests"
                    newguest = player.playerName + "_guests"
                    playerNameList = [newguest if w == guest else w for w in playerNameList]

                    self.games.update_one({"_id": game.get("_id")}, {"$set": { "PlayerList" : ','.join(playerNameList)}})
                    logger.info("PlayerList modded for oldplayerName" + oldplayerName + str(game.get("_id")))

            # now teamSummary
            team = self.teamSummary.find({})
            for ourplayer in team:
                if oldplayerName == ourplayer.get("playerName", "None"):
                    self.teamSummary.update_one({"_id" : ourplayer.get("_id")}, {"$set": { "playerName" : player.playerName}})
                    logger.debug("Updated teamSummary" + str(ourplayer.get("_id")) + "for player " + oldplayerName + " and changed name to " + player.playerName)

            # now transactions
            transactions = self.payments.find({})
            for transaction in transactions:
                if oldplayerName == transaction.get("Player", "None"):
                    self.payments.update_one({"_id" : transaction.get("_id")}, {"$set": { "Player" : player.playerName}})
                    logger.debug("Updated transaction " + str(transaction.get("_id")) + " for player " + oldplayerName + " and changed name to " + player.playerName)

            # now teamPlayers
            team = self.teamPlayers.find({})
            for ourplayer in team:
                if oldplayerName == ourplayer.get("playerName", "None"):
                    self.teamPlayers.update_one({"_id": ourplayer.get("_id")}, {"$set": { "playerName" : player.playerName}})
                    logger.debug("Updated teamPlayer " + str(ourplayer.get("_id")) + " for player " + oldplayerName + " and changed name to " + player.playerName)

            message = "Updated CFFA database from " + oldplayerName + " to "+ player.playerName + "!"
        else:
            message = "Updated player " + player.playerName + " details"

        # in all cases we update retiree and comment with whatever is passed - doesn't matter to check if they have actually changed.
        self.teamPlayers.update_one({ "playerName" : player.playerName }, { "$set" :
                                                                                     { "retiree" : player.retiree,
                                                                                       "comment" : player.comment
                                                                                 }})

        logger.info(message)
        return(message)

    def retirePlayer(self, playerName):
        try:
            self.teamPlayers.update_one({ "playerName" : playerName}, {"$set":{"retiree" : True}})
        except pymongo.errors.OperationFailure as e:
            logger.critical("Unable to reactivate player in teamPlayers " + playerName)
            logger.critical(e.code +  e.details)
            message = "Could not retire player " + playerName
            logger.info(message)
            return(message)

        message = "Retired player " + playerName
        logger.info(message)
        return (message)

    def reactivatePlayer(self, playerName):
        try:
            self.teamPlayers.update_one({"playerName" : playerName}, {"$set": {"retiree": False}})
        except pymongo.errors.OperationFailure as e:
            logger.critical("Unable to reactivate player in teamPlayers " + playerName)
            logger.critical(e.code + e.details)
            message = "Could not reactivate player " + playerName
            logger.info(message)
            return (message)

        message = "Reactivated player " + playerName
        logger.info(message)
        return (message)

    def addGame(self, newgame):
        # adds new game to DB
        logger.debug("We got to addGame()!")
        # newgame is a footballClasses game

        # append new game into game collection. treat everything as a 1-1 draw. no goals for players. need to populate
        # draw against the player name field. No need to populate any keys for players who didn't attend.
        # will log a playerNameGuests key where != 0.
        # once game is appended, recalculate summary table for impacted players.

        gameRecord = {}

        gameRecord["Timestamp"] = datetime.datetime.now()
        #gameRecord["Players"] = newgame.currentActivePlayers
        # cannot use the above as this does not include number of guests - need to set this later on
        gameRecord["Winning Team Score"] = 1
        gameRecord["Losing Team Score"] = 1
        gameRecord["Date of Game dd-MON-YYYY"] = datetime.datetime(newgame.gameDate.year, newgame.gameDate.month, newgame.gameDate.day)
        gameRecord["Cost of Game"] = Decimal128(str(newgame.gameCost))

        teamString = []
        totalPlayersThisGame = 0
        for player in newgame.playerList:
            if player.playedLastGame == True:
                gameRecord[player.playerName] = "Draw"
                teamString.append(player.playerName)
                totalPlayersThisGame +=1
            if player.guests > 0:
                guestKey = player.playerName + "_guests"
                gameRecord[guestKey] = player.guests
                teamString.append(player.playerName + "_has_" + str(player.guests) + "_guests")
                totalPlayersThisGame+=player.guests

        gameRecord["PlayerList"] = ",".join(teamString)

        gameRecord["Players"] = totalPlayersThisGame
        costEach = float(newgame.gameCost) / float(totalPlayersThisGame)
        gameRecord["Cost Each"] = Decimal128(str(costEach))
        gameRecord["Booker"] = newgame.booker

        gameRecord["CFFA"] = "Record submitted by CFFA user"

        self.games.insert(gameRecord)

        # now update summary collection for each player that played and/or has guests in newgame.playerList
        # then handle booker and cost of game

        for player in newgame.playerList:
            playerRecord = self.teamSummary.find_one({"playerName" : player.playerName}, collation=aggCollation)
            if playerRecord == None and player.playerName != "":
                # ok we didn't find this player so hopefully will be new! let's set the record to zeros
                newPlayer = footballClasses.TeamPlayer(player.playerName, False, "Created from a New Game")
                self.addPlayer(newPlayer)
                playerRecord = self.teamSummary.find_one({"playerName": player.playerName}, collation=aggCollation)
                if (playerRecord == None):
                    logger.critical("addGame(): After adding player, player does not exist in teamSummary")
                    return(False)

            if player.playedLastGame:
                logger.info("Played Player:" +  player.playerName + " gamesCost is " + str(playerRecord.get("gamesCost").to_decimal()))
                previousCost = playerRecord.get("gamesCost")
                del playerRecord["gamesCost"]
                playerRecord["gamesCost"] = Decimal128(str(float(previousCost.to_decimal()) + costEach))
                logger.info(player.playerName + " cost of all games:" + str(previousCost.to_decimal()) + " to " +  str(playerRecord.get("gamesCost").to_decimal()))

                previousBalance = playerRecord.get("balance")
                del playerRecord["balance"]
                playerRecord["balance"] = Decimal128(str(float(previousBalance.to_decimal()) - costEach))
                logger.info(player.playerName + " balance:" + str(previousBalance.to_decimal()) + " to " + str(playerRecord.get("balance").to_decimal()))

                playerRecord["lastPlayed"] = datetime.datetime(newgame.gameDate.year, newgame.gameDate.month,
                                                                  newgame.gameDate.day)
                playerRecord["gamesAttended"] = playerRecord.get("gamesAttended") + 1
                # now update DB player summary record
                self.teamSummary.update_one({"playerName" : player.playerName},
                                             { "$set": {
                                                 "gamesCost" : playerRecord.get("gamesCost"),
                                                 "balance" : playerRecord.get("balance"),
                                                 "lastPlayed" : playerRecord.get("lastPlayed"),
                                                 "gamesAttended" : playerRecord.get("gamesAttended") }})

            if player.pitchBooker:
                # add booking credit to transactions list as well
                previousBalance = playerRecord.get("balance")
                del playerRecord["balance"]
                playerRecord["balance"] = Decimal128(str(float(previousBalance.to_decimal()) + float(newgame.gameCost)))
                logger.info("Booker Player:" + player.playerName + " balance is " + str(playerRecord.get("balance").to_decimal()))

                self.teamSummary.update_one({"playerName": player.playerName},
                                             { "$set": {
                                                 "balance": playerRecord.get("balance")
                                             }})
                transactionDocument = {}
                transactionDocument["Player"] = player.playerName
                transactionDocument["Type"] = "CFFA Booking Credit"
                transactionDocument["Amount"] = Decimal128(str(float(newgame.gameCost)))
                transactionDocument["Date"] = datetime.datetime(newgame.gameDate.year, newgame.gameDate.month,
                                                                  newgame.gameDate.day)
                self.payments.insert(transactionDocument)
                logger.info("Booker " + player.playerName + "transaction added for booking credit of " + str(float(newgame.gameCost)))

            if player.guests > 0:
                previousBalance = playerRecord.get("balance")
                del playerRecord["balance"]
                playerRecord["balance"] = Decimal128(str(float(previousBalance.to_decimal()) -
                                                         ( costEach * player.guests )))
                logger.info("Guests for player:" + player.playerName)

                self.teamSummary.update_one({"playerName": player.playerName},
                                             { "$set": {
                                                 "balance": playerRecord.get("balance")
                                             }})
        return(True)

    def editGame(self, dbid, editgameform):
        # dbid must be set and exists
        logger.debug("We got to edit game")

        gameRecord = self.games.find_one({ "_id" : dbid})

        # start updating each old record with content in editgameform
        gameRecord["Timestamp"] = datetime.datetime.now()
        gameRecord["Date of Game dd-MON-YYYY"] = datetime.datetime(editgameform.gameDate.year,
                                                                   editgameform.gameDate.month,
                                                                   editgameform.gameDate.day)
        originalCostOfGame = gameRecord.get("Cost of Game")
        gameRecord["Cost of Game"] = Decimal128(str(editgameform.gameCost))

        teamString = []
        totalPlayersThisGame = 0
        for player in editgameform.playerList:
            # first check if any player is new
            playerRecord = self.teamSummary.find_one({"playerName" : player.playerName}, collation=aggCollation)
            if playerRecord == None and player.playerName != "":
                # ok we didn't find this player so hopefully will be new! let's set the record to zeros
                newPlayer = footballClasses.TeamPlayer(player.playerName, False, "Created from an Edited Game")
                self.addPlayer(newPlayer)
                playerRecord = self.teamSummary.find_one({"playerName": player.playerName}, collation=aggCollation)
                if (playerRecord == None):
                    logger.critical("addGame(): After adding player, player does not exist in teamSummary")
                    return(False)

            if player.playedLastGame == True:
                gameRecord[player.playerName] = "Draw"
                teamString.append(player.playerName)
                totalPlayersThisGame +=1
            else:
                gameRecord.pop(player.playerName, None)

            guestKey = player.playerName + "_guests"
            if player.guests > 0:
                teamString.append(player.playerName + "_has_" + str(player.guests) + "_guests")
                totalPlayersThisGame+=player.guests
                gameRecord[guestKey] = player.guests
            else:
                gameRecord.pop(guestKey, None)

            # check for guest changes.

        gameRecord["PlayerList"] = ",".join(teamString)

        # TO DO: Go through teamString again, and if any key has a value (that is set to Draw, Win, Lose, No Show, No Play)
        # but is not in the teamString list we need to pop. This is bad as someones name could be Cost of Game and this
        # would cause lots of issues. Players should be a list in the dict!
        keysToPop = []
        for key in gameRecord.keys():
            if gameRecord.get(key, None) in ["Win", "win", "Draw", "draw", "Lose", "lose", "no show", "No Show", "no play", "No Play"]:
                #now check if in teamstring
                if key not in teamString:
                    keysToPop.append(key)
                    logger.info("Removing player " + key + " from game on " +
                                str(gameRecord.get("Date of Game dd-MON-YYYY").year) + "/" +
                                str(gameRecord.get("Date of Game dd-MON-YYYY").month) + "/" +
                                str(gameRecord.get("Date of Game dd-MON-YYYY").day) )

        for key in keysToPop:
            gameRecord.pop(key, None)

        gameRecord["Players"] = totalPlayersThisGame
        costEach = float(editgameform.gameCost) / float(totalPlayersThisGame)
        gameRecord["Cost Each"] = Decimal128(str(costEach))
        originalBooker = gameRecord.get("Booker")
        gameRecord["Booker"] = editgameform.booker

        gameRecord["CFFA"] = "Record edited by CFFA user"

        # if we update then we need to add every player status and guests, long document.
        # Instead delete the game document and insert of a new one for now.
        #self.games.update( { "_id": dbid}, {"$set" : gameRecord}, upsert=False)
        self.games.delete_one({"_id" : dbid})
        logger.debug("Deleted game during edit with dbid" + str(dbid))
        self.games.insert(gameRecord)
        logger.debug("Inserted new edited game record:" + " ".join(teamString))
        # sort out impact on costs.
        # easiest to resync all costs on all historical games instead of add/removing costs on a per player  basis
        # added guests field into games to capture guests
        # but need to update transactions for booker if that has changed.

        dateString = str(editgameform.gameDate.year) + "/" + str(editgameform.gameDate.month) + "/" + str(editgameform.gameDate.day)

        if originalBooker != gameRecord.get("Booker") or originalCostOfGame != gameRecord.get("Cost of Game"):
            #add transaction to remove original booker credit with original cost of game then
            #add transaction to add new cost of booking with new (or same) booker)
            logger.debug("editGame(): Cost of game or change of booker")

            transactionDocument = {}
            transactionDocument["Player"] = originalBooker
            transactionDocument["Type"] = "CFFA Game Edit for " + dateString + ". Booker change - remove original game credit"
            transactionDocument["Amount"] = Decimal128(str(float(0 - originalCostOfGame.to_decimal())))
            transactionDocument["Date"] = datetime.datetime.now()
            self.payments.insert(transactionDocument)
            logger.debug("inserted new transaction for " + originalBooker + " to remove credit for this player" )

            transactionDocument = {}
            transactionDocument["Player"] = gameRecord.get("Booker")
            transactionDocument["Type"] = "CFFA Game Edit for " + dateString + ". Booker change - add new game credit"
            transactionDocument["Amount"] = Decimal128(str(float(editgameform.gameCost)))
            transactionDocument["Date"] = datetime.datetime.now()
            self.payments.insert(transactionDocument)
            logger.debug("inserted new transaction for " + gameRecord.get("Booker") + " to add booking credit for this player")

        playerDict = list(self.teamSummary.find({}, { "playerName" : 1}))
        playerList = []
        for player in playerDict:
            playerList.append(player.get("playerName"))

        self.calcPopulateTeamSummary(playerList)

        return(True)

    def deleteGame(self, dbid):
        # not just delete Game record (dbid), but also refund transaction (log in transaction) for booker. Then recalculate summary table.
        gameDocument = self.games.find_one({"_id" : dbid})

        transactionDocument = {}
        gameDate = gameDocument.get("Date of Game dd-MON-YYYY")
        dateString = str(gameDate.year) + "/" + str(gameDate.month) + "/" + str(gameDate.day)
        deleteMessage = ""

        transactionDocument["Player"] = gameDocument.get("Booker")
        transactionDocument["Date"] = datetime.datetime.now()

        if "Booker" in gameDocument:

            transactionDocument["Type"] = "CFFA Game Deletion for " + dateString +". Booker removal - game credit"
            transactionDocument["Amount"] = Decimal128(str( 0 - float(gameDocument.get("Cost of Game").to_decimal())))
            deleteMessage = "Game " + dateString + " deleted and transactions adjusted."
            logging.debug("Booking credit for booker" + gameDocument.get("Booker") + " removed as game is being deleted")

        else:
            deleteMessage = "Warning: Game had no booker - will need manual review of past transactions to remove booker credit"
            transactionDocument["Type"] = "CFFA Game Deletion for " + dateString + ". No booker set: no booker credit."
            transactionDocument["Amount"] = Decimal128("0.00")
            logger.warning("There was no booker for deleted game. Maybe imported game.")

        self.payments.insert(transactionDocument)
        logger.debug("Inserted new transaction to remove booking credit")

        self.games.delete_one({"_id" : dbid})

        playerDict = list(self.teamSummary.find({}, {"playerName": 1}))
        playerList = []
        for player in playerDict:
            playerList.append(player.get("playerName"))

        self.calcPopulateTeamSummary(playerList)

        return(deleteMessage)

    def getAggregatedPayments(self):
        aggregatedPayments = {}
        try:
            aggCursor = self.payments.aggregate( [ {"$group" : { "_id": "$Player" , "sum" : { "$sum": "$Amount"} } }
                                ], collation=aggCollation)
            for x in list(aggCursor):
                aggregatedPayments[x.get("_id")] = x.get("sum")
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not aggregate payments")
            logger.critical(e.code + e.details)

        return(aggregatedPayments)

    def getActivePlayerSummary(self):
        activePlayers = []
        cutoffDate = datetime.date.today() - datetime.timedelta(days=activeDays)
        cutoffDateTime = datetime.datetime(cutoffDate.year, cutoffDate.month, cutoffDate.day)
        try:
            activePlayers = list(self.teamSummary.find({"lastPlayed": { "$gte": cutoffDateTime}}, { "_id": 0}))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not return summary")
            logger.critical(e.code, e.details)

        #for checkPlayer in playersInDB:
        #    daysSincePlayed = datetime.date.today() - checkPlayer.get("lastPlayed").date()

        #    if daysSincePlayed.days < activeDays:
        #        activePlayers.append(checkPlayer)

        return(activePlayers)

    def getFullSummary(self):
        allPlayers = []
        try:
            #allPlayers = list(self.teamSummary.find({}, { "_id": 0}))
            allPlayers = list(self.teamSummary.find({}))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not return summary")
            logger.critical(e.code + e.details)

        return(allPlayers)

    def getRecentGames(self):
        # should convert the string in the date field during population to date or ISODate then find() can do the filter.
        # instead we get all games then then loop through them, slower and not efficient.

        gamesInDB = []
        # work out datetime for cutoff.
        cutoffDate = datetime.date.today() - datetime.timedelta(days=activeDays)
        cutoffDateTime = datetime.datetime(cutoffDate.year, cutoffDate.month, cutoffDate.day)
        try:
            gamesInDB = list(self.games.find({"Date of Game dd-MON-YYYY": { "$gte": cutoffDateTime}}, { "_id": 0}))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get list of games in getRecentGames()")
            logger.critical(e.code + e.details)

        return (gamesInDB)

    def getAllGames(self):
        # sort on date, latest first
        gamesInDB = []
        try:
            gamesInDB = list(self.games.find({}).sort("Date of Game dd-MON-YYYY", -1))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get list of games in getAllGames()")
            logger.critical(e.code + e.details)

        return (gamesInDB)


    def getRecentTransactions(self):
        recentTransactions = []
        cutoffDate = datetime.date.today() - datetime.timedelta(days=daysForRecentPayment)
        cutoffDateTime = datetime.datetime(cutoffDate.year, cutoffDate.month, cutoffDate.day)

        try:
            recentTransactions = list(self.payments.find({"Date": { "$gte": cutoffDateTime}}, { "_id": 0}))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get list of transactions in getRecentTransactions()")
            logger.critical(e.code + e.details)

        return (recentTransactions)


    def getAllTransactions(self):
        allTransactions=[]
        try:
            allTransactions = list(self.payments.find({}).sort("Date", -1))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get list of transactions in getAllTransactions()")
            logger.critical(e.code + e.details)

        return (allTransactions)


    def getActivePlayersForNewGame(self):
        # return list dict of players containing keys of "name" and
        # "lastGamePlayed" value 1 (default checked) or 0 (not checked)
        activePlayers = []
        # work out datetime for cutoff.
        cutoffDate = datetime.date.today() - datetime.timedelta(days=activeDays)
        cutoffDateTime = datetime.datetime(cutoffDate.year, cutoffDate.month, cutoffDate.day)
        try:
            activePlayers = list(self.teamSummary.find({"lastPlayed": { "$gte": cutoffDateTime}}, { "playerName" : 1, "lastPlayed" : 1}))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get active Players from Summary in getActivePlayersForNewGame()")
            logger.critical(e.code + e.details)

        lastGame = self.getLastGameDetails()

        for player in activePlayers:
            if player["lastPlayed"] == lastGame[0].get("Date of Game dd-MON-YYYY"):
                player["lastGamePlayed"] = True # checked
            else:
                player["lastGamePlayed"] = False #not checked

        return (activePlayers)

    def getInactivePlayersForNewGame(self):
        # return list dict of players containing keys of "name"
        inactivePlayers = []
        # work out datetime for cutoff.
        cutoffDate = datetime.date.today() - datetime.timedelta(days=activeDays)
        cutoffDateTime = datetime.datetime(cutoffDate.year, cutoffDate.month, cutoffDate.day)
        try:
            inactivePlayers = list(self.teamSummary.find({"lastPlayed": {"$lt": cutoffDateTime}},
                                                       {"playerName": 1}))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get inactive Players from Summary in getInactivePlayersForNewGame()")
            logger.critical(e.code + e.details)

        return (inactivePlayers)


    def getLastGameDetails(self):
        # return the last game cost in Decimal128 in a single element list of dict with date and cost.
        lastPlayed = []
        try:
            lastPlayed = list(self.games.find({}, {"_id": 0, "Date of Game dd-MON-YYYY": 1, "Cost of Game": 1},
                                                  collation=aggCollation) \
                                  .sort("Date of Game dd-MON-YYYY", -1).limit(1))
            if len(lastPlayed) == 0:
                lastPlayed.append({"Date of Game dd-MON-YYYY": datetime.datetime(1970, 1, 1, 0, 0),
                                     "Cost of Game": Decimal128("0.00")})
        except ValueError:
            logger.warning("Unable to find last played date for player" + player)
            lastPlayed = []
            lastPlayed.append({"Date of Game dd-MON-YYYY": datetime.datetime(1970, 1, 1, 0, 0),
                              "Cost of Game": Decimal128("0.00")})
            # lastPlayedDate will be a list of a single element of datetime.datetime
            logger.warning(lastPlayed)

        return(lastPlayed)

    def getLastGameDBID(self):

        lastPlayed = list(self.games.find({}, {"_id": 1, "Date of Game dd-MON-YYYY": 1 }).sort("Date of Game dd-MON-YYYY", -1).limit(1))
        if len(lastPlayed) == 0:
            logger.warning("getLastGameDBIB(): no games found")
            return(0)

        return(lastPlayed[0].get("_id"))


    def getDefaultsForNewGame(self, loggedInUser):
        newgameplayers = []

        activePlayers = self.getActivePlayersForNewGame()

        count=0
        for activePlayer in activePlayers:
            if loggedInUser == activePlayer.get("playerName"):
                likelyBooker = True
            else:
                likelyBooker = False

            newplayer = footballClasses.Player(activePlayer.get("_id"),
                                               activePlayer.get("playerName"),
                                               activePlayer.get("lastGamePlayed"), likelyBooker, 0)
            newgameplayers.append(newplayer)
            count+=1

        # if supplied players are less than 10, append blank defaults for the new game form.
        if count<10:
            for x in range (count, 10):
                newplayer = footballClasses.Player("empty", "", False, False, 0)
                newgameplayers.append(newplayer)

        lastGame = self.getLastGameDetails()

        newGame = footballClasses.Game(lastGame[0].get("Cost of Game"), datetime.datetime.date(datetime.datetime.now()), newgameplayers, "")

        return(newGame)


    def dateofGame(self, gamedbid):
        gameDate = self.games.find_one({ "_id" : gamedbid}, { "Date of Game dd-MON-YYYY" : 1})
        ourDate = ""
        if gameDate != None:
            ourDate = gameDate.get("Date of Game dd-MON-YYYY")
        return(ourDate)


    def getGameDetailsForEditDeleteForm(self, gamedbid, long):
       gamePlayers = []
       allPlayers = self.getAllPlayers()   # from summary table
       count=0
       for player in allPlayers:
           if player.get("playerName") == self.checkGameForBooker(gamedbid):
               booker=True
           else:
               booker=False

           if self.didPlayerPlayThisGame(gamedbid, player.get("playerName")):
               playedGame=True
           else:
               playedGame=False

           guests = self.checkGameForGuests(gamedbid, player.get("playerName"))
           if long or booker or playedGame or guests > 0:
               addplayer = footballClasses.Player(player.get("_id"),
                                              player.get("playerName"),
                                              playedGame,
                                              booker,
                                              guests)
               gamePlayers.append(addplayer)
           count+=1

       # if supplied players are less than 10, append blank defaults for the new game form.
       if count < 10:
           for x in range(count, 10):
               blankplayer = footballClasses.Player("empty", "", False, False, 0)
               gamePlayers.append(blankplayer)

       return(self.getGameFromDB(gamedbid, gamePlayers))


    def newManager(self):
        # checks if a manager has only submitted 3 game or less, if so return True
        allGames = list(self.games.find())
        if len(allGames) <= 3:
            return(True)

        return(False)


    def getAllPlayers(self):
       allPlayers=[]
       try:
           allPlayers = list(self.teamSummary.find({}, {"_id":1, "playerName":1, "lastGamePlayed":1}))
       except pymongo.errors.OperationFailure as e:
           logger.critical("Could not get All Players from Summary in getAllPlayers()")
           logger.critical(e.code + e.details)

       try:
           #inefficient - mongoDB is not relational!
           allPlayerDetails = list(self.teamPlayers.find({}))
           for player in allPlayers:
               for playerX in allPlayerDetails:
                   if player.get("playerName", None) == playerX.get("playerName", "Not Set"):
                        player["retiree"] = playerX.get("retiree", False)
                        player["comment"] = playerX.get("comment", "No comment set")
       except pymongo.errors.OperationFailure as e:
           logger.critical("Could not get/process All Players from teamPlayers in getAllPlayers()")
           logger.critical(e.code + e.details)

       return (allPlayers)


    def getGameFromDB(self, gamedbid, playerList):
       game = self.games.find_one({ "_id" : gamedbid}, { "Date of Game dd-MON-YYYY" : 1, "Cost of Game" : 1,
                                                         "PlayerList": 1, "Players": 1, "Booker": 1})

       ourGame=None
       booker = ""
       if game != None:
            if "Booker" in game:
                booker = game["Booker"]

            gameDate = datetime.datetime.date(game.get("Date of Game dd-MON-YYYY"))
            ourGame = footballClasses.Game(game.get("Cost of Game"),
                                           gameDate,
                                           playerList, booker )

       return(ourGame)


    def checkGameForBooker(self, gamedbid):
        game = self.games.find_one({"_id": gamedbid}, {"Booker": 1})
        booker = ""
        if game != None:
            if "Booker" in game:
                booker = game.get("Booker")

        return(booker)


    def checkGameForGuests(self, gamedbid, playerName):
        # need to traverse the playerList string for "<name>_has_X_guests"
        game = self.games.find_one({"_id": gamedbid}, {"PlayerList": 1})
        guests = 0
        if game != None:
            mustMatch = playerName + "_has_(\d)+_guests"
            regex = re.compile(mustMatch)
            r = regex.search(game.get("PlayerList"))
            if r != None:
                guests = r.group(1)

        return(guests)


    def didPlayerPlayThisGame(self, gamedbid, name):
        game = self.games.find_one({"_id": gamedbid}, { name: 1})
        guests = 0
        if game != None:
            if game.get(name) in ["Win", "win", "Draw", "draw", "Lose", "lose", "no show", "No Show"]:
                return(True)

        return(False)


    def getAllPlayerDetailsForPlayerEdit(self):
        # returns a list of teamPlayer objects
        allPlayers = []
        try:
            ourPlayers = list(self.teamSummary.find({}, {"_id": 1, "playerName": 1, "comment": 1, "retiree": 1}))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get All Players from Summary in getAllPlayerDetailsForPlayerEdit()")
            logger.critical(e.code + e.details)

        for player in ourPlayers:
            teamPlayer = footballClasses.TeamPlayer(player.get("playerName"),
                                                    player.get("retiree", False),
                                                    player.get("comment", "Blank"))
            allPlayers.append(teamPlayer)

        return (allPlayers)

    def getPlayerDefaultsForEdit(self, playerName):
        try:
            thisPlayer = self.teamPlayers.find_one({"playerName" : playerName})
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get the player in teamPlayers in getPlayerDefaultsForEdit()")
            logger.critical(e.code + e.details)

        player = footballClasses.TeamPlayer(playerName, thisPlayer.get("retiree", False), thisPlayer.get("comment", "Not set"))
        return(player)

    def getPlayerLabels(self):
        allPlayers = []
        try:
            ourPlayers = list(self.teamSummary.find({}, {"_id": 1, "playerName": 1}))
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get All Players from Summary in getAllPlayerDetailsForPlayerEdit()")
            logger.critical(e.code + e.details)

        for player in ourPlayers:
            allPlayers.append(player.get("playerName"))

        return (allPlayers)

    def shouldPlayerBeRetired(self, playerName):
        cutoffDate = datetime.date.today() - datetime.timedelta(days=activeDays)
        cutoffDateTime = datetime.datetime(cutoffDate.year, cutoffDate.month, cutoffDate.day)

        playerLastPlayed = self.teamSummary.find_one({ "playerName" : playerName}, {  "lastPlayed" : 1 })

        if playerLastPlayed.get("lastPlayed", datetime.datetime(1970,1,1,0,0)) < cutoffDateTime:
            return(True)

        return(False)

    def addTransaction(self, transaction):

        # assumes transaction.transactionDate is a datetime.date object, not datetime.datetime
        # assumes Amount is a float

        if self.playerExists(transaction.player):
            payment = {}
            payment["Player"] = transaction.player
            payment["Type"] = transaction.type
            payment["Amount"] = Decimal128(str(transaction.amount))   # was float
            payment["Date"] = datetime.datetime(transaction.transactionDate.year,
                                                transaction.transactionDate.month,
                                                transaction.transactionDate.day)

            try:
                self.payments.insert(payment)
                message = "Added transaction £" + str(transaction.amount) + " against " + transaction.player
            except pymongo.errors.OperationFailure as e:
                logger.critical("Could not add transaction in addTransaction()")
                logger.critical(e.code + e.details)
                message = "Internal error when adding transaction " + str(transaction.amount) + " against " + transaction.player
                logger.error(message)
                return(message)
        else:
            message = "Player " + transaction.player + " does not exist in system. Transaction not added"
            logger.error(message)
            return(message)

        # TO DO - update Summary table. ALso check if AutoPay has a duplicate
        playerRecord = self.teamSummary.find_one({"playerName": transaction.player}, collation=aggCollation)
        if playerRecord == None:
            # no record for player, this should not happen as it was in a select list. lets abort
            message = "Selected player" + transaction.player + " is not in teamSummary table. Did not adjust Summary"
            logger.error(message)
            return(message)

        # only balance and moniespaid needs to be adjusted - add transaction amount to both values
        currentBalance = float(playerRecord.get("balance", Decimal128("0.00")).to_decimal())
        currentPayments = float(playerRecord.get("moniespaid", Decimal128("0.00")).to_decimal())
        currentBalance+=transaction.amount
        currentPayments+=transaction.amount # transaction.amount is a float

        try:
            self.teamSummary.update_one({"playerName": transaction.player},
                                        {"$set": {
                                            "balance": Decimal128(str(currentBalance)),
                                            "moniespaid": Decimal128(str(currentPayments))}})
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not update summary table in  addTransaction()")
            logger.critical(e.code + e.details)
            message = "Internal error when updating summary after adding transaction " + str(
                transaction.amount) + " against " + transaction.player
            logger.critical(message)

        logger.info(message)
        return(message)


    def getAutoPayDetails(self, user):
        # return a transaction object with logged in user, amount and todays date set. type is AutoPay
        # however return a £0 credit if the transaction already exists

        lastgame = list(self.games.find({}).sort("Date of Game dd-MON-YYYY", -1).limit(1))
        if len(lastgame) == 1:
            roundedCostEach = round(lastgame[0].get("Cost Each").to_decimal(),2)
        else:
            roundedCostEach = "0.00"
        payment = footballClasses.Transaction(user, "CFFA AutoPay", float(roundedCostEach), datetime.date.today())

        return(payment)

    def getDefaultsForTransactionForm(self, player):
        #     player  type amount transactionDate
        lastgame = list(self.games.find({}).sort("Date of Game dd-MON-YYYY", -1).limit(1))
        if len(lastgame) == 1:
            transaction = footballClasses.Transaction(player, "Transfer", float(round(lastgame[0].get("Cost Each").to_decimal(),2)), datetime.date.today())
        else:
            transaction = footballClasses.Transaction(player, "Transfer", float("0.00"), datetime.date.today())

        return(transaction)


    def updateTeamName(self, newName, userID):
        settings = list(self.teamSettings.find({}))
        ourID = None
        for setting in settings:
            if setting.get("teamName", None) != None:
                ourID = setting.get("_id")
                currentTeam = setting.get("teamName", None)

        if ourID != None:
            try:
                self.teamSettings.update({"_id" : ourID},{"$set":{ "teamName" : newName }})
                message = "Successfully updated teamName from "+ currentTeam + " to " + newName
            except pymongo.errors.OperationFailure as e:
                logger.critical("Could not update teamSettings in  updateTeamName()")
                logger.critical(e.code + e.details)
                message = "Internal error when updating teamSettings name " + newname

            try:
                self.tenancy.update({"userID" : userID, "teamName" : currentTeam}, {"$set":{ "teamName" : newName}} )
            except Exception as e:
                logger.critical("Could not update tenancy for teamName " + currentTeam + " to team " + newName)
                message = "Internal error when updating database"

        logger.info(message)
        return(message)

    def getAppSettings(self):
        # this is ugly, must be a better way to populate object
        settings = list(self.teamSettings.find({}))
        teamName = None
        for setting in settings:
            if setting.get("teamName", None) != None:
                teamName = setting.get("teamName", None)

        ourSettings = footballClasses.CFFASettings(teamName)

        return(ourSettings)

    def getTeamSettings(self):
        try:
            allSettings = list(self.teamSettings.find())
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get/process settings from teamSettings in getTeamSettings()")
            logger.critical(e.code + e.details)

        return(allSettings)

    def getTeamPlayers(self):
        try:
            teamPlayers = list(self.teamPlayers.find())
        except pymongo.errors.OperationFailure as e:
            logger.critical("Could not get/process settings from teamPlayers in getTeamPlayers()")
            logger.critical(e.code + e.details)

        return(teamPlayers)