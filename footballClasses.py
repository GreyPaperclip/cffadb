# no logging req for just this class defs.

class Player:
    dbid = "No DB ID"
    playerName = "No Player set"
    playedLastGame = False
    pitchBooker = False
    guests = 0

    def __init__(self, dbid, name, playedLastGame, pitchBooker, guests):
        self.dbid = dbid
        self.playerName = name
        self.playedLastGame = playedLastGame
        self.pitchBooker = pitchBooker
        self.guests = guests

    def __repr__(self):
        return ('player(' + str(self.dbid) + ', ' +
                self.playerName + ', ' +
                str(self.playedLastGame) + ', ' +
                str(self.pitchBooker) + ', ' +
                str(self.guests))

class Game:
    gameCost = float (0.00)
    gameDate = ""
    playerList = []
    currentActivePlayers = 0
    booker = ""
    guests = 0

    def __init__(self, gameCost, gameDate, playerList, booker):
        self.gameCost = gameCost
        self.gameDate = gameDate
        self.playerList = playerList
        self.booker = booker
        self.currentActivePlayers = len(playerList)   # this is incorrect

    def __repr__(self):
        return ('game(' + str(self.gameCost.to_decimal()) + ', ' +
                str(self.gameDate) + ', ' +
                str(self.currentActivePlayers) + ', ' +
                repr(self.playerList) + ')')

    def convertToDB(self):
        # returns a dict but needs custom logic to convert playerlist accordingly.
        record = {}

class TeamPlayer:
    playerName = ""
    retiree = False
    comment = ""

    def __init__(self, playerName, retired, comment):
        self.playerName = playerName
        self.retiree = retired
        self.comment = comment

    def __repr__(self):
        return ('TeamPlayer(' + self.playerName + ', ' + self.retired + ', ' + self.comment + ')')

class Team:
    teamName = ""
    playerList = []

    def __init__(self, playerList):
        self.playerList = playerList

    def __repr__(self):
        return ('Team(' + self.teamName + "," + repr(self.playerList) + ')')


class Transaction:
    player = ""
    type = ""
    amount = 0.00   # we should be float.
    transactionDate = ""

    def __init__(self, player, type, amount, transactionDate):
        self.player = player
        self.type = type
        self.amount = amount
        self.transactionDate = transactionDate

    def __repr__(self):
        return ('transaction(' + self.player + ', ' +
                str(self.transactionDate) + ', ' +
                self.type + ', ' +
                str(self.amount.to_decimal()) + ')')

class CFFASettings:
    teamName = None

    def __init__(self, teamName):
        self.teamName = teamName

    def __repr__(self):
        return ('CFFASetting(' + self.teamName + ')')

