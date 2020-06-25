class Game:

    def __init__(self):
        self.gameDate = ""
        self.noPlayers = 0
        self.cost = Decimal128("0.00")
        self.players = [{}]
        self.individualcost = (float) 0.00
        self.dbid = ""   # db id obtained when added / populaed
            # list of dict, containing name +
                                            # result of win|lose|draw|no show
                                            # no of guests

    def set(self, gameDate, noPlayers, cost, player[], dbreference):
        pass

