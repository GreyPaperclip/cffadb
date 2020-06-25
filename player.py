class Player:
    def __init__(self, name):
        self.name = name
        self.dateLastPlayed = "Never Played"
        self.totalPayments = int(0)
        self.numberOfGames = int(0)
        self.baseAdjustment = Decimal128("0.00")
        self.currentBalance = Decimal128("0.00")
        self.photo = ""   # filename for image
        self.bio = "New Player"
        self.status = "New"  # active | retired | dormant
        self.transactions [ {} ]  # empty list of dict pair paymentDate, paymentDesc, paymentValue

    #def checkDB(self):
        # return true/false if in DB
        # no, use DB class to do ths.
        #pass

    def setAllAttributes(self, attributes):
        # dont use this as attributes except for name are not private
        # takes dict list for each attribute.
        pass

    #def addPlayertoDB(self):
        # add record to DB if unique name
        # no, the DB object uses this to add person object to DB.
    #   pass

    def getName(self):
        return self.name

    def getLastPlayed(self):
        # connect to db to calculate
        pass

    def getTotalPayments(self):
        # connect to be to obtain
        pass

    def getGames(self):
        # return list of games played
        pass

    def getBaseAdjustment(self):
        # return original adjustment figure
        pass

    def getCurrentBalance(self):
        # return
        pass

