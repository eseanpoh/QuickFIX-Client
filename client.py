import sys
import os 
import time
import quickfix as fix
import quickfix42 as fix42
import logging
from datetime import datetime
import random

class Client(fix.Application):
    orderID = 0
    execID = 0
    totalUSD = {'AAPL': 0, 'MSFT': 0, 'BAC': 0, 'Total USD': 0}
    pnl = 0
    totalSharesTraded = {'AAPL': 0, 'MSFT': 0, 'BAC': 0}
    brokerIDs = {}
    TESTLIST = []
    statuses = {
        "0": "New", 
        "1": "Partial fill", 
        "2": "Fill", 
        "3": "Done for day", 
        "4": "Canceled", 
        "5": "Replaced", 
        "6": "Pending Cancel (e.g. result of Order Cancel Request <F>)", 
        "7": "Stopped", 
        "8": "Rejected", 
        "9": "Suspended", 
        "A": "Pending New", 
        "B": "Calculated", 
        "C": "Expired", 
        "D": "Restated (ExecutionRpt sent unsolicited by sellside, with ExecRestatementReason <378> set)", 
        "E": "Pending Replace (e.g. result of Order Cancel/Replace Request <G>)"}
    global sessionID
    
    def onCreate(self, sessionID):
        print("Application created with session ID: " + sessionID.toString())
        return
    
    def onLogon(self, sessionID):
        self.sessionID = sessionID
        print("Logged on with session ID:" + sessionID.toString())
        return
    
    def onLogout(self, sessionID):
        print("Logged out of session ID: " + sessionID.toString())
        return
    
    def toAdmin(self, message, sessionID):
        # print("TO ADMIN: " + message.toString())
        return 
    
    def fromAdmin(self, message, sessionID):
        # print("FROM ADMIN: " + message.toString())
        return 
    
    def toApp(self, message, sessionID):
        print("SENT: " + message.toString())
        return
    
    def fromApp(self, message, sessionID):
        print("RECEIVED: " + message.toString())
        self.onMessage(message, sessionID)
        return
    
    def genOrderID(self):
        self.orderID = self.orderID + 1
        # return datetime.utcnow().strftime('%Y%m%d%H%M%S') + str(self.orderID)
        return str(self.orderID)
    
    def genExecID(self):
        self.execID = self.execID + 1
        # return datetime.utcnow().strftime('%Y%m%d%H%M%S') + str(self.execID)
        return str(self.execID)
    
    def getFieldValue(self, fieldType, message):
        if message.isSetField(fieldType.getField()):
            message.getField(fieldType)
            return str(fieldType.getValue())
        else:
            return None
        
    def addBrokerId(self, ClOrdId, brokerId):
        print(ClOrdId + ' = ' + brokerId)
        self.brokerIDs[ClOrdId] = brokerId

    def addMessageLine(self, messageString, fieldType, message):
        dataDict = fix.DataDictionary("./spec/FIX42.xml")
        newString = self.getFieldValue(fieldType, message)
        if newString != None:
            messageString += str(dataDict.getFieldName(fieldType.getTag(), "")[0]) + ": " + newString + "\n"
            # messageString += str(fieldType.getTag()) + ": " + newString + "\n"
        return messageString
    
    def onMessage(self, message, sessionID):
        msgType = fix.MsgType()
        message.getHeader().getField(msgType)
        # Reject (35=3)
        if msgType.getValue() == fix.MsgType_Reject:
            messageString = "Reject\n"
            messageString = self.addMessageLine(messageString, fix.RefSeqNum(), message)
            messageString = self.addMessageLine(messageString, fix.SessionRejectReason(), message)
            messageString = self.addMessageLine(messageString, fix.Text(), message)

        # Execution Report (35=8)
        if msgType.getValue() == fix.MsgType_ExecutionReport:
            messageString = "Execution Report\n"
            messageString = self.addMessageLine(messageString, fix.OrderID(), message)
            messageString = self.addMessageLine(messageString, fix.ClOrdID(), message)
            messageString = self.addMessageLine(messageString, fix.OrigClOrdID(), message)
            messageString = self.addMessageLine(messageString, fix.ExecID(), message)

            execTransType = self.getFieldValue(fix.ExecTransType(), message)
            if execTransType == "0":
                messageString += "ExecTransType: New\n"
            if execTransType == "1":
                messageString += "ExecTransType: Cancel\n"
            if execTransType == "2":
                messageString += "ExecTransType: Correct\n"
            if execTransType == "3":
                messageString += "ExecTransType: Status\n"
            
            messageString = self.addMessageLine(messageString, fix.ExecRefID(), message)

            if self.getFieldValue(fix.ExecType(), message) != None:
                if self.getFieldValue(fix.ExecType(), message) not in ["0", "1", "2"]:
                    self.TESTLIST += self.getFieldValue(fix.ExecType(), message)
                messageString += "ExecType: " + self.statuses.get(self.getFieldValue(fix.ExecType(), message)) + '\n'
            if self.getFieldValue(fix.OrdStatus(), message) != None:
                messageString += "OrdStatus: " + self.statuses.get(self.getFieldValue(fix.OrdStatus(), message)) + '\n'
            
            messageString = self.addMessageLine(messageString, fix.LastMkt(), message)
            messageString = self.addMessageLine(messageString, fix.LastPx(), message)
            messageString = self.addMessageLine(messageString, fix.LastShares(), message)
            messageString = self.addMessageLine(messageString, fix.Symbol(), message)
            messageString = self.addMessageLine(messageString, fix.Side(), message)
            messageString = self.addMessageLine(messageString, fix.OrderQty(), message)
            messageString = self.addMessageLine(messageString, fix.LeavesQty(), message)
            messageString = self.addMessageLine(messageString, fix.CumQty(), message)
            messageString = self.addMessageLine(messageString, fix.AvgPx(), message)
            messageString = self.addMessageLine(messageString, fix.Text(), message)

            self.calculateTotalTradingVolume(message)
            self.calculatePNL(message)
            self.addTotalShares(message)
            self.addBrokerId(self.getFieldValue(fix.ClOrdID(), message), self.getFieldValue(fix.OrderID(), message))
        #Order Cancel Reject (35=9)
        if msgType.getValue() == fix.MsgType_OrderCancelReject:
            messageString = "Order Cancel Reject\n"
            messageString = self.addMessageLine(messageString, fix.OrderID(), message)
            messageString = self.addMessageLine(messageString, fix.ClOrdID(), message)
            messageString = self.addMessageLine(messageString, fix.OrigClOrdID(), message)
            
            if self.getFieldValue(fix.OrdStatus(), message) != None:
                messageString += "OrdStatus: " + self.statuses.get(self.getFieldValue(fix.OrdStatus(), message)) + '\n'

            cxlRejResponseTo = self.getFieldValue(fix.CxlRejResponseTo(), message)
            if cxlRejResponseTo == "1":
                messageString += "CxlRejResponseTo: Order Cancel Request <F>\n"
            if cxlRejResponseTo == "2":
                messageString += "CxlRejResponseTo: Order Cancel/Replace Request <G>\n"

            messageString = self.addMessageLine(messageString, fix.Text(), message)
            self.addBrokerId(self.getFieldValue(fix.ClOrdID(), message), self.getFieldValue(fix.OrderID(), message))

        print(messageString)
        return
    
    def newOrder(self, symbol, orderType, side):
        message = fix.Message()
        header = message.getHeader()

        header.setField(fix.MsgType(fix.MsgType_NewOrderSingle))
        clOrdID = self.genOrderID()
        message.setField(fix.ClOrdID(clOrdID))
        message.setField(fix.HandlInst(fix.HandlInst_MANUAL_ORDER_BEST_EXECUTION))
        message.setField(fix.Symbol(symbol))

        message.setField(fix.Side(side))
        if side == fix.Side_SELL_SHORT:
            message.setField(fix.LocateReqd(True))
        transactionTime = fix.TransactTime()
        transactionTime.setString(datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3])
        message.setField(transactionTime)
        # RANDOM
        message.setField(fix.OrderQty(random.randint(1,100)))
        if orderType == 'l':
            message.setField(fix.OrdType(fix.OrdType_LIMIT))
            # RANDOM
            message.setField(fix.Price(random.randint(10,400)))
        if orderType == 'm':
            message.setField(fix.OrdType(fix.OrdType_MARKET))
        message.setField(fix.TimeInForce('0'))

        message.setField(fix.Text("New Single Order"))
        
        fix.Session.sendToTarget(message, self.sessionID)
        return [clOrdID, symbol, side]
    
    def cancelOrder(self, orderCancelID, brokerOrderId, symbol, side):
        message = fix.Message()
        header = message.getHeader()
        
        header.setField(fix.MsgType(fix.MsgType_OrderCancelRequest))

        message.setField(fix.OrderID(brokerOrderId))
        message.setField(fix.ClOrdID(self.genOrderID()))
        message.setField(fix.OrigClOrdID(orderCancelID))
        message.setField(fix.Symbol(symbol))
        message.setField(fix.Side(side))
        transactionTime = fix.TransactTime()
        transactionTime.setString(datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3])
        message.setField(transactionTime)

        fix.Session.sendToTarget(message, self.sessionID)
        return
    
    def calculateTotalTradingVolume(self, message):
        if self.getFieldValue(fix.OrdStatus(), message) == "2" or self.getFieldValue(fix.OrdStatus(), message) == "1":
            self.totalUSD[self.getFieldValue(fix.Symbol(), message)] += float(self.getFieldValue(fix.LastPx(), message)) * float(self.getFieldValue(fix.LastShares(), message))
            self.totalUSD['Total USD'] += float(self.getFieldValue(fix.LastPx(), message)) * float(self.getFieldValue(fix.LastShares(), message))
    
    def calculatePNL(self, message):
        if self.getFieldValue(fix.OrdStatus(), message) == "2" or self.getFieldValue(fix.OrdStatus(), message) == "1":
            if self.getFieldValue(fix.Side(), message) == "1":
                self.pnl -= float(self.getFieldValue(fix.LastPx(), message)) * float(self.getFieldValue(fix.LastShares(), message))
            if self.getFieldValue(fix.Side(), message) == "2":
                self.pnl += float(self.getFieldValue(fix.LastPx(), message)) * float(self.getFieldValue(fix.LastShares(), message))
            if self.getFieldValue(fix.Side(), message) == "5":
                self.pnl += float(self.getFieldValue(fix.LastPx(), message)) * float(self.getFieldValue(fix.LastShares(), message))

    def addTotalShares(self, message):
        if self.getFieldValue(fix.OrdStatus(), message) == "2" or self.getFieldValue(fix.OrdStatus(), message) == "1":
            self.totalSharesTraded[self.getFieldValue(fix.Symbol(), message)] += float(self.getFieldValue(fix.LastShares(), message))
    
    def getVWAP(self):
        # VWAP = totalDollarsTraded / TotalSharesTraded
        VWAP = {'AAPL': 0, 'MSFT': 0, 'BAC': 0}

        if self.totalSharesTraded['AAPL'] != 0:
            VWAP['AAPL']= self.totalUSD['AAPL'] / self.totalSharesTraded['AAPL']
        if self.totalSharesTraded['MSFT'] != 0:
            VWAP['MSFT'] = self.totalUSD['MSFT'] / self.totalSharesTraded['MSFT']
        if self.totalSharesTraded['BAC'] != 0:
            VWAP['BAC'] = self.totalUSD['BAC'] / self.totalSharesTraded['BAC']
        return VWAP
    
    def randomOrder(self):
        symbol = random.choice(['MSFT', 'AAPL', 'BAC'])
        orderType = random.choice(['l', 'm'])
        side = random.choice([fix.Side_BUY, fix.Side_SELL, fix.Side_SELL_SHORT])
        return self.newOrder(symbol, orderType, side)
        

    def run(self):
        while True:
            command = str(input("Please enter command: \n"))
            # List of commands
            if command == 'help':
                print("\nCOMMANDS\nn : New Order\nc : Order Cancel Request\nt : Get total trading volume (USD)\np : Get total PNL generated from this trading\nv : Get VWAP of the fills for each instrument\nauto : Automatically places and cancels orders within a 5 minute period")
                continue
            # New Order (35=D)
            if command == 'n':
                orderType = str(input("Please enter 'l' for Limit Order or 'm' for Market Order: \n"))
                if orderType == 'l' or orderType == 'm':
                    symbol = str(input("Please enter ticker symbol: \n"))
                    side = str(input("Please enter 'b' for Buy, 's' for Sell or 'sh' for Short: \n"))
                    if side == 'b':
                        side = fix.Side_BUY
                    if side == 's':
                        side = fix.Side_SELL
                    if side == 'sh':
                        side = fix.Side_SELL_SHORT
                    self.newOrder(symbol, orderType, side)
                    continue
                else:
                    print("ERROR: Invalid order input")
                continue
            # Order Cancel Request (35=F)
            if command == 'c':
                orderCancelID = str(input("Please enter the client order ID of the order to cancel\n"))
                symbol = str(input("Please enter ticker symbol: \n"))
                side = str(input("Please enter 'b' for Buy, 's' for Sell or 'sh' for Short: \n"))
                if side == 'b':
                    side = fix.Side_BUY
                if side == 's':
                    side = fix.Side_SELL
                if side == 'sh':
                    side = fix.Side_SELL_SHORT

                self.cancelOrder(orderCancelID, self.brokerIDs[orderCancelID], symbol, side)
                continue
            # Get total trading volume (USD)
            if command == 't':
                print("Total Trading Volume (USD): " + str(self.totalUSD))
                continue
            # Get PNL generated from this trading
            if command == 'p':
                print("Total PNL generated from this trading (USD): " + str(self.pnl))
                continue
            # Get VWAP of the fills for each instrument
            if command == 'v':
                print("VWAP of the fills for each instrument: \n" + str(self.getVWAP()))
                continue
            if command == 'test':
                orderDetails = self.newOrder('MSFT', 'l', fix.Side_BUY)
                time.sleep(0.5)
                self.cancelOrder(orderDetails[0], self.brokerIDs[orderDetails[0]], orderDetails[1], orderDetails[2])
                continue
            # Send 1000 random orders within 5 minutes for MSFT, AAPL, and BAC. One side can be BUY, SELL, or SHORT. It could be a limit or a market order.
            # Also randomly cancel them within 5 minutes of starting
            if command == 'auto':
                starttime = time.monotonic()
                count = 0
                commandsPerSecond = {}
                for i in range(300):
                    commandsPerSecond[i] = 0
                for i in range(1000):
                    commandsPerSecond[random.randint(0, 299)] += 1
                
                cancelCommand = {}
                for i in range (300):
                    cancelCommand[i] = []
                
                while count != 300:
                    print(count)
                    for i in range(commandsPerSecond[count]):
                        orderValues = self.randomOrder()
                        cancelCommand[random.randint(count,299)] += [orderValues]
                    for order in cancelCommand[count]:
                        if order[0] in self.brokerIDs:
                            self.cancelOrder(order[0], self.brokerIDs[order[0]], order[1],order[2])
                    count += 1
                    time.sleep(1 - ((time.monotonic() - starttime) % 1))
                print(self.TESTLIST)
                continue
            # Quit
            if command == 'quit':
                sys.exit(0)
            else:
                print("ERROR: Invalid input (Enter command 'help' for a list of commands)\n")
                continue

if __name__=='__main__':
    try:
        file = "client.cfg"
        application = Client()
        settings = fix.SessionSettings(file)
        storeFactory = fix.FileStoreFactory(settings)
        logFactory = fix.FileLogFactory(settings)
        initiator = fix.SocketInitiator(application, storeFactory, settings, logFactory)
        
        initiator.start()
        application.run()
        initiator.stop()
    except fix.ConfigError as e:
        print(e)
