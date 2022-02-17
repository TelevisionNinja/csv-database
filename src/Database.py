import csv
import os.path


class csvDialect(csv.Dialect):
    quoting = csv.QUOTE_ALL
    delimiter = ','
    quotechar = '"'
    lineterminator = '\n'


class DB:
    def __init__(self):
        self.recordSize = 0
        self.numSortedRecords = 0
        self.numOverflowRecords = 0
        self.delimiter = csvDialect.delimiter
        self.databaseName = ""

        # files
        self.data = None
        self.overflow = None
        # self.config = None


    # create the data, overflow, and config files
    def create(self, filename):
        if not os.path.isfile(filename + ".csv"):
            print(str(filename) + " not found")
            return False

        # check if the program has already made the files for the database
        if not os.path.isfile(filename + ".data"):
            self.recordSize = 200 # default record size is 200 chars
            self.data = open(filename + ".data", 'w')
            self.overflow = open(filename + ".overflow", 'w')

            # create the data file
            with open(filename + ".csv", 'r') as f:
                for line in f:
                    csvReader = csv.reader([line.replace('_', ' ')], csvDialect())

                    for row in csvReader:
                        self.writeRecord(row[0], row[1], row[2], row[3])

            self.databaseName = filename
            self.close()

            return True

        return False


    # open the database files
    def open(self, filename):        
        self.databaseName = filename

        # check if the program has already made the files for the database
        if os.path.isfile(filename + ".data"):
            self.data = open(filename + ".data", 'r+')
            self.overflow = open(filename + ".overflow", 'r+')

            # open and read config and set these variables
            iteration = 1

            with open(filename + ".config", 'r') as f:
                for line in f:
                    if iteration == 1: # first line is the record size
                        self.recordSize = int(line)
                    elif iteration == 2: # second line is the total number of records
                        self.numSortedRecords = int(line)
                    elif iteration == 3: # third line is the total number of overflow records
                        self.numOverflowRecords = int(line)

                    iteration += 1
            
            return True
        
        return False


    # close the database files and clear the vars
    def close(self):
        if not self.isOpen():
            return

        # save config variables
        with open(self.databaseName + ".config", 'w') as f:
            f.write(str(self.recordSize) + '\n')
            f.write(str(self.numSortedRecords) + '\n')
            f.write(str(self.numOverflowRecords) + '\n')

        if self.data is not None:
            self.data.close()

        if self.overflow is not None:
            self.overflow.close()

        # if self.config is not None:
        #     self.config.close()

        # clear vars

        self.recordSize = 0
        self.numSortedRecords = 0
        self.numOverflowRecords = 0
        self.delimiter = csvDialect.delimiter
        self.databaseName = ""

        self.data = None
        self.overflow = None
        # self.config = None


    # check if the DB is open
    def isOpen(self):
        if self.data is None:
            return False

        return not self.data.closed


    # read record depending on the file given
    def readRecord(self, filename, recordIndex):
        id = state = city = name = None
        file = None
        totalRecords = 0
        isOverflow = False

        if filename.endswith(".data"):
            file = self.data
            totalRecords = self.numSortedRecords
        elif filename.endswith(".overflow"):
            file = self.overflow
            totalRecords = self.numOverflowRecords
            isOverflow = True
        else:
            file = self.data
            totalRecords = self.numSortedRecords

        if recordIndex >= 0 and recordIndex < totalRecords:
            file.seek(recordIndex * self.recordSize)
            line = file.readline().rstrip()

            csvReader = csv.reader([line], csvDialect())

            for row in csvReader:
                id = row[0]
                state = row[1]
                city = row[2]
                name = row[3]

        return dict({"ID":id,"state":state,"city":city,"name":name, "overflow": isOverflow})


    # private function to construct a fixed length record
    def _constructDataLine(self, id, state, city, name):
        dataStr = str(id) + self.delimiter + '"' + str(state) + '"' + self.delimiter + '"' + str(city) + '"' + self.delimiter + '"' + str(name) + '"'
        dataStr = dataStr.ljust(self.recordSize - 1, ' ') + '\n'
        return dataStr


    # private writing function
    def _write(self, filename, recordNum, id, state, city, name):
        dataStr = self._constructDataLine(id, state, city, name)

        # record size constraint
        if len(dataStr) > self.recordSize:
            print("The record is too long:"
                + "\nID: " + str(id)
                + "\nstate: " + str(state)
                + "\ncity: " + str(city)
                + "\nname: " + str(name))

            return

        file = None
        totalRecords = 0

        if filename.endswith(".data"):
            file = self.data
            totalRecords = self.numSortedRecords
        elif filename.endswith(".overflow"):
            file = self.overflow
            totalRecords = self.numOverflowRecords
        else:
            file = self.data
            totalRecords = self.numSortedRecords

        # check and correct for incorrect indices
        if recordNum < 0:
            recordNum = 0
        elif recordNum > totalRecords:
            recordNum = totalRecords

        # write to file
        file.seek(recordNum * self.recordSize)
        file.write(dataStr)


    # write records to the data file to create the database
    def writeRecord(self, id, state, city, name):
        self._write(".data", self.numSortedRecords, id, state, city, name)
        self.numSortedRecords += 1

        return self.isOpen()


    # overwrite existing records
    def overwriteRecord(self, filename, recordNum, id, state, city, name):
        if not self.isOpen():
            return False

        totalRecords = 0

        if filename.endswith(".data"):
            totalRecords = self.numSortedRecords
        elif filename.endswith(".overflow"):
            totalRecords = self.numOverflowRecords
        else:
            totalRecords = self.numSortedRecords

        # check for incorrect indices
        if recordNum < 0 or recordNum >= totalRecords:
            return False

        self._write(filename, recordNum, id, state, city, name)
        return True


    # write new records to the end of the overflow file
    def appendRecord(self, id, state, city, name):
        if not self.isOpen():
            return False

        self._write(".overflow", self.numOverflowRecords, id, state, city, name)
        self.numOverflowRecords += 1

        return True


    # binary search by record id
    def binarySearch(self, id):
        low = 0
        high = self.numSortedRecords - 1

        while high >= low:
            index = (low + high) // 2
            currentRecord = self.readRecord(".data", index)
            mid_id = currentRecord["ID"]

            if int(mid_id) == int(id):
                return currentRecord, index 
            elif int(mid_id) > int(id):
                high = index - 1
            elif int(mid_id) < int(id):
                low = index + 1

        return None, -1


    def findRecord(self, id): # binary search and then linear search the overflow file
        if not self.isOpen():
            return None, -1

        # search sorted file first
        record, index = self.binarySearch(id)
        
        if record is not None:
            return record, index

        # linear search the overflow file
        for i in range(self.numOverflowRecords):
            record = self.readRecord(".overflow", i)

            if str(id) == str(record["ID"]):
                return record, i

        return None, -1


    # write new records to the end of the overflow file
    def addRecord(self, id, state, city, name):
        return self.appendRecord(id, state, city, name)


    # find and update a record
    def updateRecord(self, id, state, city, name):
        if not self.isOpen():
            return False

        record, index = self.findRecord(id)

        if record is not None:
            if record["overflow"]:
                self.overwriteRecord(".overflow", index, id, state, city, name)
            else:
                self.overwriteRecord(".data", index, id, state, city, name)

            return True

        return False


    # deletes a record by making the record blank
    def deleteRecord(self, id):
        if not self.isOpen():
            return False

        record, index = self.findRecord(id)

        if record is not None:
            if record["overflow"]:
                self.overwriteRecord(".overflow", index, id, "", "", "")
            else:
                self.overwriteRecord(".data", index, id, "", "", "")

            return True

        return False


    # create and print a report of the first 10 records
    def createReport(self):
        for i in range(10):
            record = self.readRecord(".data", i)
            print("ID: " + str(record["ID"]) + "\tstate: " + str(record["state"]) + "\tcity: " + str(record["city"]) + "\tname: " + str(record["name"]))


def main():
    databaseObj = DB()

    # command handler
    while True:
        print("Type the number of the command to execute the command:")
        print("\t1) create new database")
        print("\t2) open database")
        print("\t3) close database")
        print("\t4) display record")
        print("\t5) update record")
        print("\t6) create report")
        print("\t7) add a record")
        print("\t8) delete a record")
        print("\t9) quit")

        selection = input()

        # command 1
        if selection == "1":
            dbFile = input("What is the name of the database csv file? Do not include the file extension.\n")

            if databaseObj.create(dbFile):
                print("Database created")

        # command 2
        elif selection == "2":
            if databaseObj.isOpen():
                print("Close the current database first")
            else:
                dbFile = input("What is the name of the database? Do not include a file extension.\n")

                if databaseObj.open(dbFile):
                    print("Database opened")
                else:
                    print("Could not open the database")

        # command 3
        elif selection == "3":
            databaseObj.close()
            print("Database closed")

        # command 4
        elif selection == "4":
            if not databaseObj.isOpen():
                print("Open a database first")
            else:
                id = input("Enter the id of the record you want to get\n")
                record, index = databaseObj.findRecord(id)

                if record is not None:
                    file = "data"

                    if record["overflow"]:
                        file = "overflow"

                    print("Record at index " + str(index)
                        + " in the " + file + " file."
                        + "\nID: " + str(record["ID"])
                        + "\nstate: " + str(record["state"])
                        + "\ncity: " + str(record["city"])
                        + "\nname: " + str(record["name"]))
                else:
                    print("The record with that id was not found")

        # command 5
        elif selection == "5":
            if not databaseObj.isOpen():
                print("Open a database first")
            else:
                id = input("Enter the id of the record you want to update\n")
                state = input("Enter the state of the record you want to update\n")
                city = input("Enter the city of the record you want to update\n")
                name = input("Enter the name of the record you want to update\n")

                if databaseObj.updateRecord(id, state, city, name):
                    print("Updated record " + id)
                else:
                    print("Could not find record " + id)

        # command 6
        elif selection == "6":
            if not databaseObj.isOpen():
                print("Open a database first")
            else:
                databaseObj.createReport()

        # command 7
        elif selection == "7":
            if not databaseObj.isOpen():
                print("Open a database first")
            else:
                id = input("Enter the id of the record you want to add\n")
                state = input("Enter the state of the record you want to add\n")
                city = input("Enter the city of the record you want to add\n")
                name = input("Enter the name of the record you want to add\n")

                databaseObj.addRecord(id, state, city, name)

                print("Added a new record"
                    + "\nID: " + str(id)
                    + "\nstate: " + str(state)
                    + "\ncity: " + str(city)
                    + "\nname: " + str(name))

        # command 8
        elif selection == "8":
            if not databaseObj.isOpen():
                print("Open a database first")
            else:
                id = input("Enter the id of the record you want to delete\n")
                if databaseObj.deleteRecord(id):
                    print("Deleted record " + id)
                else:
                    print("Could not find record " + id)

        # command 9
        elif selection == "9":
            databaseObj.close()
            print("Goodbye! Any open database was closed just in case you forgot")
            return

        # unknown command
        else:
            print("That is not a command")

        print()


if __name__ == "__main__":
    main()
