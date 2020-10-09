import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

# DB
myDb = myclient["anuvaad"]

# TEST FOR DB EXISTENCE
dblist = myclient.list_database_names()
if "anuvaad" in dblist:
  print("The database exists.")
print('----------------------')


# COLLECTION
myCol = myDb["sent_pair"]


# TEST FOR COLLECTION EXISTENCE
colList = myDb.list_collection_names()
if "people" in colList:
  print("The collection exists.")
print('----------------------')


# INSERT RECORD
mydict = { "name": "John", "address": "Highway 37" }
x = myCol.insert_one(mydict)


# PRINT KEY
mydict = { "name": "Peter", "address": "Lowstreet 27" }
x = myCol.insert_one(mydict)
print(x.inserted_id)
print('----------------------')


# INSERT MULTI-RECORDS
mylist = [
  { "name": "Amy", "address": "Apple st 652"},
  { "name": "Hannah", "address": "Mountain 21"},
  { "name": "Michael", "address": "Valley 345"},
  { "name": "Sandy", "address": "Ocean blvd 2"},
  { "name": "Betty", "address": "Green Grass 1"},
  { "name": "Richard", "address": "Sky st 331"},
  { "name": "Susan", "address": "One way 98"},
  { "name": "Vicky", "address": "Yellow Garden 2"},
  { "name": "Ben", "address": "Park Lane 38"},
  { "name": "William", "address": "Central st 954"},
  { "name": "Chuck", "address": "Main Road 989"},
  { "name": "Viola", "address": "Sideway 1633"}
]
# x = myCol.insert_many(mylist)
# print(x.inserted_ids)
print('----------------------')

# PRINT PEEK
print(myCol.find_one())
print('----------------------')


# PRINT ALL RECORDS
for x in myCol.find():
  print(x)
print('----------------------')

# QUERY
myquery = { "address": "Park Lane 38" }
mydoc = myCol.find(myquery)
for x in mydoc:
  print(x)
print('----------------------')

# DELETE
myquery = { "address": "Mountain 21" }
myCol.delete_one(myquery)

# UPDATE
myquery = { "address": "Valley 345" }
newvalues = { "$set": { "address": "Canyon 123" } }
myCol.update_one(myquery, newvalues)
#print "customers" after the update:
for x in myCol.find():
  print(x)
print('----------------------')


