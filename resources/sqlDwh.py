import pymongo
from sqlalchemy_utils import database_exists
import logging
from sqlalchemy import create_engine,MetaData,inspect,String,Column,Table,engine,TEXT,DateTime, text,VARCHAR, BIGINT,NUMERIC,TIMESTAMP
import os


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logging.getLogger('pymongo').setLevel(logging.WARNING)




class sqlDwh:

#class variables
    client = pymongo.MongoClient(os.getenv('mongoDbConnStr'), socketTimeoutMS=None)
    metadataDB = client[os.getenv('metadataDbName')]
    configMongoDB = client[os.getenv('configDbName')]
    

    def __init__(self,extractionConfig:String,sqlSSL:bool):
        
        self.datasourceId = extractionConfig['_id']
        self.accountID = extractionConfig['accountID']
        self.sqlDb = 'acc-' + self.accountID #--> the name of the database in the SQL server --> acc-03445d66
        
        #the list of the entities that will be scooped from the extraction config.
        self.scoopedEntities = extractionConfig['entities']

        self.metadataCollection = sqlDwh.metadataDB[self.datasourceId] # --> the metadata collection in the MongoDB- the collection name is the datasource ID
        self.metadata = MetaData() #--> SQLalchemy metadata instance
        
        
        # Checking if SSL certificate is requeired, if TRUE add the SSL certificate path to the connection string, else keep the original SQL connection string.
        if sqlSSL:
            logging.debug('SSL certificate is set to True')
            self.sqlConnectionStr = os.getenv('sqlConnStr') + self.sqlDb + '?ssl_ca=ssl-certificate/DigiCertGlobalRootCA.crt.pem'
            
        
        else:
            logging.debug('SSL certificate is set to False')
            self.sqlConnectionStr = os.getenv('sqlConnStr') + self.sqlDb


        

        logging.debug(f'check if the the DB acc-{extractionConfig["accountID"]} exists on the Postgres DB server')
        engine = create_engine(self.sqlConnectionStr)
        with engine.connect() as connection:
            # Execute SQL query to create the database if it doesn't exist
            if  not self.ifDbExist():
                logging.debug(f'DB {self.sqlDb} dont exist, running CREATE DATABASE IF NOT EXISTS {self.sqlDb}')
                connection.execute(f"CREATE DATABASE IF NOT EXISTS {self.sqlDb}")
                
            else:
                logging.debug(f'{self.sqlDb} DB already exists')

        self.engine = create_engine(self.sqlConnectionStr,pool_pre_ping=True)

        logging.debug('__init__ finished successfully')



    def pingDwh(self):
        '''
        Pings the database to check if it is reachable.
        Returns:
            str: "OK" if the database is reachable, or an error message otherwise.
        '''
        try:
            # Use the engine to establish a connection
            with self.engine.connect() as connection:
                if connection.closed:
                    raise Exception("Connection is closed unexpectedly.")
                logging.info(f"Successfully connected to the database: {self.sqlDb}")
                return "OK"
        except Exception as e:
            logging.error(f"Failed to connect to the database {self.sqlDb}: {e}")
            return f"Error: {e}"


    
    
    def deleteTables(self):
        
        '''
        description: delete all the tables in the SQL db.
        
        params: None
        
        return: None
        
        '''

        # Reflect the schema from the database
        self.metadata.reflect(bind=self.engine)

        # Drop all views
        with self.engine.connect() as conn:
            result = conn.execute(text(
                "SELECT table_name FROM information_schema.views WHERE table_schema = 'public'"
            ))
            views = result.fetchall()
            for view in views:
                view_name = view[0]
                conn.execute(text(f'DROP VIEW IF EXISTS {view_name} CASCADE'))
            logging.debug(f'Dropped all views.')




        
        tableBefore = list(self.metadata.tables.keys())          #---> set of tables on the dwh before
        countTablesBefore = len(tableBefore)                    #---> number of tables on the dwh before
        
        logging.debug(f'Deleting all {countTablesBefore} tables in the dwh')
        self.metadata.drop_all(bind=self.engine)
        
        c = {'results' : f'All {countTablesBefore} tables dropped.',
            'dwhTables': tableBefore
            }
        return c
    
    
    def getScoopedEntitiesMetadata(self):
        '''
        description: get the metadata of the scooped entities from the MongoDB.
        
        params: None
        
        return: a list of the metadata JSON documents of the scooped entities.
        
        '''
        #return a list of the entities and sub-entities in the extraction config
        
        x = self.scoopedEntities
        
        print(x)
        e = self.flatEntities(x)
        return list(self.metadataCollection.find({'_id': {'$in': e}}))
        
    
    
    def flatEntities(self)->list:
        '''
        description: flatten the entity JSON document to a list of the columns names.
        params:
            entity: the entity JSON document.
        
        return: a list of the columns names.

        for example:
                    entity={
                                "EntityID": "AINVOICES",


                                "expand": [
                                    "AINVOICEITEMS",
                                    "IVORD"
                                ]
                            }
        
                            
        return:
                    ['AINVOICES', 'AINVOICEITEMS_subform', 'IVORD_subform']
        '''
        e = self.scoopedEntities
        a=[]
        

        return [item for entity in e for item in [entity['EntityID']] + entity['expand']]
        

    
    def deployExtractionconfigTables(self):
        '''
        description: create the tables in the SQL db based on the extractionConfig files.
        
        '''
        
        # initialize the counters for the success and failed tables creation
        success = []
        failed = []
        exists=[]
       
        
        # flat the entities to a list of the entities and sub-entities names
        tableList=self.flatEntities()

        # iterate over the scooped entities metadata
        for i in tableList:
            
            logging.debug(f'quering MongoDB the get the medata of entity {i}')
            metadata=self.getEntitymetadtata(i)   
            
            logging.debug(f'Creating table {i}')
            tableObject = self.setTableMetadata(metadata)
  
            #change the table names and columns names to lower case
            allLowerTable = self.lowercaseTableObject(tableObject)


            createTable = self.createTable(allLowerTable)
            

            if createTable == "Table exists":
                exists.append(i)
            elif createTable == 'Table created':
                success.append(i)
            else:
                failed.append(i)
                

        return {'success':success, 'exists':exists, 'failed':failed}
        

    def lowercaseTableObject(self,table:Table):
        metadata = MetaData()
        columns = [Column(col.name.lower(), col.type, primary_key=col.primary_key) for col in table.columns]
        lowercaseTable = Table(table.name.lower(), metadata, *columns)
        return lowercaseTable
            
    def ifDbExist(self):
        '''
        description: check if the database exists in the SQL server.
        
        params: None
        
        return: True if the database exists, False otherwise.
        '''
        return database_exists(self.sqlConnectionStr)
    




    def createDb(self):
        '''
        description: create a database in the SQLs server if it doesn't exist.
        
        params: None
        
        return: None
        
        '''
        # Create a connection to the SQL server
        engine = create_engine(self.sqlConnectionStr)
        with engine.connect() as connection:
            # Execute SQL query to create the database if it doesn't exist
            if  not self.ifDbExist():
                logging.debug(f'DB {self.sqlDb} dont exist, running CREATE DATABASE IF NOT EXISTS {self.sqlDb}')
                connection.execute(f"CREATE DATABASE IF NOT EXISTS {self.sqlDb}")
                
            else:
                return logging.debug(f'{self.sqlDb} DB already exists')


    
    def ifTableExists(self, tableName:str)->bool:
        '''
        description: check if the table exists in the SQL DB.
        
        params:
            tableName: the name of the table to check if it exists.
        
        return:
                True if the table exists, False otherwise.
        
        '''
        if self.ifDbExist():
            logging.debug(f'db exists in db {self.sqlDb}')
            logging.debug(f'checking if table {tableName} exists in db {self.sqlDb}')
            engine = create_engine(self.sqlConnectionStr)
            inspector = inspect(engine)
            return inspector.has_table(tableName)
        else:
            logging.debug(f'db does not exist: {self.sqlDb}')
            return False



    
    
    def getEntitymetadtata(self,entityID:str)->dict:
        '''
        description: get the metadata of the entity from the MongoDB.
        
        params:
            entityID: the entity ID.
        retun:
            the metadata JSON document of the entity.
        '''
        
        logging.debug(f'retriving metadata for entity {entityID}')
        return self.metadataCollection.find_one({'_id':entityID})
    
    
    
    def setTableMetadata(self,tableMetadata:dict)->Table:
        """
        Create SQLAlchemy table object based on the provided entity metadata.

        Args:
            entityMetadata (dict): A dictionary containing information about the entity and its fields.
                Example:
                {
                    "_id": "ENTITY_NAME",
                    "Fields": [
                        {
                            "fieldName": "FIELD1",
                            "SourceDataType": "<the data type in the source system>",
                            "targetDataType": "<mysql data type>",
                            "KeyFlag": True/False
                        },
                        
                    ]
                }

        Returns:
            dict: A dictionary containing the table name and a list of column objects.
                Example:
                {
                    'tableName': 'ENTITY_NAME',
                    'tableColumns': [
                        ('FIELD1', DataType, True/False),
                        # Additional column specifications...
                        ('insertDateUTC', DateTime, False),
                        ('lastModifiedDateUTC', DateTime, False)
                    ]
                }

        Notes:
            - The 'TableSpec' list includes tuples for each column, consisting of the column name, data type, and key flag.
            - 'insertDateUTC' and 'lastModifiedDateUTC' columns are added by default with DateTime data type and False key flag.
        """
    
        
        logging.debug('creating SQLAlchemy columns objects')
        tableName = tableMetadata['_id']
        fieldsList = tableMetadata['Fields']
        columnsObjectsList = []
                
        
        logging.debug(f'iterating over fieldsList- length: {len(fieldsList)}')
        for i in fieldsList:
            
            if i['KeyFlag']:
                # If the column is a primary key, enforce the data type as String(length=255)
                columnDataType = String(length=255)
            else:
                columnDataType = eval(i['targetDataType'])
                

            cursorColumn = Column(i['fieldName'], columnDataType, primary_key=i['KeyFlag'])
            columnsObjectsList.append(cursorColumn)
            
        
        # adding inserDate and lastModifiedDate columns
        extractionId = Column('extractionId', TEXT, primary_key=False)
        extractionTimestamp = Column('extractionTimestampUTC', DateTime, primary_key=False)


        columnsObjectsList.append(extractionId)
        columnsObjectsList.append(extractionTimestamp)


        logging.debug("creating a SQLAlchemy Table Object")
        tableObject = Table(tableName, self.metadata, *columnsObjectsList)
        return tableObject
    


 
    def createTable(self,table:Table):
        
        '''
        Description:
                    The method accept a SQLAlchemy Table object and create the table in the DWH according to the Table object, if the table already exists in the DWH, the method will not create the table.
                    
                    
        Parameters:
                    - table (SQLAlchemy Table Object): The Table object that will be created in the DWH.
                      
        Return:
                    
        '''
        
        tableName = table.name
        
        
        if not self.ifTableExists(tableName):
            logging.debug(f'Table {tableName} not found in the SQL server --> creating the table {tableName}')
            
       

            try: #--> table not exists, try to create the table
                
                
                table.create(self.engine)
                logging.debug(f'Table {tableName} created successfully with columns {table.columns.keys()}')
                return "Table created"



            except Exception as e:
                logging.error("An error occurred while creating table %s: %s", table.name, e)
                return e
                
            
        
        else: #--> table exists
            logging.debug(f'Table {tableName} found in the SQL server')
            return "Table exists"
        
                



    def getPk(self,tableName:String)->String:
        ''''
        description:
                    The method returns the primary key of the provided table.

        Parameters:
                    - tableName (string): the table name (string)
        return:
                    - the primary key of the provided table (string)    

        
        '''
        tableObject = Table(tableName, self.metadata, autoload_with=engine)
        return tableObject.columns.keys()

    
    def getTable(self,tableName:String)->Table:
        '''
        description:
                    The method returns the table object of the provided table name.

        Parameters:
                    - tableName (string): the table name (string)

        return:
                    - the table object of the provided table name (Table)
        
        '''
        return Table(tableName, self.metadata, autoload_with=engine)
        
        


    def getDtypeDict(self,tableName:String):
        tableMeatdata = self.getTableMetadata(tableName)
        fieldsMetadata = tableMeatdata['Fields']
        
        columnMetadataList = [
                                (d['fieldName']
                                ,eval(String(255)) if d['KeyFlag'] else eval(d['targetDataType'])
                                
                                )
                            
                            for d in fieldsMetadata
                            ]
        
        return columnMetadataList
    
    
    
    def getTableMetadata(self,tableName:String):

            '''
            --------------------------------------------
            Description:
                        The method connects to the client metadata collection in the MongoDB and return the metadta of the provided entity.
            
            Parameters:
                        - tableName- the table name (string)
                    
            Return:
                        - <JSON string> a json which details the table name (string), the fields and theire datatype and a primary key boolean expression (list of dicionaries.)
                        {
                            "tableName": "ACCESSTOKENENV",
                            "Fields": [
                                {
                                    "fieldName": "TOKENDATA",
                                    "KeyFlag": false,
                                    "targetDataType": "TEXT(255)"
                                },
                                {
                                    "fieldName": "LINE",
                                    "KeyFlag": true,
                                    "targetDataType": "BigInteger"
                                },
                                {
                                    "fieldName": "TYPE",
                                    "targetDataType": "BigInteger"
                                }
                            ]
                        }
            '''
            
            
            # Return the filter metadata collection for tableName.
            query = {"_id": tableName}
            return (self.metadataCollection.find_one(query))



    def writeStgData(self,df,table:String):
        tableDtypeDict = self.getDtypeDict(table)
        recordsWritten = df.to_sql(table,self.engine,if_exists='append', Index=False ,dtypedict = tableDtypeDict)

        return {
                'tableName': table,
                'recordsWritten' : recordsWritten
                }
    


    def writeJSONToMongodb(self,json:dict,dbName:str,collectionName:str):
        '''
        description: the function writes a json to a MongoDB collection

        Parameters: json (dict): the json to be written to the MongoDB collection
                    dbName (str): the name of the database
                    collectionName (str): the name of the collection

        return: None
        
        '''
        db = self.client[dbName]
        collection = db[collectionName]
        return collection.insert_one(json)




