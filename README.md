# Oracle Storage Adapter for Parse Server

This document describes how to build and run [Parse Server](https://parseplatform.org/) with the new Oracle storage adapter based on the [Oracle NodeJS libraries](https://node-oracledb.readthedocs.io/en/latest). It will demonstrate running against the [Free23c Docker container](https://www.oracle.com/database/free) and the [JSON Autonomoumous database](https://www.oracle.com/autonomous-database/autonomous-json-database/) in the cloud. The team is currently working with Parse to upstream the adapter to their [Parse repository](https://github.com/parse-community/parse-server) but, in the meantime, this is an early adopter version.

## Prerequisites

The Oracle SQL client is a software application that allows users to connect to an Oracle database and execute queries and manage the database.

[SQL Client](https://www.oracle.com/database/sqldeveloper/technologies/sqlcl/download/)

The Oracle Instant Client is a set of software libraries that allow users to connect to an Oracle database without a full Oracle database installation.

[Instant Client Libraries](https://www.oracle.com/cis/database/technologies/instant-client/downloads.html)

## Installation
1. Clone [Parse Server Repository](https://github.com/parse-community/parse-server). Use the alpha branch
2. Clone this Oracle Samples repo into src/Adapters/Storage/Oracle
   ```
   cd src/Adapters/Storage
   git clone git@github.com:oracle-samples/oracleadapter-parse.git Oracle
   cd Oracle
   rm -rf .git    // IMPORTANT or build will fail
   cd ../../../.. // Go back to Project Root
   ```


## Getting Started
### Building Parse with Oracle Storage Adpater
1. Add the Oracle database dependency to package.json

    ```"oracledb": "^5.5.0",```
2. Edit src/Controllers/index.js and add import

   ```import OracleStorageAdapter from '../Adapters/Storage/Oracle/OracleStorageAdapter';```
3. Scroll to the bottom of the file and add
    ```
    case 'oracledb:':
      return new OracleStorageAdapter({
        uri: databaseURI,
        collectionPrefix,
        databaseOptions,
    });
    ```
    to the case statement
4. Run ```npm install``` to get Oracle database dependencies
5. Run ```npm ci``` to build the server

## How To Run
### Configuring Free23c Oracle database image
1. Get and Start the image

    ```docker run --name free23c -d -p 1521:1521 -e ORACLE_PWD=Welcome12345 container-registry.oracle.com/database/free:latest```

   It takes about a minute for the image to reach a healthy state on my MacBook

2. Connect to the image as sysdba

    ```sql sys/Welcome12345@localhost:1521/free as sysdba```

   and run the following commands to enable JSON support

    ```
    alter session set container=FREEPDB1;
    grant db_developer_role to pdbadmin;
    grant soda_app to pdbadmin;
    GRANT UNLIMITED TABLESPACE TO pdbadmin;
    quit;
    ```

### Run Parse Server
1. Create a config.json.  This is a minimal set of [configuration parameters](https://parseplatform.org/parse-server/api/master/ParseServerOptions.html) for booting the server
    ```
    {
    "appId": "APPLICATION_ID",
    "masterKey": "MASTER_KEY",
    "databaseURI": "oracledb://pdbadmin:Welcome12345@localhost:1521/freepdb1",
    "port": 1338,
    "logLevel": "info",
    "verbose": false,
    "mountGraphQL": true,
    "mountPlayground": true,
    "graphQLPath": "/graphql"
    }
    ```

2. Boot the Server using the Oracle Instant Client location

    ```ORACLE_CLIENT_LOCATION=/Users/myuser/instantclient_19_8  npm start -- ./config.json```


### Test the Local Stack
1. Run a curl command

    ```curl -X POST -H "X-Parse-Application-Id: APPLICATION_ID" -H "Content-Type: application/json" -d '{"score":12,"playerName":"scooby","cheatmode":false}' http://localhost:1338/parse/classes/GameScore```

   Upon success

    ```{"objectId":"CdmLJT6Duc","createdAt":"2023-10-16T19:33:27.382Z"}```

2. Connect to the database and verify

    ```sql pdbadmin/Welcome12345@localhost:1521/FREEPDB1```

3. Run SODA commands

    ```
    SQL> soda list
    List of collections:

	GameScore
	_Hooks
	_Idempotency
	_Role
	_SCHEMA
	_User

    SQL> soda get GameScore
	KEY						                Created On

	3A8CB47A41A74F59BFDD143A3F365F4A		2023-10-16T19:33:27.404374000Z

    1 row selected. 

    SQL> soda get GameScore -k 3A8CB47A41A74F59BFDD143A3F365F4A

    Key:    	 3A8CB47A41A74F59BFDD143A3F365F4A
    Content:	 {"score":12,"playerName":"scooby","cheatmode":false,"updatedAt":"2023-10-16T19:33:27.382Z","createdAt":"2023-10-16T19:33:27.382Z","_id":"CdmLJT6Duc"}

    1 row selected. 


     soda help – list all soda commands

    ```

 
### Running against Autonomous Database in the cloud
1. Update databaseURI in config.json to point at the cloud database instance

    ``` "databaseURI": "oracledb://username:password@tnsname",```

2. Download the cloud database wallet and use it when starting the server

    ```ORACLE_CLIENT_LOCATION=/Users/myuser/instantclient_19_8 ORACLE_WALLET_LOCATION=/Users/myuser/wallet-oradb  npm start -- ./config.json```

## Contributing

This project welcomes contributions from the community. Before submitting a pull request, please [review our contribution guide](./CONTRIBUTING.md)

## Security

Please consult the [security guide](./SECURITY.md) for our responsible security vulnerability disclosure process

## License

Copyright (c) 2023 Oracle and/or its affiliates.

Released under the Universal Permissive License v1.0 as shown at
<https://oss.oracle.com/licenses/upl/>.
