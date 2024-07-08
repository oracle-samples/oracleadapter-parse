# Oracle Storage Adapter for Parse Server

This document describes how to build and run [Parse Server](https://parseplatform.org/) with the new Oracle storage adapter based on the [Oracle NodeJS libraries](https://node-oracledb.readthedocs.io/en/latest). It will demonstrate running against the [Free Oracle Database 23ai Docker container](https://www.oracle.com/database/free) and the [JSON Autonomoumous database](https://www.oracle.com/autonomous-database/autonomous-json-database/) in the cloud. 

## Prerequisites

The Oracle SQL client is a software application that allows users to connect to an Oracle database and execute queries and manage the database.

[SQL Client](https://www.oracle.com/database/sqldeveloper/technologies/sqlcl/download/)

The Oracle Instant Client is a set of software libraries that allow users to connect to an Oracle database without a full Oracle database installation.

[Instant Client Libraries](https://www.oracle.com/cis/database/technologies/instant-client/downloads.html)

## Installation
Clone [Parse Server Repository](https://github.com/parse-community/parse-server). Supported version 7.1.0 and above.  

There were changes in this release that supported:

1. [Test Exclusion List](https://github.com/parse-community/parse-server/pull/8774)
2. [Dynamic Database Adapter configuration](https://github.com/parse-community/parse-server/pull/8883)

```
git clone --depth 1 --branch 7.1.0 https://github.com/parse-community/parse-server.git
cd parse-server
```
2. Clone this Oracle Samples repo into src/Adapters/Storage/Oracle
 ```
cd src/Adapters/Storage
git clone git@github.com:oracle-samples/oracleadapter-parse.git Oracle
cd Oracle
rm -rf .git    # IMPORTANT or build will fail
cd ../../../.. # Go back to Project Root
```


## Getting Started
### Building Parse with Oracle Storage Adpater
1. Add the Oracle database dependency

    ```npm install oracledb@6.5.0```

    [Quick Start node-oracledb Installation](https://node-oracledb.readthedocs.io/en/latest/user_guide/installation.html#quick-start-node-oracledb-installation)
2. Add the Parse File Adapter dependency

    ```npm install --save @parse/fs-files-adapter```

    This defaults to local storage. 

    [Parse Server File Storage Adapter Repository](https://github.com/parse-community/parse-server-fs-adapter)

5. Run ```npm ci``` to build the server

## How To Run
### Configuring Free23c Oracle database image
1. Get and Start the image

    ```docker run --name free23ai -d -p 1521:1521 -e ORACLE_PWD=Welcome12345 container-registry.oracle.com/database/free:latest```

   It takes about a minute for the image to reach a healthy state on my MacBook

2. Connect to the image as sysdba

    ```sql sys/Welcome12345@localhost:1521/freepdb1 as sysdba```

   and run the following commands to enable JSON support

    ```
    alter session set container=FREEPDB1;
    grant db_developer_role to pdbadmin;
    grant soda_app to pdbadmin;
    GRANT UNLIMITED TABLESPACE TO pdbadmin;
    quit;
    ```

    or run the commands as a script. Create a file called `soda` that contains the above commands

    ```
    sql sys/Welcome12345@localhost:1521/freepdb1 as sysdba @./soda
    ```

### Run Parse Server
1. Create a config.json.  This is a minimal set of [configuration parameters](https://parseplatform.org/parse-server/api/master/ParseServerOptions.html) for booting the server. The databaseURI is configured to attach to the local 23c Oracle Database instance.
```
{
  "appId": "APPLICATION_ID",
  "masterKey": "MASTER_KEY",
  "allowClientClassCreation": true,
  "port": 1338,
  "logLevel": "info",
  "verbose": false,
  "mountGraphQL": true,
  "mountPlayground": true,
  "graphQLPath": "/graphql",
  "filesAdapter": {
    "module": "@parse/fs-files-adapter"
  },
  "databaseAdapter": {
    "module": "./Storage/Oracle/OracleStorageAdapter",
    "options": {
      "databaseURI": "oracledb://pdbadmin:Welcome12345@localhost:1521/freepdb1",
      "collectionPrefix": ""
    }
  }
}  
```
2. If using an Oracle Instant Client prior to 23ai. I am running on MacOS Intel and the most recent Instant Client for that platform is 19_16 so export the variable below.
```
export ORACLEDB_VERSION=19
```

3. Boot the Server using the Oracle Instant Client location

```
ORACLE_CLIENT_LOCATION=/Users/myuser/instantclient_19_16  npm start -- ./config.json
```


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
1. Update databaseAdapter.options.databaseURI in config.json to point at the cloud database instance

    ``` "databaseURI": "oracledb://username:password@tnsname",```

2. Download the cloud database wallet and use it when starting the server

    ```ORACLE_CLIENT_LOCATION=/Users/myuser/instantclient_19_16 ORACLE_WALLET_LOCATION=/Users/myuser/wallet-oradb  npm start -- ./config.json```


## Cloud Code
[Use Cloud Code to call Custom Oracle PL/SQL](./cloud/README.md)


## Contributing

This project welcomes contributions from the community. Before submitting a pull request, please [review our contribution guide](./CONTRIBUTING.md)

## Security

Please consult the [security guide](./SECURITY.md) for our responsible security vulnerability disclosure process

## License

Copyright (c) 2024 Oracle and/or its affiliates.

Released under the Universal Permissive License v1.0 as shown at
<https://oss.oracle.com/licenses/upl/>.
