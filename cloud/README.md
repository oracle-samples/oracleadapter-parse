# Cloud Code Examples

This folder includes example [cloud code](https://docs.parseplatform.org/cloudcode/guide/) that demonstrates how to add [PL/SQL](https://www.oracle.com/database/technologies/appdev/plsql.html) and [Advanced Queueing](https://www.oracle.com/database/technologies/advanced-queuing.html) as APIS for Mobile and Web Developers. This demo code is configured to run against [Free23c](https://www.oracle.com/database/free/get-started/). There is 1 new file

1. qapi.js - a collection of cloud functions for supporting common queueing functions. These cloud fucntions are written using the [Node.js Oracle module](https://node-oracledb.readthedocs.io/en/latest/user_guide/introduction.html#getting-started-with-node-oracledb).

## Configure the Server to use Cloud Code
 Copy the cloud dir to the project root

   

    
    cp -R src/Adapters/Storage/Oracle/cloud .

Add cloud code location to config.json
```
  "cloud": "./cloud/qapi.js",
```

And boot the server

    ORACLE_CLIENT_LOCATION=/Users/myuser/instantclient_19_8  npm start -- ./config.json

## New APIs

1. Create Queue

    Input Parameter 

    qname - name of queue
    ```
    curl -X POST -H "X-Parse-Application-Id: APPLICATION_ID" -H "Content-Type: application/json" -d '{ "qname": "FRED"}' http://localhost:1338/parse/functions/createQ
    ```

2. Enqueue Message

    Input Parameter 

    qname - name of queue to enqueu message

    msg   - message to enque
    
    ```
    curl -X POST -H "X-Parse-Application-Id: APPLICATION_ID" -H "Content-Type: application/json" -d '{"qname": "FRED","msg": "Wilma says Hi"}' http://localhost:1338/parse/functions/enqueMsg 
    ```

3. Dequeue Message

    Input Parameter 

    qname - name of queue to dequeue message

    ```
    curl -X POST -H "X-Parse-Application-Id: APPLICATION_ID" -H "Content-Type: application/json" -d '{"qname": "FRED"}' http://localhost:1338/parse/functions/dequeMsg
    ```

**NOTE:** The API will block if there are no messages to dequeue

[Continuous Query Notification](https://node-oracledb.readthedocs.io/en/latest/user_guide/cqn.html) is used to get around this limitation. The message is then pushed to the mobile device using [Parse Server's Push Notification suopprt](https://docs.parseplatform.org/parse-server/guide/#push-notifications). Look at the setCQN cloud code function which registers a callback.

There are a series of Linked In posts that describe this in detail

1. [Building a Custom MBaaS Server with Oracle's MERN Stack](https://www.linkedin.com/pulse/building-custom-mbaas-server-oracles-mern-stack-doug-drechsel-dyjme/)
2. [Using Oracle's Continuous Query Notifications in a Mobile App](https://www.linkedin.com/pulse/using-oracles-continuous-query-notifications-mobile-app-doug-drechsel-jcxse/)
3. [Push Notifications with Oracle's MERN Stack](https://www.linkedin.com/pulse/push-notifications-oracles-mern-stack-doug-drechsel-hmbcf/)


## Use the server connection pool

qapi.js embeds the credentials that are used to get an active conection. A better option would be to use the server's internal connection pool. Added to the cloud dir are;


 connection.js - methods that get connections from the server pool
 qapisrvrpool.js - cloud code functions that now use connection.js to obtain connections from the server pool

To use the server pool, 3 modifications are required for OracleStorageAdapter.js
1. import setter from connection.js
    ```
    import { setOracleStorageAdapter } from '../../../../cloud/connection';
    ```
   this assumes that cloud directory is subordinate to project root

2. call setOracleStorageAdapter in connection.js during construction of Storage Adapter
    ```
    this._uri = options.databaseURI;
    this._collectionPrefix = options.collectionPrefix;
    this._connectionPool = null;
    this._collections = new Map();
    setOracleStorageAdapter(this);
    ```

3. Enable events for connections in the pool. In the connect method add events: true
    ```
        this.connectionPromise = await oracledb.createPool({
          poolAlias: 'parse',
          user: user,
          password: pw,
          connectString: tnsname,
          poolIncrement: 5,
          poolMax: 100,
          poolMin: 3,
          poolTimeout: 10,
          enableStatistics: true,
          events: true,
    ```


Rebuild the server


    npm cache clean --force
    npm ci

Add cloud code location to config.json
```
  "cloud": "./cloud/qapisrvrpool.js",
```

And boot the server

    ORACLE_CLIENT_LOCATION=/Users/myuser/instantclient_19_8  npm start -- ./config.json

