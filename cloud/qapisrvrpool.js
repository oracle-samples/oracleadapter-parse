'use strict';

Error.stackTraceLimit = Infinity;

const oracledb = require('oracledb');
const { getConnection } = require('./connection');

Parse.Cloud.define('createQ', async req => {

  console.log(req.params.qname);
  const QUEUE_NAME = req.params.qname;
  const RAW_TABLE = QUEUE_NAME + "_TAB";

  /*  
    Hardcoded sys credentials for demo purposes
    Sys is needed for Q creation 
  */
  const dbaCredential = { 
    user: "sys",
    password: "Welcome12345",    
    connectString: "localhost:1521/FREEPDB1",
    privilege: oracledb.SYSDBA  // Needed when running aginst local Free23c Image
  };

  const plsql = `
  BEGIN
      EXECUTE IMMEDIATE ('
        GRANT AQ_ADMINISTRATOR_ROLE, AQ_USER_ROLE TO pdbadmin
      ');
      EXECUTE IMMEDIATE ('
        GRANT EXECUTE ON DBMS_AQ TO pdbadmin
      ');
      EXECUTE IMMEDIATE ('
        GRANT CHANGE NOTIFICATION TO pdbadmin
      ');      
  END;`;

  const connAsDBA = await oracledb.getConnection(dbaCredential);
  await connAsDBA.execute(plsql);
  await connAsDBA.close();

  // Get connection from server pool
  const connection = await getConnection();

  const plsql2 = `
    BEGIN
      DBMS_AQADM.CREATE_QUEUE_TABLE(
        QUEUE_TABLE        =>  'pdbadmin.${RAW_TABLE}',
        QUEUE_PAYLOAD_TYPE =>  'RAW'
      );
      DBMS_AQADM.CREATE_QUEUE(
        QUEUE_NAME         =>  'pdbadmin.${QUEUE_NAME}',
        QUEUE_TABLE        =>  'pdbadmin.${RAW_TABLE}'
      );
      DBMS_AQADM.START_QUEUE(
        QUEUE_NAME         => 'pdbadmin.${QUEUE_NAME}'
      );
    END;`;
  await connection.execute(plsql2);
  await connection.close();
  return "Queue " + QUEUE_NAME + " Successfully Created";
});

Parse.Cloud.define('enqueMsg', async req => {

  console.log(req.params.qname);
  console.log(req.params.msg);

  let connection;
  try {
    const connection = await getConnection();
    const queue = await connection.getQueue(req.params.qname);
    await queue.enqOne(req.params.msg);
    await connection.commit();
    return "ENQUE Success";
  } catch (err) {
    console.error(err);
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.error(err);
        throw err;
      }
    }
  }
});

Parse.Cloud.define('dequeMsg', async req => {  

  console.log(req.params.qname); 
  let connection;
  try {
    const connection = await getConnection();
    const queue = await connection.getQueue(req.params.qname);
    const msg = await queue.deqOne();
    await connection.commit();
    console.log("DEQ MSG =" + msg.payload.toString());
    return msg.payload.toString();
  } catch (err) {
    console.error(err);
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.error(err);
        throw err;
      }
    }
  }
});

Parse.Cloud.define('unsubscribe', async () => {

  let connection;
  try {
    const connection = await getConnection();
    connection.unsubscribe('mysub')
    console.log("Unsubscribe Success");
  } catch (err) {
    console.error(err);
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.error(err);
        throw err;
      }
    }
  }


});


Parse.Cloud.define('setCQN', async () => {

  const options = {
      sql      : `select * from  FRED_TAB`,  // query of interest        
      callback : myCallback,                 // method called by notifications
      clientInitiated : true                 // For Oracle DB & Client 19.4 or later
    };
    
    const connection = await getConnection();
    await connection.subscribe('mysub', options);
    console.log("Subscribe to CQN Success");
    await connection.close();
    return "Subscription SUCCESS";
});

async function myCallback(message) {

  console.log("IN MY CALLBACK");
  console.log(message);
  console.log("Message type:", message.type);
  console.log("Message database name:", message.dbName);
  console.log("Message transaction id:", message.txId);

  for (const table of message.tables) {
    console.log("--> Table Name:", table.name);
    console.log("--> Table Operation:", table.operation);
    if(oracledb.CQN_OPCODE_INSERT & table.operation) {
      console.log("WE have an insert");
      const msg = await deq();
      console.log("Deque returned msg = " + msg);
      pushMSG(msg);
      console.log("PUSHED MSG");
    }

  }
}

function pushMSG(msg)
{
  try {
    Parse.Push.send({
      where: { "deviceType": {
        "$in": [
          "ios"
        ]
      } },
      data: { 
        "alert": msg
      },
    }, { useMasterKey: true });
  } catch (err) {
    console.error(err);
  }  
}

async function deq() {
  console.log("IN DEQUE");
  const QUEUE_NAME = "FRED"; 

  let connection;
  try {
    const connection = await getConnection();
    const queue = await connection.getQueue(QUEUE_NAME);
    const msg = await queue.deqOne();
    await connection.commit();
    console.log("DEQ MSG =" + msg.payload.toString());
    return msg.payload.toString();
  } catch (err) {
    console.error(err);
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.error(err);
      }
    }
  }  

}
