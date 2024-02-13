// Helper file that uses server connections for cloud code

let oracleStorageAdapter;

function setOracleStorageAdapter(adapter) {
    oracleStorageAdapter = adapter;
}

async function getConnection() {
    console.log('Getting connection for Cloud Code');
    const pool = await oracleStorageAdapter.connect();
    const connection = await pool.getConnection();
    return connection;
}

module.exports.setOracleStorageAdapter = setOracleStorageAdapter;
module.exports.getConnection = getConnection;