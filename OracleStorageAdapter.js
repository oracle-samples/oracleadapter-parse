// Copyright (c) 2023, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
import OracleSchemaCollection from './OracleSchemaCollection';
import OracleCollection from './OracleCollection';
import { StorageAdapter } from '../StorageAdapter';
import type { SchemaType, StorageClass, QueryType, QueryOptions } from '../StorageAdapter';
// @flow-disable-next
import Parse from 'parse/node';
// @flow-disable-next
import _ from 'lodash';
import logger from '../../../logger.js';

import {
  transformKey,
  transformWhere,
  transformUpdate,
  parseObjectToOracleObjectForCreate,
  oracleObjectToParseObject,
  transformPointerString,
} from './OracleTransform';
import { Pool } from 'oracledb';

const oracledb = require('oracledb');
const OracleSchemaCollectionName = '_SCHEMA';

const storageAdapterAllCollections = oracleAdapter => {
  const collections = oracleAdapter.listAllCollections(oracleAdapter._collectionPrefix);
  logger.verbose('collections is ' + JSON.stringify(collections));
  return collections;
};

//CDB
//to preserve the original query to hack for $containedBy
var queryBackup = '';
//CDB-END

var initialized = false;
var createConnPool = true;
var schemaCollection = null;

const convertParseSchemaToOracleSchema = ({ ...schema }) => {
  delete schema.fields._rperm;
  delete schema.fields._wperm;

  if (schema.className === '_User') {
    // Legacy mongo adapter knows about the difference between password and _hashed_password.
    // Future database adapters will only know about _hashed_password.
    // Note: Parse Server will bring back password with injectDefaultSchema, so we don't need
    // to add _hashed_password back ever.
    delete schema.fields._hashed_password;
  }

  return schema;
};

// Returns { code, error } if invalid, or { result }, an object
// suitable for inserting into _SCHEMA collection, otherwise.
const oracleSchemaFromFieldsAndClassNameAndCLP = (
  fields,
  className,
  classLevelPermissions,
  indexes
) => {
  const oracleObject = {
    _id: className,
    // TODO: I'm not sure we need objectId
    objectId: 'string',
    updatedAt: 'string',
    createdAt: 'string',
    _metadata: undefined,
  };

  for (const fieldName in fields) {
    const { type, targetClass, ...fieldOptions } = fields[fieldName];
    oracleObject[fieldName] = OracleSchemaCollection.parseFieldTypeToOracleFieldType({
      type,
      targetClass,
    });
    if (fieldOptions && Object.keys(fieldOptions).length > 0) {
      oracleObject._metadata = oracleObject._metadata || {};
      oracleObject._metadata.fields_options = oracleObject._metadata.fields_options || {};
      oracleObject._metadata.fields_options[fieldName] = fieldOptions;
    }
  }

  if (typeof classLevelPermissions !== 'undefined') {
    oracleObject._metadata = oracleObject._metadata || {};
    if (!classLevelPermissions) {
      delete oracleObject._metadata.class_permissions;
    } else {
      oracleObject._metadata.class_permissions = classLevelPermissions;
    }
  }

  if (indexes && typeof indexes === 'object' && Object.keys(indexes).length > 0) {
    oracleObject._metadata = oracleObject._metadata || {};
    oracleObject._metadata.indexes = indexes;
  }

  if (!oracleObject._metadata) {
    // cleanup the unused _metadata
    delete oracleObject._metadata;
  }

  return oracleObject;
};

function validateExplainValue(explain) {
  if (explain) {
    // The list of allowed explain values is from node-mongodb-native/lib/explain.js
    const explainAllowedValues = [
      'queryPlanner',
      'queryPlannerExtended',
      'executionStats',
      'allPlansExecution',
      false,
      true,
    ];
    if (!explainAllowedValues.includes(explain)) {
      throw new Parse.Error(Parse.Error.INVALID_QUERY, 'Invalid value for explain');
    }
  }
}

export class OracleStorageAdapter implements StorageAdapter {
  // private
  _onchange: any;
  _collectionPrefix: string;
  _connectionPool: Pool;
  _collections: Map<String, OracleCollection>;

  constructor(options: any) {
    logger.verbose(
      'OracleStorageAdapter constructor, uri = ' +
        options.databaseURI +
        ' collectionPrefix = ' +
        options.collectionPrefix
    );
    this._uri = options.databaseURI;
    this._collectionPrefix = options.collectionPrefix;
    this._connectionPool = null;
    this._collections = new Map();
  }

  _schemaCollection(): Promise<OracleSchemaCollection> {
    try {
      const collection = this._adaptiveCollection(OracleSchemaCollectionName);
      if (schemaCollection === null) {
        if (!this._stream && this.enableSchemaHooks) {
          // TODO make sure these are all defined
          this._stream = collection._orcaleCollection.watch();
          this._stream.on('change', () => this._onchange());
        }
        schemaCollection = new OracleSchemaCollection(collection);
      }
      return schemaCollection;
    } catch (error) {
      this.handleError(error);
    }
  }

  listAllCollections(prefix) {
    const result = new Array();
    this._collections.forEach(function (value, key) {
      if (key.includes(prefix)) {
        const array = key.split(prefix);
        if (array.length == 2) {
          result.push(array[1]);
        } else {
          result.push(array[0]);
        }
      }
    });
    return result;
  }

  async _truncate(collectionName) {
    logger.verbose('Storage Adapter _truncate for ' + collectionName);
    try {
      const collection = this._adaptiveCollection(collectionName);
      const result = await collection.truncate();
      logger.verbose(
        'Storage Adapter _truncate for collection ' +
          collectionName +
          ' returns ' +
          JSON.stringify(result)
      );
      return result;
    } catch (error) {
      logger.error(
        'Storage Adapter _truncate Error for  collection' + collectionName + '    ERROR = ' + error
      );
      this.handleError(error);
    }
  }

  async _drop(collectionName) {
    logger.verbose('StorageAdapter _drop ' + collectionName);
    try {
      const collection = this._adaptiveCollection(collectionName);
      const result = await collection.drop();
      if (result) {
        // Remove Collection
        logger.verbose('Dropping ' + this._collectionPrefix + collection + ' from collectionMap');
        this._collections.delete(this._collectionPrefix + collectionName);
        if (collectionName.includes(OracleSchemaCollectionName)) {
          schemaCollection = null;
        }
      }
      logger.verbose('StorageAdapter _drop returns ' + result);
      return result;
    } catch (error) {
      logger.error('Storage Adapter _drop Error for ' + collectionName);
      this.handleError(error);
    }
  }

  _adaptiveCollection(name: string): OracleCollection {
    let realName;

    if (name.includes(this._collectionPrefix)) {
      realName = name;
    } else {
      realName = this._collectionPrefix + name;
    }
    // first check if we already have this collection, and if so, just return it
    // this will reuse the same collection and its embedded connection, so we don't
    // create a connection starvation scenario
    if (this._collections.get(realName)) {
      logger.verbose('Adaptive Collection returning Existing collection ' + realName);
      return this._collections.get(realName);
    }

    const collection = new OracleCollection(this, realName);
    this._collections.set(realName, collection);
    logger.verbose('Adaptive Collection returning Created collection ' + realName);
    return collection;
  }

  async initialize() {
    if (initialized === false) {
      const wallet_location = process.env.ORACLE_WALLET_LOCATION;
      const client_location = process.env.ORACLE_CLIENT_LOCATION;

      if (typeof client_location === 'undefined') {
        throw 'Required Environment Variable, ORACLE_CLIENT_LOCATION, is not defined';
      }

      logger.verbose('wallet location = ' + process.env.ORACLE_WALLET_LOCATION);
      logger.verbose('oracle client = ' + process.env.ORACLE_CLIENT_LOCATION);

      try {
        if (typeof wallet_location === 'undefined') {
          logger.info(
            'No Wallet location specified. Intializing Oracle Client to access Local Database Docker Image'
          );
          oracledb.initOracleClient({
            libDir: client_location,
          });
        } else {
          logger.info(
            'Wallet location specified. Intializing Oracle Client to access Cloud Database Instance'
          );
          oracledb.initOracleClient({
            libDir: client_location,
            configDir: wallet_location,
          });
        }
      } catch (error) {
        if (error.message.includes('NJS-077')) {
          // already initialized - so ignore the error
          logger.verbose('oracledb already initialized');
        } else {
          logger.error('Error Initalizing Oracle Client: ' + error);
          // if we get here, probably should exit the whole server process
          process.exit(1);
        }
      }
      initialized = true;
    }
  }

  async connect() {
    if (this.connectionPromise) {
      logger.verbose('reusing connection pool ' + JSON.stringify(this._connectionPool));
      logger.verbose('  statistics: ' + JSON.stringify(this._connectionPool.getStatistics()));
      return this._connectionPool;
    }

    this.initialize();

    var re = new RegExp('oracledb://[a-zA-Z0-9_]*:[^@:]*@[a-zA-Z0-9_.:/]*$');

    if (!re.test(this._uri)) {
      throw 'Incorrect Connection String Format.  Format is oracledb://user:password@tnsname';
    }

    const user = this.getUserFromUri(this._uri);
    const pw = this.getPasswordFromUri(this._uri);
    const tnsname = this.getTnsNameFromUri(this._uri);

    logger.info('creating a connection pool');
    try {
      if (createConnPool) {
        createConnPool = false;
        this.connectionPromise = await oracledb.createPool({
          poolAlias: 'parse',
          user: user,
          password: pw,
          connectString: tnsname,
          poolIncrement: 5,
          poolMax: 100,
          poolMin: 3,
          poolTimeout: 10,
          //  Use default of 60000 ms
          //          queueTimeout: 10,
          enableStatistics: true,
        });
        logger.info('connection pool successfully created');
        this._connectionPool = oracledb.getPool('parse');
        return Promise.resolve(this._connectionPool);
      } else {
        logger.verbose('Returning connection promise while connecting');
        return this.connectionPromise;
      }
    } catch (error) {
      logger.error('Error Creating Connection Pool: ', error);
      throw error;
    }
  }

  getUserFromUri(uri) {
    const myArray = uri.split('//');
    const myArray2 = myArray[1].split(':');
    return myArray2[0];
  }

  getPasswordFromUri(uri) {
    const myArray = uri.split(':');
    const myArray2 = myArray[2].split('@');
    return myArray2[0];
  }

  getTnsNameFromUri(uri) {
    const myArray = uri.split('@');
    return myArray[1];
  }

  handleError<T>(error: ?(Error | Parse.Error)): Promise<T> {
    if (error && error.code === 13) {
      // Unauthorized error
      delete this.client;
      delete this.database;
      delete this.connectionPromise;
      logger.error('Received unauthorized error', { error: error });
    }

    if (typeof error === 'object' && error.code !== 101 && error.code != 137) {
      console.log(JSON.stringify(error));
      if (error.errorNum === 0) {
        console.trace();
      }
    }

    // What to throw?  Maybe need to map ORA msgs to Parse msgs
    // throw error.message;
    throw error;
  }

  classExists(className: string): Promise<boolean> {
    return new Promise(resolve => {
      logger.verbose('classExists name = ' + className);
      const collections = storageAdapterAllCollections(this);
      resolve(collections.includes(className));
    });
  }

  async setClassLevelPermissions(className, CLPs) {
    try {
      logger.verbose('StorageAdapter setClassLevelPermissions for ' + className);
      logger.verbose('setClassLevelPermissions permissions =  ' + JSON.stringify(CLPs));
      const newCLPS = '{"_metadata": {"class_permissions":' + JSON.stringify(CLPs) + '}}';
      const newCLPSObj = JSON.parse(newCLPS);
      const result = await this._schemaCollection().updateSchema(className, newCLPSObj);
      logger.verbose('StorageAdapter setClassLevelPermissions returns ' + result);
      return result;
    } catch (error) {
      logger.error('StorageAdapter setClassLevelPermissions Error for ' + className);
      this.handleError(error);
    }
  }

  async createClass(className: string, schema: SchemaType): Promise<void> {
    try {
      logger.verbose('StorageAdapter createClass for ' + className);
      schema = convertParseSchemaToOracleSchema(schema);
      const oracleObject = oracleSchemaFromFieldsAndClassNameAndCLP(
        schema.fields,
        className,
        schema.classLevelPermissions,
        schema.indexes
      );
      oracleObject._id = className;
      const result = await this._schemaCollection().insertSchema(oracleObject);
      logger.verbose('StorageAdapter createClass insertSchema result =  ' + result);
      if (typeof schema.indexes !== 'undefined' && Object.keys(schema.indexes).length > 0) {
        if (Array.isArray(schema.indexes)) {
          await this.createIndexes(className, schema.indexes);
        } else {
          const indexes = new Array(schema.indexes);
          await this.createIndexes(className, indexes);
        }
      }
      return result;
    } catch (error) {
      logger.error('StorageAdapter createClass Error for ' + className);
      this.handleError(error);
    }
  }

  async addFieldIfNotExists(className: string, fieldName: string, type: any): Promise<void> {
    try {
      logger.verbose('StorageAdapter addFieldIfNotExists for ' + className);
      const result = await this._schemaCollection().addFieldIfNotExists(className, fieldName, type);
      logger.verbose('StorageAdapter addFieldIfNotExists returns ' + result);
      await this.createIndexesIfNeeded(className, fieldName, type);
      return result;
    } catch (error) {
      logger.error('StorageAdapter addFieldIfNotExists Error for ' + className);
      this.handleError(error);
    }
  }

  async updateFieldOptions(className: string, fieldName: string, type: any): Promise<void> {
    const schemaCollection = this._schemaCollection();
    await schemaCollection.updateFieldOptions(className, fieldName, type);
  }

  async deleteClass(className: string): Promise<void> {
    try {
      logger.verbose('StorageAdapter deleteClass for ' + className);
      const result1 = await this._drop(className);
      logger.verbose('StorageAdapter deleteClass drop returns ' + result1);
      const result = await this._schemaCollection().findAndDeleteSchema(className);
      logger.verbose('StorageAdapter deleteClass deleteSchema returns ' + result);
      return result;
    } catch (error) {
      logger.error('StorageAdapter deleteClass Error for ' + className);
      this.handleError(error);
    }
  }

  async deleteAllClasses(fast: boolean) {
    //    let result;
    logger.verbose('entering deleteAllClasses fast = ' + fast);
    const collections = storageAdapterAllCollections(this);
    return Promise.all(
      collections.map(collection => (fast ? this._truncate(collection) : this._drop(collection)))
    );
  }

  async deleteFields(
    className: string,
    schema: SchemaType,
    fieldNames: Array<string>
  ): Promise<void> {
    logger.verbose('StorageAdapter deleteFields for className: ' + className);
    logger.verbose('StorageAdapter deleteFields for schema: ' + schema);
    logger.verbose('StorageAdapter deleteFields oracleFormatNames = ' + fieldNames);
    try {
      const collection = this._adaptiveCollection(className);
      const result = await collection.deleteFields(fieldNames);
      const result1 = await this._schemaCollection().deleteSchemaFields(className, fieldNames);
      logger.verbose('StorageAdapter deleteFields collection result =  ' + result);
      logger.verbose('StorageAdapter deleteFields schemacollection result =  ' + result1);
    } catch (error) {
      logger.error('StorageAdapter deleteFields Error for ' + className);
      this.handleError(error);
    }
  }

  async getAllClasses(): Promise<StorageClass[]> {
    try {
      const schemaCollection = this._schemaCollection();
      const result = await schemaCollection._fetchAllSchemasFrom_SCHEMA();
      logger.verbose('StorageAdapter getAllClasses returns ' + result);
      return result;
    } catch (error) {
      logger.error('StorageAdapter getAllClasses Error');
      this.handleError(error);
    }
  }
  // getClass(className: string): Promise<StorageClass>;

  // TODO: As yet not particularly well specified. Creates an object. Maybe shouldn't even need the schema,
  // and should infer from the type. Or maybe does need the schema for validations. Or maybe needs
  // the schema only for the legacy mongo format. We'll figure that out later.
  async createObject(
    className: string,
    schema: SchemaType,
    object: any,
    transactionalSession: ?any
  ) {
    logger.verbose('StorageAdapter createObject for className: ' + className);

    try {
      schema = convertParseSchemaToOracleSchema(schema);
      const oracleObject = parseObjectToOracleObjectForCreate(className, object, schema);
      const collection = this._adaptiveCollection(className);
      const result = await collection.insertOne(oracleObject, transactionalSession);
      logger.verbose('StorageAdapter createObject insertOne returns: ' + result);
      return { ops: [oracleObject] };
    } catch (error) {
      // "ORA-00001: unique constraint (ADMIN.index_name) violated"
      if (error.errorNum === 1) {
        // Duplicate value
        const err = new Parse.Error(
          Parse.Error.DUPLICATE_VALUE,
          'A duplicate value for a field with unique values was provided'
        );
        err.underlyingError = error;
        if (error.message) {
          const matches = error.message.match(/index:[\sa-zA-Z0-9_\-\.]+\$?([a-zA-Z_-]+)_1/);
          if (matches && Array.isArray(matches)) {
            err.userInfo = { duplicated_field: matches[1] };
          }
        }
        this.handleError(err);
      }
      this.handleError(error);
    }
  }

  async findOneAndUpdate(className, schema, query, update, transactionalSession) {
    try {
      logger.verbose('StorageAdapter findOneAndUpdate for ' + className);
      let oraWhere = transformWhere(className, query, schema);
      const oraUpdate = transformUpdate(className, update, schema);
      // Check if this query needs Oracle Storage Adapter _wperm syntax
      oraWhere = this.checkUserQuery(oraWhere);
      const collection = this._adaptiveCollection(className);
      const result = await collection.findOneAndUpdate(oraWhere, oraUpdate, transactionalSession);
      logger.verbose('StorageAdapter findOneAndUpdate returns ' + JSON.stringify(result));
      return result;
    } catch (error) {
      logger.error('StorageAdapter indOneAndUpdate Error for ' + className);
      this.handleError(error);
    }
  }

  /*
      Parse has ACL formats that are part of a query which causes an error which was fixed in
      https://bug.oraclecorp.com/pls/bug/webbug_print.show?c_rptno=34596223

      Basically, Oracle cannot handle a null as part of an in operator clause
      {_id: "TV5CazXRtP",_wperm: {$in: [null,"*","tE8wEhXmJg","role:Admins",],},}

      This needs to be modified to
      {_id: "tE8wEhXmJg",$or : [{_wperm: {"$in": [ "*", "tE8wEhXmJg" ]}}, {_wperm : null}]}
      to work with Oracle SODA

      Waiting on maintenance update to pick up the fix.
      In the meantime, checkUserQuery will fix up the query

      More here
      https://orahub.oci.oraclecorp.com/ora-microservices-dev/mbaas-parse-server/-/wikis/Error:-ORA-40596:-error-occurred-in-JSON-processing-jznEngValCmpWithTypCnv:invTyp
  */

  checkUserQuery(query) {
    logger.verbose('in StorageAdapter checkUserQuery');
    logger.verbose('Input query = ' + JSON.stringify(query));
    const newObj = new Object();
    const queryObj = JSON.parse(JSON.stringify(query));
    let checkNull = false;

    for (const x in queryObj) {
      if (x === '_wperm' && typeof queryObj[x] === 'object') {
        const myArray = [];
        const json = JSON.parse(JSON.stringify(queryObj[x]));

        for (const y in json) {
          if (y === '$in') {
            if (json[y].length >= 2) {
              for (let i = 0; i < json[y].length; i++) {
                if (json[y][i] === null) {
                  checkNull = true;
                } else {
                  myArray.push(json[y][i]);
                }
              }
            }
          }
          if (json[y].length !== myArray.length) {
            let temp;
            if (checkNull) {
              // Case where no Perms exists on the document
              temp = `[{"_wperm":{"$in":${JSON.stringify(
                myArray
              )}}},{"_wperm":null},{"_wperm":{"$exists":false}}]`;
            } else {
              temp = `[{"_wperm":{"$in":${JSON.stringify(myArray)}}},{"_wperm":null}]`;
            }
            delete queryObj['_wperm'];
            newObj['$or'] = JSON.parse(temp);
          } else {
            newObj[x] = queryObj[x];
          }
        }
      } else {
        newObj[x] = queryObj[x];
      }
      if (x === '_rperm' && typeof queryObj[x] === 'object') {
        const myArray = [];
        const json = JSON.parse(JSON.stringify(queryObj[x]));

        for (const y in json) {
          if (y === '$in') {
            if (json[y].length >= 2) {
              for (let i = 0; i < json[y].length; i++) {
                if (json[y][i] === null) {
                  checkNull = true;
                } else {
                  myArray.push(json[y][i]);
                }
              }
            }
          }
          if (json[y].length !== myArray.length) {
            let rpermOr;
            delete newObj['_rperm'];
            if (checkNull) {
              // Case where no Perms exists on the document
              rpermOr = `[{"_rperm":{"$in":${JSON.stringify(
                myArray
              )}}},{"_rperm":null},{"_rperm":{"$exists":false}}]`;
            } else {
              rpermOr = `[{"_rperm":{"$in":${JSON.stringify(myArray)}}},{"_rperm":null}]`;
            }
            if (Object.prototype.hasOwnProperty.call(newObj, '$or')) {
              // $and the existing $or with the _rperm $or
              const originalOr = JSON.stringify(newObj['$or']);
              const andString = `[{"$or":${originalOr}},{"$or":${rpermOr}}]`;
              // TODO: replacing the $and without checking if it existed
              //       look at lodash to merge
              newObj['$and'] = JSON.parse(andString);
              delete newObj['$or'];
            } else {
              newObj['$or'] = JSON.parse(rpermOr);
            }
          } else {
            newObj[x] = queryObj[x];
          }
        }
      } else {
        newObj[x] = queryObj[x];
      }
    }
    logger.verbose('Return query = ' + JSON.stringify(newObj));
    return newObj;
  }

  async upsertOneObject(className, schema, query, update, transactionalSession) {
    try {
      logger.verbose('StorageAdapter upsertOneObject for Collection ' + className);
      schema = convertParseSchemaToOracleSchema(schema);
      const oraWhere = transformWhere(className, query, schema);
      const oraUpdate = transformUpdate(className, update, schema);
      const collection = this._adaptiveCollection(className);
      const result = await collection.upsertOne(oraWhere, oraUpdate, transactionalSession);
      logger.verbose('StorageAdapter upsertOneObject returns ' + result);
      return result;
    } catch (error) {
      logger.error('StorageAdapter upsertOneObject Error for ' + className);
      this.handleError(error);
    }
  }

  async deleteObjectsByQuery(className, schema, query, transactionalSession) {
    try {
      logger.verbose('StorageAdapter deleteObjectsByQuery for ' + className);
      schema = convertParseSchemaToOracleSchema(schema);
      let oraWhere = transformWhere(className, query, schema);
      // Check if query needs Oracle Storage Adapter _wperm syntax
      oraWhere = this.checkUserQuery(oraWhere);
      const collection = this._adaptiveCollection(className);
      const result = await collection.deleteObjectsByQuery(oraWhere, transactionalSession);
      logger.verbose('StorageAdapter deleteObjectsByQuery returns ' + result);
      return result;
    } catch (error) {
      logger.error('StorageAdapter deleteObjectsByQuery Error for ' + className);
      this.handleError(error);
    }
  }

  // Executes a find. Accepts: className, query in Parse format, and { skip, limit, sort }.
  find(
    className: string,
    schema: SchemaType,
    query: QueryType,
    { skip, limit, sort, keys, readPreference, hint, caseInsensitive, explain }: QueryOptions
  ): Promise<any> {
    //    try {
    logger.verbose('StorageAdapter find for ' + className);
    validateExplainValue(explain);
    schema = convertParseSchemaToOracleSchema(schema);
    logger.verbose('query = ' + JSON.stringify(query));

    // start hack
    // this is a temporary hack while i work on the _rperm stuff
    // remove that from the query if present
    //CDB
    //to preserve the original query to hack for $containedBy
    queryBackup = query;
    //CDB-END
    // end hack

    let oracleWhere = transformWhere(className, query, schema);
    // Check if this query needs Oracle Storage Adapter _wperm syntax
    oracleWhere = this.checkUserQuery(oracleWhere);
    logger.verbose('oracleWhere = ' + JSON.stringify(oracleWhere));
    // fix 15-11
    const oracleSort = _.mapKeys(sort, (value, fieldName) =>
      transformKey(className, fieldName, schema)
    );

    const sortTypes = new Object();
    for (const s in sort) {
      let schemaFieldName;
      let sortType = 'string';
      if (s.split('.').length > 1) {
        schemaFieldName = s.split('.')[0];
      } else {
        schemaFieldName = s;
      }
      const schemaTypeEntry = schema.fields[schemaFieldName];
      const schemaType = schemaTypeEntry[Object.keys(schemaTypeEntry)[0]];
      if (schemaType === 'Number') {
        sortType = 'number';
      }
      sortTypes[s] = sortType;
    }

    logger.verbose('Make linter happy by using keys = ' + keys);

    const oracleKeys = keys;

    logger.verbose('oracleKeys = ' + JSON.stringify(oracleKeys));
    logger.verbose('make linter ignore ' + readPreference);

    const collection = this._adaptiveCollection(className);
    return collection
      .find(oracleWhere, {
        skip,
        limit,
        sort: oracleSort,
        keys: oracleKeys,
        maxTimeMS: this._maxTimeMS,
        readPreference: null,
        hint,
        caseInsensitive,
        explain,
        sortTypes,
      })
      .then(objects => {
        logger.verbose('after the find, objects = ' + JSON.stringify(objects));
        logger.verbose('about to map oracleObjectToParseObject');
        let result = objects.map(object => oracleObjectToParseObject(className, object, schema));
        logger.verbose('result = ' + JSON.stringify(result));

        //CDB
        //$containedBy issue: remove extra documents from the collection with Diff between two Sets
        if (JSON.stringify(queryBackup).indexOf('$containedBy') > -1) {
          for (var prop in queryBackup) {
            if (!(/*typeof*/ (queryBackup[prop].$containedBy === undefined))) {
              //let arr = queryBackup[prop].$containedBy;
              //11-11fix for 'containedBy number array'
              var filteredResult = result.filter(function (myObject) {
                const diff = myObject[prop].filter(
                  x => !queryBackup[prop].$containedBy.includes(x)
                );
                return diff.length == 0;
              });
              result = filteredResult;
              /*
                  for (const r in result) {
                    const myObject = result[r];
                    const diff = myObject[prop].filter(x => !queryBackup[prop].$containedBy.includes(x));

                    if (diff.length > 0) {
                      //remove document
                      result.splice(r, 1);
                    }
                  }
                  */
              //END11-11 fix
            }
          }
        }
        //CDB-END

        //CDB
        //Delete all fields not in oracleKeys
        if (!(typeof oracleKeys === 'undefined')) {
          for (const r in result) {
            logger.verbose('oracleKeys to mantain = ' + JSON.stringify(oracleKeys));
            //to be cleaned
            const myObject = result[r];

            var oracleKeysSet = new Set(oracleKeys);
            oracleKeysSet.add('createdAt');
            oracleKeysSet.add('updatedAt');
            oracleKeysSet.add('objectId');

            var keysResult = new Set(Object.keys(myObject));
            logger.verbose('keys remained = ' + JSON.stringify(keysResult));

            const diff = new Set([...keysResult].filter(element => !oracleKeysSet.has(element)));
            logger.verbose('keys toDel = ' + JSON.stringify(diff));

            for (var iter = diff.values(), toDel = null; (toDel = iter.next().value);) {
              // Do NOT remove _rperm and _wperm. DatabaseController uses them to value ParseObject.ACL
              if (!(toDel === '_rperm' || toDel === '_wperm')) {
                delete myObject[toDel];
              }
            }
            logger.verbose('properties remained = ' + JSON.stringify(myObject));
          }
        }
        //CDB-END
        logger.verbose('StorageAdapter find returns ' + result);
        return result;
      })
      .catch(err => this.handleError(err));
  }

  async setIndexesFromOracle(className: string) {
    try {
      logger.verbose('StorageAdapter setIndexesFromOracle for ' + className);
      const indexes = await this.getIndexes(className);
      const result = await this._schemaCollection().updateSchema(className, {
        _metadata: { indexes: indexes },
      });
      logger.verbose('StorageAdapter setIndexesFromOracle returns ' + result);
      return result;
    } catch (error) {
      logger.error('StorageAdapter setIndexesFromOracle throws for className ' + className);
      this.handleError(error);
    }
  }

  createTextIndexesIfNeeded(className: string, query: QueryType, schema: any): Promise<void> {
    logger.verbose('entered createTextIndexesIfNeeded query = ' + JSON.stringify(query));
    for (const fieldName in query) {
      logger.verbose('processing field ' + fieldName);
      if (!query[fieldName] || !query[fieldName].$text) {
        continue;
      }
      const existingIndexes = schema.indexes;
      logger.verbose('existingIndexes = ' + existingIndexes);
      for (const key in existingIndexes) {
        const index = existingIndexes[key];
        if (Object.prototype.hasOwnProperty.call(index, fieldName)) {
          return Promise.resolve();
        }
      }
      const indexName = `${fieldName}_text`;
      const textIndex = {
        [indexName]: { [fieldName]: 'text' },
      };
      return this.setIndexesWithSchemaFormat(
        className,
        textIndex,
        existingIndexes,
        schema.fields
      ).catch(error => {
        logger.error('got error ' + JSON.stringify(error));
        if (error.code === 85) {
          // Index exist with different options
          return this.setIndexesFromOracle(className);
        }
        throw error;
      });
    }
    return Promise.resolve();
  }
  /*
  TODO:
    Are multiple indexes processed? I think not because of
    const fieldName = Object.keys(indexCreationRequest)[0];
    Also, can the code creating the indexspec in
       ensureIndex and ensureUniqueness
    be combined into 1 method
    https://orahub.oci.oraclecorp.com/ora-microservices-dev/mbaas-parse-server/-/issues/35
  */
  async ensureIndex(
    className: string,
    schema: SchemaType,
    fieldNames: string[],
    indexName: ?string,
    caseInsensitive: boolean = false,
    options?: Object = {}
  ): Promise<any> {
    try {
      logger.verbose('StorageAdapter ensureIndex for ' + className);
      schema = convertParseSchemaToOracleSchema(schema);
      const indexCreationRequest = {};
      const oracleFieldNames = fieldNames.map(fieldName =>
        transformKey(className, fieldName, schema)
      );
      oracleFieldNames.forEach(fieldName => {
        indexCreationRequest[fieldName] = options.indexType !== undefined ? options.indexType : 1;
      });

      logger.verbose(
        'use these to make linter happy ' +
          JSON.stringify(indexName) +
          ' ' +
          JSON.stringify(caseInsensitive)
      );

      const fieldName = Object.keys(indexCreationRequest)[0];
      const indexRequest = {
        name: fieldName,
        fields: [
          {
            path: fieldName,
          },
        ],
        unique: true,
      };
      const collection = this._adaptiveCollection(className);
      const result = await collection._createIndex(indexRequest);
      logger.verbose('StorageAdapter ensureIndex returns ' + result);
      return result;
    } catch (error) {
      logger.error('StorageAdapter ensureIndex throws for className ' + className);
      this.handleError(error);
    }
  }

  // Create a unique index. Unique indexes on nullable fields are not allowed. Since we don't
  // currently know which fields are nullable and which aren't, we ignore that criteria.
  // As such, we shouldn't expose this function to users of parse until we have an out-of-band
  // Way of determining if a field is nullable. Undefined doesn't count against uniqueness,
  // which is why we use sparse indexes.
  async ensureUniqueness(className: string, schema: SchemaType, fieldNames: string[]) {
    try {
      logger.verbose('StorageAdapter ensureUniqueness for ' + className);
      schema = convertParseSchemaToOracleSchema(schema);
      const indexCreationRequest = {};
      const oracleFieldNames = fieldNames.map(fieldName =>
        transformKey(className, fieldName, schema)
      );
      oracleFieldNames.forEach(fieldName => {
        indexCreationRequest[fieldName] = 1;
      });
      const fieldName = Object.keys(indexCreationRequest)[0];
      const indexRequest = {
        name: fieldName,
        fields: [
          {
            path: fieldName,
          },
        ],
        unique: true,
      };

      const collection = this._adaptiveCollection(className);
      const result = await collection._ensureSparseUniqueIndexInBackground(indexRequest);
      logger.verbose('StorageAdapter ensureUniqueness returns ' + result);
      return result;
    } catch (error) {
      logger.error('StorageAdapter ensureUniqueness throws for className ' + className);
      this.handleError(error);
    }
  }

  async count(className, schema, query, readPreference, hint) {
    const skip = 0;
    const limit = 0;
    const sort = {};
    let keys;
    const caseInsensitive = false;
    const explain = false;
    // See line 1183 in DatabaseController, it passes null in query
    if (query === null) {
      query = {};
    }
    return this.find(className, schema, query, {
      skip,
      limit,
      sort,
      keys,
      readPreference,
      hint,
      caseInsensitive,
      explain,
    })
      .then(collection => {
        return collection.length;
      })
      .catch(err => {
        logger.error('in the catch block after collection.find for count()');
        this.handleError(err);
      });
  }

  //CDB Fix 18-11
  async distinct(className, schema, query, fieldName) {
    try {
      logger.verbose('StorageAdapter distinct for ' + className);
      schema = convertParseSchemaToOracleSchema(schema);
      const isPointerField =
        schema.fields[fieldName] && schema.fields[fieldName].type === 'Pointer';
      const transformField = transformKey(className, fieldName, schema);
      const collection = this._adaptiveCollection(className);
      let objects = collection.distinct(transformField, transformWhere(className, query, schema));
      objects = objects.filter(obj => obj != null);
      logger.verbose('StorageAdapter distinct returns ' + objects);
      return objects.map(object => {
        if (isPointerField) {
          return transformPointerString(schema, fieldName, object);
        }

        return oracleObjectToParseObject(className, object, schema);
      });
    } catch (error) {
      logger.error('StorageAdapter distinct throws for className ' + className);
      this.handleError(error);
    }
  }

  //TO BE TESTED
  /*
  aggregate(
    className: string,
    schema: any,
    pipeline: any,
    readPreference: ?string,
    hint: ?mixed,
    explain?: boolean
  ) {
    validateExplainValue(explain);
    let isPointerField = false;
    pipeline = pipeline.map(stage => {
      if (stage.$group) {
        stage.$group = this._parseAggregateGroupArgs(schema, stage.$group);
        if (
          stage.$group._id &&
          typeof stage.$group._id === 'string' &&
          stage.$group._id.indexOf('$_p_') >= 0
        ) {
          isPointerField = true;
        }
      }
      if (stage.$match) {
        stage.$match = this._parseAggregateArgs(schema, stage.$match);
      }
      if (stage.$project) {
        stage.$project = this._parseAggregateProjectArgs(schema, stage.$project);
      }
      if (stage.$geoNear && stage.$geoNear.query) {
        stage.$geoNear.query = this._parseAggregateArgs(schema, stage.$geoNear.query);
      }
      return stage;
    });
    readPreference = this._parseReadPreference(readPreference);
    return this._adaptiveCollection(className)
      .then(collection =>
        collection.aggregate(pipeline, {
          readPreference,
          maxTimeMS: this._maxTimeMS,
          hint,
          explain,
        })
      )
      .then(results => {
        results.forEach(result => {
          if (Object.prototype.hasOwnProperty.call(result, '_id')) {
            if (isPointerField && result._id) {
              result._id = result._id.split('$')[1];
            }
            if (
              result._id == null ||
              result._id == undefined ||
              (['object', 'string'].includes(typeof result._id) && _.isEmpty(result._id))
            ) {
              result._id = null;
            }
            result.objectId = result._id;
            delete result._id;
          }
      });
        return results;
      })
      .then(objects => objects.map(object => oracleObjectToParseObject(className, object, schema)))
      .catch(err => this.handleError(err));
  }
*/
  //CDB-END

  performInitialization(): Promise<void> {
    return Promise.resolve();
  }

  watch(callback: () => void): void {
    this._onchange = callback;
  }

  createIndexPaths(index) {
    var paths = Array();

    Object.keys(index).forEach(key => {
      paths.push({
        path: key,
      });
    });
    return paths;
  }

  async createIndexes(className: string, indexes: any) {
    try {
      logger.verbose('StorageAdapter createIndexes for ' + className);
      var promises = Array();
      const collection = this._adaptiveCollection(className);

      for (let idx = 0; idx < indexes.length; idx++) {
        const index = indexes[idx];

        let idxName = Object.keys(index)[0];
        let paths;
        /*
          2 index formats can be passed in
          { key: { aString: 1 }, name: 'name1' }
          { name1: { aString: 1 } }
          Handle them both
        */
        if (idxName === 'key') {
          paths = index[idxName];
          idxName = index['name'];
        } else {
          paths = index[idxName];
        }

        const indexRequest = {
          name: idxName,
          fields: this.createIndexPaths(paths),
          unique: true,
        };
        const promise = await collection._createIndex(indexRequest);
        promises.push(promise);
      }
      const results = await Promise.all(promises);
      logger.verbose('StorageAdapter createIndexes returns ' + results);
      return results;
    } catch (error) {
      logger.error('StorageAdapter createIndexes throws for className ' + className);
      this.handleError(error);
    }
  }

  async getIndexes(className: string, connection: ?any): Promise<void> {
    try {
      logger.verbose(
        'StorageAdapter getIndexes for ' + className + '   Connection = ' + connection
      );
      const collection = this._adaptiveCollection(className);
      const result = collection.getIndexes(className);
      logger.verbose('StorageAdapter getIndexes returns ' + result);
      return result;
    } catch (error) {
      logger.error('StorageAdapter getIndexes throws for className ' + className);
      this.handleError(error);
    }
  }

  updateSchemaWithIndexes() {
    return this.getAllClasses()
      .then(classes => {
        const promises = classes.map(schema => {
          return this.setIndexesFromOracle(schema.className);
        });
        return Promise.all(promises);
      })
      .catch(err => this.handleError(err));
  }

  async setIndexesWithSchemaFormat(
    className: string,
    submittedIndexes: any,
    existingIndexes: any = {},
    fields: any
  ): Promise<void> {
    try {
      logger.verbose('StorageAdapter setIndexesWithSchemaFormat for ' + className);
      if (submittedIndexes === undefined) {
        return Promise.resolve();
      }
      if (Object.keys(existingIndexes).length === 0) {
        existingIndexes = { _id_: { _id: 1 } };
      }
      const deletePromises = [];
      const insertedIndexes = [];

      for (let i = 0; i < Object.keys(submittedIndexes).length; i++) {
        const name = Object.keys(submittedIndexes)[i];
        const field = submittedIndexes[name];
        if (existingIndexes[name] && field.__op !== 'Delete') {
          throw new Parse.Error(Parse.Error.INVALID_QUERY, `Index ${name} exists, cannot update.`);
        }
        if (!existingIndexes[name] && field.__op === 'Delete') {
          throw new Parse.Error(
            Parse.Error.INVALID_QUERY,
            `Index ${name} does not exist, cannot delete.`
          );
        }
        if (field.__op === 'Delete') {
          const promise = await this.dropIndex(className, name);
          deletePromises.push(promise);
          delete existingIndexes[name];
        } else {
          Object.keys(field).forEach(key => {
            if (
              !Object.prototype.hasOwnProperty.call(
                fields,
                key.indexOf('_p_') === 0 ? key.replace('_p_', '') : key
              )
            ) {
              throw new Parse.Error(
                Parse.Error.INVALID_QUERY,
                `Field ${key} does not exist, cannot add index.`
              );
            }
          });
          existingIndexes[name] = field;
          insertedIndexes.push({
            key: field,
            name,
          });
        }
      }
      if (insertedIndexes.length > 0) {
        const insertPromise = await this.createIndexes(className, insertedIndexes);
        logger.verbose(
          'StorageAdapter setIndexesWithSchemaFormat insertPromise =  ' + insertPromise
        );
      }
      // Munge existing indexs into expected format based on Shema.spec.js tests
      const newExistindIndexes =
        '{"_metadata": {"indexes":' + JSON.stringify(existingIndexes) + '}}';
      const newExistingIndexesObj = JSON.parse(newExistindIndexes);
      await Promise.all(deletePromises);
      const result = await this._schemaCollection().updateSchemaIndexes(
        className,
        newExistingIndexesObj
      );
      logger.verbose('StorageAdapter setIndexesWithSchemaFormat returns ' + result);
      return result;
    } catch (error) {
      logger.error('StorageAdapter setIndexesWithSchemaFormat throws for className ' + className);
      this.handleError(error);
    }
  }

  // createTransactionalSession(): Promise<any>;
  // commitTransactionalSession(transactionalSession: any): Promise<void>;
  // abortTransactionalSession(transactionalSession: any): Promise<void>;

  async dropIndex(className: string, index: any) {
    try {
      logger.verbose('StorageAdapter dropIndex for ' + className);
      const collection = this._adaptiveCollection(className);
      const result = await collection.dropIndex(index);
      logger.verbose('StorageAdapter dropIndex returns ' + result);
      return result;
    } catch (error) {
      logger.error('StorageAdapter dropIndex throws for className ' + className);
      this.handleError(error);
    }
  }

  createIndexesIfNeeded(className, fieldName, type) {
    // The original Method impl from Mongo below
    // Not sure if we need the 2dsphere index for Geo, may be Mongo specific
    logger.verbose(
      'createIndexesIfNeeded use className, fieldName and type to make linter happy ' +
        className +
        ' ' +
        fieldName +
        ' ' +
        type
    );
    /*    if (type && type.type === 'Polygon') {
      const index = {
        [fieldName]: '2dsphere'
      };
      return this.createIndex(className, index);
    }*/
    return Promise.resolve();
  }
}

export default OracleStorageAdapter;
