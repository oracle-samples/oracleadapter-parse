// Copyright (c) 2023, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
import Parse from 'parse/node';
import logger from '../../../logger.js';
import _ from 'lodash';
import OracleStorageAdapter from './OracleStorageAdapter';

const oracledb = require('oracledb');
oracledb.autoCommit = true;
const Collection = oracledb.SodaCollection;
const SodaDB = oracledb.SodaDB;

const DB_VERSION = process.env.ORACLEDB_VERSION;
const ddlTimeOut = `
BEGIN
    EXECUTE IMMEDIATE ('
    alter session set ddl_lock_timeout=1000
    ');
END;`;

export default class OracleCollection {
  _oracleSodaDB: SodaDB;
  _oracleCollection: Collection;
  _oracleStorageAdapter: OracleStorageAdapter;
  _name: string;
  indexes = new Array();
  idIndexCreating = false;
  jsonSQLtype = 'JSON'; //DBVersion 23c default

  constructor(oracleStorageAdapter: OracleStorageAdapter, collectionName: String) {
    this._oracleStorageAdapter = oracleStorageAdapter;
    this._name = collectionName;
    this._oracleCollection = undefined;
    logger.verbose('Oracle Database Version = ' + DB_VERSION);
    // To support backwards compatibility with instant clients
    if (typeof DB_VERSION !== 'undefined' && DB_VERSION !== '23') {
      this.jsonSQLtype = 'BLOB';
    }
  }

  async getCollectionConnection() {
    const mymetadata = {
      keyColumn: { name: 'ID', assignmentMethod: 'UUID' },
      contentColumn: { name: 'JSON_DOCUMENT', sqlType: this.jsonSQLtype },
      versionColumn: { name: 'VERSION', method: 'UUID' },
      lastModifiedColumn: { name: 'LAST_MODIFIED' },
      creationTimeColumn: { name: 'CREATED_ON' },
    };

    logger.verbose('getCollectionConnection about to connect for collection ' + this._name);
    let localConn;
    this._oracleCollection = await this._oracleStorageAdapter
      .connect()
      .then(p => {
        logger.verbose('getCollectionConnection about to get connection from pool ');
        logger.verbose('  statistics: ' + JSON.stringify(p.getStatistics()));
        return p.getConnection();
      })
      .then(conn => {
        logger.verbose('getCollectionConnection about to get SodaDB');
        localConn = conn;
        return conn.getSodaDatabase();
      })
      .then(sodadb => {
        logger.verbose('getCollectionConnection open collection for  ' + this._name);
        this._oracleSodaDB = sodadb;
        return sodadb.openCollection(this._name);
      })
      .then(async coll => {
        if (!coll) {
          logger.verbose('getCollectionConnection create NEW collection for  ' + this._name);
          const newCollection = await this._oracleSodaDB.createCollection(this._name, {
            metaData: mymetadata,
          });

          /*
            Create index on _id for every new collection
            This imitates Mongo behavior which happens automatically

            Index names MUST be unique in a schema, append table name
            cannot have two indexes with the same name in a single schema.
          */
          if (!this.idIndexCreating) {
            this.idIndexCreating = true;
            const indexName = 'ididx' + this._name;
            const indexSpec = { name: indexName, unique: true, fields: [{ path: '_id' }] };
            await newCollection.createIndex(indexSpec);
            logger.verbose(
              'getCollectionConnection successfully create _id index for  ' + this._name
            );
            // Add _id if it doesn't exist to indexes array
            const found = this.indexes.find(item => {
              return Object.keys(item)[0] === '_id_';
            });
            if (typeof found === 'undefined') {
              this.indexes.push({ _id_: { _id: 1 } });
            }
          }
          return newCollection;
        }
        return coll;
      })
      .catch(error => {
        logger.error('getCollectionConnection ERROR:  ' + error);
        throw error;
      });
    logger.verbose(
      'getCollectionConnection returning collection for  ' +
        this._name +
        ' returned ' +
        this._oracleCollection
    );
    return localConn;
  }

  // Atomically updates data in the database for a single (first) object that matched the query
  // If there is nothing that matches the query - does insert
  // Postgres Note: `INSERT ... ON CONFLICT UPDATE` that is available since 9.5.
  async upsertOne(query, update, session) {
    /*
      UpsertOne is of the form
      where query =
      {"_id": "HasAllPOD"}
      and update = the new document
      {"_id": "HasAllPOD","numPODs": 17"}

      in this case if update fails becuase no document existed then
      rerunning the query would return 0 and indicate an insert
    */

    logger.verbose('in upsertOne query = ' + JSON.stringify(query));
    logger.verbose('use session to make linter happy ' + JSON.stringify(session));
    // TODO need to use save(), which is the SODA equivalent of upsert() andit takes a SodaDocument
    let docs;
    let promise;

    try {
      promise = await this.findOneAndUpdate(query, update, null);
      logger.verbose('Upsert Promise = ' + promise);
      if (promise === false) {
        logger.verbose('Upsert Insert for query ' + JSON.stringify(query));
        promise = await this._rawFind(query, { type: 'sodadocs' }).then(d => (docs = d));
        if (docs && docs.length == 0) {
          // Its an insert so merge query into update
          _.merge(update, query);
          promise = await this.insertOne(update);
        }
      }
      return promise;
    } catch (error) {
      logger.error('Collection UpsertOne throws ' + error);
      throw error;
    }
  }

  async findOneAndUpdate(query, update, transactionalSession) {
    try {
      logger.verbose('in Collection findOneAndUpdate query = ' + JSON.stringify(query));
      logger.verbose(
        'use transactionalSession to make linter happy ' + JSON.stringify(transactionalSession)
      );

      // TODO:  Fix updatedAt, it should be _updatedAt because its an internal field
      //              and updatedAt doesn't get updated for Schemas

      let updateObj;

      let result = await this._rawFind(query, { type: 'one' }).then(result => {
        return result;
      });
      //************************************************************************************************/
      // Modify Update based on Mongo operators
      //
      // Look for $unset, Mongo's deleteField
      // Create array of fieldNames to be deleted
      const newUpdate = new Object();
      const fieldNames = new Array();
      Object.keys(update).forEach(item => {
        if (item === '$unset') {
          Object.keys(update[item]).forEach(item => {
            fieldNames.push(item);
          });
        } else {
          if (item === '_updated_at') {
            newUpdate['updatedAt'] = update[item];
          } else {
            newUpdate[item] = update[item];
          }
        }
      });

      // if FieldNames > 0, delete those fields and
      // repalce update with newUpdate that has the $unset pairs removed
      // Don't move deletefields to update transform code
      if (fieldNames.length > 0) {
        await this.deleteFields(fieldNames).then(result => {
          update = newUpdate;
          return result;
        });
        // Ya changed the key values get them again
        result = await this._rawFind(query, { type: 'one' }).then(result => {
          return result;
        });
      }

      // Process Increments  $inc
      const newIncUpdate = new Object();
      let incUpdt = false;
      Object.keys(update).forEach(item => {
        if (item === '$inc') {
          Object.keys(update[item]).forEach(it2 => {
            incUpdt = true;
            _.set(result.content, it2, _.result(result.content, it2) + update[item][it2]);
          });
        } else {
          if (item === '_updated_at') {
            newIncUpdate['updatedAt'] = update[item];
          } else {
            newIncUpdate[item] = update[item];
          }
        }
      });

      if (incUpdt) {
        update = newIncUpdate;
      }

      // Process $AddToSet operator adds a value to an array unless the value is already present, in which case $addToSet does nothing to that array.
      const newAddToSetUpdate = new Object();
      let addToSetUpdt = false;
      Object.keys(update).forEach(item => {
        if (item === '$addToSet') {
          Object.keys(update[item]).forEach(it2 => {
            Object.keys(update[item][it2]).forEach(it3 => {
              if (it3 === '$each') {
                const updtArray = update[item][it2][it3];
                // Check for dot notation
                const temp = it2.split('.');
                let newArray;
                if (temp.length > 1) {
                  newArray = result.content[temp[0]][temp[1]];
                } else {
                  newArray = result.content[it2];
                }
                updtArray.forEach(updt => {
                  if (typeof updt === 'object') {
                    if (!newArray.some(entry => Object.keys(entry)[0] === Object.keys(updt)[0])) {
                      addToSetUpdt = true;
                      newArray.push(updt);
                    }
                  } else {
                    if (!newArray.includes(updt)) {
                      addToSetUpdt = true;
                      newArray.push(updt);
                    }
                  }
                });
              }
            });
          });
        } else {
          if (item === '_updated_at') {
            newAddToSetUpdate['updatedAt'] = update[item];
          } else {
            newAddToSetUpdate[item] = update[item];
          }
        }
      });

      if (addToSetUpdt) {
        update = newAddToSetUpdate;
      }

      // Process $pullAll operator removes all instances of the specified values from an existing array.
      const newPullAllUpdate = new Object();
      let pullAllUpdt = false;
      Object.keys(update).forEach(item => {
        if (item === '$pullAll') {
          Object.keys(update[item]).forEach(it2 => {
            const updtArray = update[item][it2];
            const rsltArray = result.content[it2];
            const newArray = new Array();
            updtArray.forEach(updt => {
              if (typeof updt === 'object') {
                rsltArray.forEach(entry => {
                  if (Object.keys(entry)[0] != Object.keys(updt)[0]) {
                    newArray.push(entry);
                    pullAllUpdt = true;
                  }
                });
              }
              newPullAllUpdate[it2] = newArray;
            });
          });
        } else {
          if (item === '_updated_at') {
            newPullAllUpdate['updatedAt'] = update[item];
          } else {
            newPullAllUpdate[item] = update[item];
          }
        }
      });

      if (pullAllUpdt) {
        update = newPullAllUpdate;
      }

      // End of Transform Update
      //************************************************************************************************/

      if (result && Object.keys(result).length > 0) {
        // found the doc, so we need to update it
        const key = result.key;
        logger.verbose('key = ' + key);
        const version = result.version;
        logger.verbose('version = ' + version);
        const oldContent = result.content;

        logger.verbose('oldContent = ' + JSON.stringify(oldContent));
        logger.verbose('update = ' + JSON.stringify(update));

        // Check for empty object and remove it from original, no merging, replacing
        Object.keys(update).forEach(item => {
          if (
            typeof update[item] === 'object' &&
            update[item] !== null &&
            item !== 'updatedAt' &&
            Object.keys(update[item]).length === 0
          ) {
            _.unset(oldContent, item);
          }
        });

        if (update.fieldName) {
          const theUpdate = { [update.fieldName]: update.theFieldType };
          logger.verbose('theUpdate = ' + JSON.stringify(theUpdate));
          updateObj = { ...oldContent, ...theUpdate };
        } else {
          if (pullAllUpdt || update['_metadata']) {
            // Handle set or merge for _metadata in Schema
            Object.keys(update).forEach(item => {
              const found = Object.keys(oldContent).find(item => {
                return item === '_metadata';
              });
              if (item === '_metadata') {
                if (found) {
                  if (
                    Object.prototype.hasOwnProperty.call(oldContent[item], 'class_permissions') &&
                    Object.prototype.hasOwnProperty.call(update[item], 'class_permissions')
                  ) {
                    // Just reset class_permissions to update
                    _.set(oldContent[item], 'class_permissions', update[item]['class_permissions']);
                  } else {
                    _.merge(oldContent['_metadata'], update[item]);
                  }
                } else {
                  _.set(oldContent, item, update[item]);
                }
              } else {
                _.set(oldContent, item, update[item]);
              }
            });
            updateObj = oldContent;
          } else {
            updateObj = _.merge(oldContent, update);
          }
        }
        logger.verbose('Updated Object = ' + JSON.stringify(updateObj));
        let localConn = null;
        return this.getCollectionConnection()
          .then(conn => {
            localConn = conn;
            return this._oracleCollection.find().key(key).version(version).replaceOne(updateObj);
          })
          .then(result => {
            if (result.replaced == true) {
              return updateObj;
            } else {
              return 'retry';
            }
          })
          .finally(async () => {
            if (localConn) {
              await localConn.close();
              localConn = null;
            }
          })
          .catch(error => {
            logger.error('Find One and Update replaceOne ERROR = ', error);
            throw error;
          });
      } else {
        logger.verbose('No Docs, nothing to update, return false');
        return false;
      }
    } catch (error) {
      logger.error('Find One and Update ERROR = ', error);
      throw error;
    }
  }

  async updateSchemaIndexes(query, update) {
    // This method just updates Schema _metadata.indexes
    // It is laways a set (replace), never a merge
    logger.verbose('in Collection updateSchemaIndexes query = ' + JSON.stringify(query));
    logger.verbose('update = ' + JSON.stringify(update));
    const result = await this._rawFind(query, { type: 'one' }).then(result => {
      return result;
    });
    if (Object.keys(result).length > 0) {
      // found the doc, so we need to update it
      const key = result.key;
      logger.verbose('key = ' + key);
      const version = result.version;
      logger.verbose('version = ' + version);
      const oldContent = result.content;
      logger.verbose('oldContent = ' + JSON.stringify(oldContent));
      logger.verbose('update = ' + JSON.stringify(update));
      // Either set or merge _metadata depending on if it existed before
      Object.keys(update).forEach(item => {
        if (item === '_metadata') {
          if (Object.prototype.hasOwnProperty.call(oldContent, item)) {
            if (Object.prototype.hasOwnProperty.call(oldContent[item], 'indexes')) {
              if (
                Object.keys(update).length <= Object.keys(oldContent['_metadata']['indexes']).length
              ) {
                // Its a delete.  Parse deletes by sending an update with the deleted index
                // Set Indexes w Update only
                _.set(oldContent[item], 'indexes', update[item]['indexes']);
              } else {
                _.merge(oldContent['_metadata'], update[item]);
              }
            } else {
              _.merge(oldContent['_metadata'], update[item]);
            }
          } else {
            _.set(oldContent, item, update[item]);
          }
        }
      });
      const updateObj = oldContent;
      logger.verbose('Updated Object = ' + JSON.stringify(updateObj));

      let localConn = null;
      return this.getCollectionConnection()
        .then(conn => {
          localConn = conn;
          return this._oracleCollection.find().key(key).version(version).replaceOne(updateObj);
        })
        .then(result => {
          if (result.replaced == true) {
            return update;
          } else {
            return 'retry';
          }
        })
        .finally(async () => {
          if (localConn) {
            await localConn.close();
            localConn = null;
          }
        })
        .catch(error => {
          logger.error('updateSchemaIndexes update ERROR: ', error);
          throw error;
        });
    } else {
      logger.verbose('updateSchemaIndexes No record found for query: ' + JSON.stringify(query));
      return false;
    }
  }
  catch(error) {
    logger.error('updateSchemaIndexes ERROR: ', error);
    throw error;
  }

  async findOneAndDelete(query: string) {
    try {
      logger.verbose('in Collection findOneAndDelete query = ' + JSON.stringify(query));

      const result = await this._rawFind(query, { type: 'one' }).then(result => {
        return result;
      });

      if (Object.keys(result).length > 0) {
        // found the doc, so we need to update it
        const key = result.key;
        logger.verbose('key = ' + key);
        const version = result.version;
        logger.verbose('version = ' + version);

        let localConn = null;
        return this.getCollectionConnection()
          .then(conn => {
            localConn = conn;
            return this._oracleCollection.find().key(key).version(version).remove();
          })
          .finally(async () => {
            if (localConn) {
              await localConn.close();
              localConn = null;
            }
          })
          .catch(error => {
            logger.error('Find One and Delete remove ERROR: ', error);
            throw error;
          });
      } else {
        logger.verbose('Find One and Delete No record found for query: ' + JSON.stringify(query));
      }
    } catch (error) {
      logger.error('Find One and Delete ERROR: ', error);
      throw error;
    }
  }

  async deleteObjectsByQuery(query, transactionalSession) {
    try {
      logger.verbose('in Collection deleteObjectsByQuery query = ' + JSON.stringify(query));
      logger.verbose(
        'use transactionalSession to make linter happy ' + JSON.stringify(transactionalSession)
      );

      const result = await this._rawFind(query, { type: 'all' }).then(result => {
        return result;
      });

      if (result.length > 0) {
        for (let i = 0; i < result.length; i++) {
          // found the doc, so we need to update it
          const key = result[i].key;
          logger.verbose('key = ' + key);
          const version = result[i].version;
          logger.verbose('version = ' + version);
          let localConn = null;
          return this.getCollectionConnection()
            .then(conn => {
              localConn = conn;
              return this._oracleCollection.find().key(key).version(version).remove();
            })
            .finally(async () => {
              if (localConn) {
                await localConn.close();
                localConn = null;
              }
            })
            .catch(error => {
              logger.error('Delete Objects By Query remove ERROR: ', error);
              throw error;
            });
        }
      } else {
        throw new Parse.Error(Parse.Error.OBJECT_NOT_FOUND, 'Object not found.');
      }
    } catch (error) {
      logger.error('Delete Objects By Query ERROR: ', error);
      throw error;
    }
  }

  // Delete fields from all documents in a collection
  async deleteFields(fieldNames: Array<string>) {
    try {
      var promises = Array();
      // Rewriting like createIndexes, Collection method will just delete a field
      logger.verbose(
        'DeleteFields ' + JSON.stringify(fieldNames) + ' for Collection ' + this._name
      );
      for (let idx = 0; idx < fieldNames.length; idx++) {
        const fieldName = fieldNames[idx];
        logger.verbose('about to delete field' + fieldName);
        const promise = this.deleteFieldFromCollection(fieldName)
          .then(promise => {
            if (promise === 'retry') {
              return this.deleteFieldFromCollection(fieldName);
            }
            return promise;
          })
          .catch(error => {
            logger.error('Collection deleteFields caught error ' + error.message);
            throw error;
          });
        promises.push(promise);
      }

      const results = await Promise.all(promises);
      logger.verbose('DeleteFields returns ' + results);
      return results;
    } catch (error) {
      logger.error('Delete Fields ERROR: ', error);
      throw error;
    }
  }

  // deleteField from all docs in a collection that has it
  async deleteFieldFromCollection(fieldName: string) {
    try {
      logger.verbose('deleteFieldFromCollection fieldName to delete is ' + fieldName);
      const query = JSON.parse(`{"${fieldName}":{"$exists":true}}`);
      const result = await this._rawFind(query, { type: 'all' }).then(result => {
        return result;
      });

      if (result.length > 0) {
        // found the doc, so we need to update it
        var promises = Array();
        for (let i = 0; i < result.length; i++) {
          const promise = this.deleteField(
            fieldName,
            result[i].key,
            result[i].version,
            result[i].content
          )
            .then(promise => {
              if (promise === 'retry') {
                return this.deleteFieldFromCollection(fieldName);
              }
              return promise;
            })
            .catch(error => {
              logger.error('deleteFieldFromConnection caught error ' + error.message);
              throw error;
            });
          promises.push(promise);
        }

        const results = await Promise.all(promises);
        logger.verbose('DeleteFieldFromCollection returns ' + results);
        return results;
      } else {
        logger.verbose('Field ' + fieldName + ' Not Found In DeleteFieldFromCollection');
        return false;
      }
    } catch (error) {
      logger.error('Delete Field ERROR: ', error);
      throw error;
    }
  }

  // deleteField from a specific document containing it
  async deleteField(fieldName: string, key: string, version: string, oldContent: string) {
    logger.verbose('key = ' + key);
    logger.verbose('version = ' + version);
    logger.verbose('oldContent before delete = ' + JSON.stringify(oldContent));
    delete oldContent[fieldName];
    logger.verbose('oldContent after delete update = ' + JSON.stringify(oldContent));

    let localConn = null;
    return this.getCollectionConnection()
      .then(conn => {
        localConn = conn;
        return this._oracleCollection.find().key(key).version(version).replaceOne(oldContent);
      })
      .then(result => {
        if (result.replaced == true) {
          return oldContent;
        } else {
          return 'retry';
        }
      })
      .finally(async () => {
        if (localConn) {
          await localConn.close();
          localConn = null;
        }
      })
      .catch(error => {
        logger.error('DeleteFieldFromCollection replaceOne ERROR: ', error);
        throw error;
      });
  }

  //  Delete a field in a specific SCHEMA doc
  async deleteSchemaField(query: string, fieldName: string) {
    try {
      logger.verbose('fieldName to delete is ' + fieldName);
      const existobj = JSON.parse(`{"${fieldName}":{"$exists":true}}`);
      const newquery = { ...query, ...existobj };
      const result = await this._rawFind(newquery, { type: 'one' }).then(result => {
        return result;
      });

      if (result) {
        // found the doc, so we need to update it
        const key = result.key;
        logger.verbose('key = ' + key);
        const version = result.version;
        logger.verbose('version = ' + version);
        const oldContent = result.content;

        logger.verbose('oldContent before delete = ' + JSON.stringify(oldContent));
        delete oldContent[fieldName];
        logger.verbose('oldContent after delete update = ' + JSON.stringify(oldContent));

        let localConn = null;
        return this.getCollectionConnection()
          .then(conn => {
            localConn = conn;
            return this._oracleCollection.find().key(key).version(version).replaceOne(oldContent);
          })
          .then(result => {
            if (result.replaced == true) {
              return oldContent;
            } else {
              return 'retry';
            }
          })
          .finally(async () => {
            if (localConn) {
              await localConn.close();
              localConn = null;
            }
          })
          .catch(error => {
            logger.error('Delete SCHEMA Field replaceOne ERROR: ', error.message);
            throw error;
          });
      } else {
        logger.verbose('Field ' + fieldName + ' Not Found In DeleteSchemaField');
        return false;
      }
    } catch (error) {
      logger.error('Delete SCHEMA Field ERROR: ', error);
      throw error;
    }
  }

  // Does a find with "smart indexing".
  // Currently this just means, if it needs a geoindex and there is
  // none, then build the geoindex.
  // This could be improved a lot but it's not clear if that's a good
  // idea. Or even if this behavior is a good idea.
  async find(
    query,
    {
      skip,
      limit,
      sort,
      keys,
      maxTimeMS,
      readPreference,
      hint,
      caseInsensitive,
      explain,
      sortTypes,
    } = {}
  ) {
    try {
      logger.verbose('entering find()');
      // Support for Full Text Search - $text
      if (keys && keys.$score) {
        delete keys.$score;
        keys.score = { $meta: 'textScore' };
      }

      return this._rawFind(
        query,
        { type: 'content' },
        {
          skip,
          limit,
          sort,
          keys,
          maxTimeMS,
          readPreference,
          hint,
          caseInsensitive,
          explain,
          sortTypes,
        }
      ).then(result => {
        return result;
      });
    } catch (error) {
      logger.verbose("in find()'s error block");
      // Check for "no geoindex" error
      if (error.code != 17007 && !error.message.match(/unable to find index for .geoNear/)) {
        throw error;
      }
      // Figure out what key needs an index
      const key = error.message.match(/field=([A-Za-z_0-9]+) /)[1];
      if (!key) {
        throw error;
      }
      // TODO: Need to fix up this call to DB
      // TODO:  MUST FIX
      var index = {};
      index[key] = '2d';
      await this.getCollectionConnection();

      const result = await this._oracleCollection
        .createIndex(index)
        // Retry, but just once.
        .then(() =>
          this._rawFind(query, {
            skip,
            limit,
            sort,
            keys,
            maxTimeMS,
            readPreference,
            hint,
            caseInsensitive,
            explain,
          })
        );
      this.closeConnection();
      return result.map(i => i.getContent());
    }
  }

  async _rawFind(
    query,
    retval,
    {
      skip,
      limit,
      sort,
      keys,
      maxTimeMS,
      readPreference,
      hint,
      caseInsensitive,
      explain,
      sortTypes,
    } = {}
  ) {
    logger.verbose('_rawFind: collection = ' + JSON.stringify(this._oracleCollection));
    logger.verbose('query = ' + JSON.stringify(query));
    logger.verbose('limit = ' + limit);
    // use these so the linter will not complain - until i actually use them properly
    logger.verbose(
      'TODO: not using these: ' + sort,
      maxTimeMS,
      readPreference,
      caseInsensitive,
      explain
    );

    let localConn = null;
    try {
      let findOperation;

      await this.getCollectionConnection()
        .then(conn => {
          localConn = conn;
          findOperation = this._oracleCollection.find();
        })
        .catch(async error => {
          logger.error('Error getting connection in _rawFind, ERROR =' + error);
          if (localConn) {
            await localConn.close();
            localConn = null;
          }
          throw error;
        });

      //    let findOperation = this._oracleCollection.find(); // find() is sync and returns SodaOperation

      //  All this below is to handle empty array in $in selection
      //  Node APIs fail for empty array error
      //  The fix will be in a future release of instant client
      //  https://orahub.oci.oraclecorp.com/ora-microservices-dev/mbaas-parse-server/-/wikis/ORA-40676:-invalid-Query-By-Example-(QBE)-filter-specification-JZN-00305:-Array-of-values-was-empty
      const myObj = JSON.parse(JSON.stringify(query));

      for (const x in myObj) {
        if (typeof myObj[x] === 'object') {
          const json = JSON.parse(JSON.stringify(myObj[x]));

          //CDB
          //to manage EqualTo() with null
          // when an input query is like
          // {"foo":null,"$or":[{"_rperm":{"$in":["*","*"]}},{"_rperm":null},{"_rperm":{"$exists":false}}]}
          // and need to generate a $or for null check, need to wrap the whole thing with a $and
          // It looks like null = non-existance or null
          if (json == null) {
            let newQuery = {};

            if (Object.prototype.hasOwnProperty.call(myObj, '$or')) {
              // This whole not handling null is getting ugly
              const originalOr = JSON.stringify(myObj['$or']);
              const queryOr = JSON.stringify({ $or: [{ [x]: { $exists: false } }, { [x]: null }] });
              const andString = `[${queryOr},{"$or":${originalOr}}]`;
              newQuery['$and'] = JSON.parse(andString);
              delete myObj['$or'];
            } else {
              newQuery = { $or: [{ [x]: { $exists: false } }, { [x]: null }] };
            }
            query = newQuery;
          }
          //CDB-END
          //CDB
          //to manage notEqualTo() with null
          if (json != null) {
            if (Object.keys(json)[0] == '$ne') {
              if (json['$ne'] == null) {
                const newQuery = { $and: [{ [x]: { $exists: true } }, { [x]: { $ne: null } }] };
                query = newQuery;
              }
            }
          }
          //CDB-END

          //CDD
          // Remove empty objects from $and clause
          // ORA-40676: invalid Query-By-Example (QBE) filter specification
          // JZN-00315: Empty objects not allowed
          //
          // fix up queries like
          // { '$and': [ {}, { _p_user: '_User$EYTVvcG4j9' } ] }
          if (json != null && x == '$and') {
            if (Array.isArray(json)) {
              const condList = new Array();
              json.forEach(item => {
                if (!(Object.keys(item).length === 0)) {
                  condList.push(item);
                }
              });
              query = {
                $and: condList,
              };
            }
          }
          //CDD

          for (const y in json) {
            //query should not match on array when searching for null
            if (y === '$all' && Array.isArray(json[y]) && json[y][0] == null) {
              if (localConn) {
                await localConn.close();
                localConn = null;
              }
              return [];
            } else {
              // to manage $all of normal expression for query match on array with multiple objects
              if (
                y === '$all' &&
                Array.isArray(json[y]) &&
                json[y][0]['__FIELD__!!__'] === undefined
              ) {
                const newCondList = Array();

                for (var ass in myObj[x]['$all']) {
                  if (typeof myObj[x]['$all'][ass] === 'object') {
                    // ???
                    const condList = myObj[x]['$all'][0];
                    Object.keys(condList).forEach(function (key) {
                      // key: the name of the object key
                      // index: the ordinal position of the key within the object
                      const newField = x + '[*].' + key;
                      newCondList.push({
                        [newField]: condList[key],
                      });
                    });
                  }
                }
                // For 'containsAll date array queries','containsAll string array queries','containsAll number array queries'
                // no 'objects' in array: doesn't need a query re-write in $and:[] 'for query match on array with multiple objects'
                // newCondList == []
                if (newCondList.length != 0) {
                  query = {
                    $and: newCondList,
                  };
                }
              } //CDB
            }

            if (y === '$in' || y === '$nin' || y === '$all') {
              if (json[y].length > 0 && json[y][0] !== null) {
                //TO MANAGE 'containsAllStartingWith single empty value returns empty results' test
                if (
                  Object.keys(json[y][0]).length == 0 &&
                  y === '$all' &&
                  typeof json[y][0] == 'object'
                ) {
                  if (localConn) {
                    await localConn.close();
                    localConn = null;
                  }
                  return [];
                }
              }

              if (json[y].length == 0) {
                if (y === '$in' || y === '$all') {
                  if (localConn) {
                    await localConn.close();
                    localConn = null;
                  }
                  return [];
                } else {
                  query = JSON.parse('{}');
                }
              }
            }
            // to manage $all of $regex expression
            //To exclude a $all on $regex array to be transformed in $and

            /* CDD Commented this code out becuase it broke this query
               {"numbers":{"$all":[1,2,3]}
               and this test
               containsAll number array queries
               */

            /*          if (y === '$all' && json[y][0]['__FIELD__!!__'] === undefined) {
              //find wrong field
              for (ass in myObj[x]['$all']) {
                if (typeof myObj[x]['$all'][ass] === 'object') {
                  if (Object.keys(ass)[0] != '$regex') {
                    //TO BE FIXED
                    if (localConn) {
                      localConn.close();
                      localConn = null;
                    }
                    return [];
                  }
                }
              } //To manage 'containsAll number array queries' in conflict with 'containsAllStartingWith single empty value returns empty results' test
              if (localConn) {
                localConn.close();
                localConn = null;
              }
              return [];
            }*/

            if (y === '$all' && !(json[y][0]['__FIELD__!!__'] === undefined)) {
              const condList = [];

              for (const condition in query[x][y]) {
                condList.push({
                  [x]: query[x][y][condition]['__FIELD__!!__'],
                });
              }

              query = {
                $and: condList,
              };
            } //CDB-END
          }

          // Let $or just passthrough
          if (x === '$or') {
            query[x] = myObj[x];
          }
        }
      } //CDB

      if (sort && Object.keys(sort).length != 0) {
        //ADD ORDER IN QUERY
        //FIX 15-11
        const orderByList = []; //let collection = new OracleSchemaCollection(this._oracleCollection);
        for (const s in sort) {
          const order = sort[s] == -1 ? 'desc' : 'asc';
          const orderStatement = {
            path: s,
            datatype: sortTypes[s],
            order: order,
          }; //Fix 11-11

          orderByList.push(orderStatement);
        } //Fix 15-11

        const oldQuery = query;
        query = {};
        query['$query'] = oldQuery;
        query['$orderby'] = orderByList; //Fix-End 11-11
      } // CDB-END

      findOperation = findOperation.filter(query);

      if (skip) {
        findOperation = findOperation.skip(Number(skip));
      }

      if (limit) {
        findOperation = findOperation.limit(Number(limit));
      }

      if (hint) {
        findOperation = findOperation.hint(String(hint));
      }
      // TODO need to handle sort and readPreference
      // let findOperation = this._oracleCollection.find(query, {
      //   skip,
      //   limit,
      //   sort,
      //   readPreference,
      //   hint,
      // });

      if (keys) {
        logger.verbose('keys.. with input = ' + JSON.stringify(keys));
        // param needs to be an Array
        // check it is not an empty object...
        if (!_.isEmpty(keys)) {
          logger.verbose('keys was not empty');
          //CDB
          //findOperation = findOperation.keys(keys);
          //CDB-END
        }
      }

      // if (caseInsensitive) {
      //   findOperation = findOperation.collation(OracleCollection.caseInsensitiveCollation());
      // }

      // if (maxTimeMS) {
      //   findOperation = findOperation.maxTimeMS(maxTimeMS);
      // }

      logger.verbose('findOperation = ' + JSON.stringify(findOperation));
      logger.verbose('about to getDocuments()');
      let localDocs;
      return findOperation
        .getDocuments()
        .then(docs => {
          if (retval.type === 'content') {
            localDocs = docs.map(i => i.getContent());
          }
          if (retval.type === 'sodadocs') {
            localDocs = docs;
          }
          if (retval.type === 'one') {
            // return docs, keys and version
            if (docs && docs.length == 1) {
              const one = new Object();
              one.content = docs[0].getContent();
              one.key = docs[0].key;
              one.version = docs[0].version;
              localDocs = one;
            } else {
              if (docs && docs.length == 0) {
                return {};
              } else {
                logger.error('rawFind ONE return type found multiple docs');
                throw 'rawFind ONE return type found multiple docs';
              }
            }
          }
          if (retval.type === 'all') {
            // return docs, keys and version
            if (docs) {
              const returndocs = new Array();
              for (var i = 0; i < docs.length; i++) {
                const all = new Object();
                all.content = docs[i].getContent();
                all.key = docs[i].key;
                all.version = docs[i].version;
                returndocs.push(all);
              }
              localDocs = returndocs;
            }
          }
          return localDocs;
        })
        .finally(async () => {
          if (localConn) {
            await localConn.close();
            localConn = null;
          }
        })
        .catch(error => {
          logger.error('Error running findOperation GetDocuments, ERROR =' + error);
          throw error;
        });
    } catch (error) {
      if (localConn) {
        await localConn.close();
        localConn = null;
      }
      logger.error('Error running _rawfind, ERROR =' + error);
      throw error;
    }
  }

  //CDB 17-11 fix

  async distinct(field, query) {
    //return this._oracleCollection.distinct(field, query);
    const objects = await this._oracleCollection.find().filter(query).getDocuments();
    const arr = [];
    for (const obj in objects) {
      const content = _.get(objects[obj].getContent(), field);
      Array.isArray(content) ? arr.push(...content) : arr.push(content);
    }
    //let distinctObjects = [...new Set(arr)];
    return [...new Set(arr)];
  }
  //CDB-END

  async updateOne(query, update) {
    logger.verbose('UpdateOne calling findOneandUpdate');
    return this.findOneAndUpdate(query, update, null);
  }

  async insertOne(object) {
    let localConn = null;

    return this.getCollectionConnection()
      .then(conn => {
        localConn = conn;
        localConn.execute(ddlTimeOut);
        const result = this._oracleCollection.insertOne(object);
        return result;
      })
      .finally(async () => {
        if (localConn) {
          await localConn.close();
          localConn = null;
        }
      })
      .catch(error => {
        logger.error('error during insertOne = ' + error);
        throw error;
      });
  }

  async drop() {
    let localConn = null;

    logger.verbose('entered drop for ' + this._name);
    return this.getCollectionConnection()
      .then(conn => {
        localConn = conn;
        return this._oracleCollection.drop();
      })
      .then(result => {
        if (result) {
          logger.verbose('drop succeeded for  ' + this._name);
        } else {
          logger.verbose('drop failed for  ' + this._name);
        }
        return result;
      })
      .finally(async () => {
        if (localConn) {
          await localConn.close();
          localConn = null;
        }
      })
      .catch(error => {
        logger.error('in Drop Error' + error);
        throw error;
      });
  }

  async truncate() {
    // collection.truncate() does not work with instant clients less than version 20
    // https://oracle.github.io/node-oracledb/doc/api.html#-11212-sodacollectiontruncate
    // Error: DPI-1050: Oracle Client library is at version 19.8 but version 20.1 or higher is needed
    // for now, do it the old fashioned way with collection.find.remove
    let localConn = null;
    return this.getCollectionConnection()
      .then(conn => {
        localConn = conn;
        return this._oracleCollection.find().remove();
      })
      .finally(async () => {
        if (localConn) {
          await localConn.close();
          localConn = null;
        }
      })
      .catch(error => {
        logger.error('in truncate Error' + error);
        throw error;
      });
  }
  async _fetchAllSchemasFrom_SCHEMA() {
    return this._rawFind({}, { type: 'content' })
      .then(schemas => {
        logger.verbose('schemas = ' + schemas);
        return schemas;
      })
      .catch(error => {
        logger.error('error during fetchAllSchemasFrom_SCHEMA = ' + error);
        throw error;
      });
  }

  getCollectionName() {
    return this._name;
  }

  _ensureSparseUniqueIndexInBackground(indexRequest) {
    // TODO rewrite params to suit oracle soda
    logger.verbose(
      'entered _ensureSparseUniqueIndexInBackground with indexRequest = ' +
        JSON.stringify(indexRequest)
    );
    return this._createIndex(indexRequest);
  }

  async _createIndex(indexSpec) {
    let localConn = null;

    logger.verbose('_createIndex index spec is ' + JSON.stringify(indexSpec));
    return await this.getCollectionConnection()
      .then(async conn => {
        localConn = conn;
        await localConn.execute(ddlTimeOut);
        await this._oracleCollection.createIndex(indexSpec);
        return Promise.resolve;
      })
      .then(result => {
        // Parse expects _id index in Schema to be
        // _metadata: { indexes: { _id_: { _id: 1 }, name_1: { name: 1 } } }
        const idx = { [indexSpec.fields[0].path]: 1 };
        if (indexSpec.fields[0].path === '_id') {
          //          indexSpec.fields[0].path = '_id_';
          indexSpec.name = '_id_';
        }
        //        const obj = { [indexSpec.fields[0].path]: [idx] };
        const obj = { [indexSpec.name]: [idx] };
        this.indexes.push(obj);
        return result;
      })
      .finally(async () => {
        if (localConn) {
          await localConn.close();
          localConn = null;
        }
      })
      .catch(error => {
        if (error.errorNum === 40733) {
          /*
          Rebuild internal indexes array on server restart from schema indexes
          */
          const found = this.indexes.find(item => {
            // Parse expects _id index in Schema to be
            // _metadata: { indexes: { _id_: { _id: 1 }, name_1: { name: 1 } } }
            if (indexSpec.fields[0].path === '_id') {
              indexSpec.fields[0].path = '_id_';
            }
            return Object.keys(item)[0] === indexSpec.fields[0].path;
          });

          if (typeof found === 'undefined') {
            const idx = { [indexSpec.fields[0].path]: 1 };
            if (indexSpec.fields[0].path === '_id') {
              indexSpec.name = '_id_';
            }
            //            const obj = { [indexSpec.fields[0].path]: [idx] };
            const obj = { [indexSpec.name]: [idx] };
            this.indexes.push(obj);
            return Promise.resolve;
          }
          logger.verbose('Index' + JSON.stringify(indexSpec) + ' already exists');
        } else {
          logger.error('createIndex throws ' + error);
          throw error;
        }
      });
  }

  getIndexes(className) {
    logger.verbose('OracleCollection getIndexes className = ' + className);

    // There is an odd case where _id is not added to schema document until server restart
    // If _id_ does not exist in indexes array add it to returned array.
    // It does exist on the actual Collection
    const found = this.indexes.find(item => {
      return Object.keys(item)[0] === '_id_';
    });
    if (typeof found === 'undefined') {
      this.indexes.push({ _id_: { _id: 1 } });
    }
    logger.verbose('getIndexes returns ' + JSON.stringify(this.indexes));
    return this.indexes;
  }

  async dropIndex(indexName) {
    logger.verbose('Collection ' + this._name + ' is dropping index' + indexName);
    let localConn = null;

    const result = await this.getCollectionConnection()
      .then(async conn => {
        localConn = conn;
        const result = await this._oracleCollection.dropIndex(indexName);
        return result;
      })
      .finally(async () => {
        if (localConn) {
          await localConn.close();
          localConn = null;
        }
      })
      .catch(error => {
        logger.error('error during dropIndex = ' + error);
        throw error;
      });

    const found = this.indexes.find(item => {
      return Object.keys(item)[0] === indexName;
    });
    if (found) {
      this.indexes.splice(this.indexes.indexOf(found), 1);
    }

    return result;
  }
}
