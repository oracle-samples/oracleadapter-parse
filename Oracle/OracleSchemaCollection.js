// Copyright (c) 2023, Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
import logger from '../../../logger.js';
import OracleCollection from './OracleCollection';
import Parse from 'parse/node';

function _objectWithoutProperties(source, excluded) {
  if (source == null) return {};
  var target = _objectWithoutPropertiesLoose(source, excluded);
  var key, i;
  if (Object.getOwnPropertySymbols) {
    var sourceSymbolKeys = Object.getOwnPropertySymbols(source);
    for (i = 0; i < sourceSymbolKeys.length; i++) {
      key = sourceSymbolKeys[i];
      if (excluded.indexOf(key) >= 0) continue;
      if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue;
      target[key] = source[key];
    }
  }
  return target;
}
function _objectWithoutPropertiesLoose(source, excluded) {
  if (source == null) return {};
  var target = {};
  var sourceKeys = Object.keys(source);
  var key, i;
  for (i = 0; i < sourceKeys.length; i++) {
    key = sourceKeys[i];
    if (excluded.indexOf(key) >= 0) continue;
    target[key] = source[key];
  }
  return target;
}

const emptyCLPS = Object.freeze({
  find: {},
  count: {},
  get: {},
  create: {},
  update: {},
  delete: {},
  addField: {},
  protectedFields: {},
});

const defaultCLPS = Object.freeze({
  find: { '*': true },
  count: { '*': true },
  get: { '*': true },
  create: { '*': true },
  update: { '*': true },
  delete: { '*': true },
  addField: { '*': true },
  protectedFields: { '*': [] },
});

function oracleFieldToParseSchemaField(type) {
  if (type[0] === '*') {
    return {
      type: 'Pointer',
      targetClass: type.slice(1),
    };
  }

  if (type.startsWith('relation<')) {
    return {
      type: 'Relation',
      targetClass: type.slice('relation<'.length, type.length - 1),
    };
  }

  switch (type) {
    case 'number':
      return { type: 'Number' };
    case 'string':
      return { type: 'String' };
    case 'boolean':
      return { type: 'Boolean' };
    case 'date':
      return { type: 'Date' };
    case 'map':
    case 'object':
      return { type: 'Object' };
    case 'array':
      return { type: 'Array' };
    case 'geopoint':
      return { type: 'GeoPoint' };
    case 'file':
      return { type: 'File' };
    case 'bytes':
      return { type: 'Bytes' };
    case 'polygon':
      return { type: 'Polygon' };
  }
}

// Returns a type suitable for inserting into mongo _SCHEMA collection.
// Does no validation. That is expected to be done in Parse Server.
function parseFieldTypeToOracleFieldType({ type, targetClass }) {
  switch (type) {
    case 'Pointer':
      return `*${targetClass}`;
    case 'Relation':
      return `relation<${targetClass}>`;
    case 'Number':
      return 'number';
    case 'String':
      return 'string';
    case 'Boolean':
      return 'boolean';
    case 'Date':
      return 'date';
    case 'Object':
      return 'object';
    case 'Array':
      return 'array';
    case 'GeoPoint':
      return 'geopoint';
    case 'File':
      return 'file';
    case 'Bytes':
      return 'bytes';
    case 'Polygon':
      return 'polygon';
  }
}

const nonFieldSchemaKeys = ['_id', '_metadata', '_client_permissions'];
function oracleSchemaFieldsToParseSchemaFields(schema) {
  var fieldNames = Object.keys(schema).filter(key => nonFieldSchemaKeys.indexOf(key) === -1);
  var response = fieldNames.reduce((obj, fieldName) => {
    obj[fieldName] = oracleFieldToParseSchemaField(schema[fieldName]);
    if (
      schema._metadata &&
      schema._metadata.fields_options &&
      schema._metadata.fields_options[fieldName]
    ) {
      obj[fieldName] = Object.assign(
        {},
        obj[fieldName],
        schema._metadata.fields_options[fieldName]
      );
    }
    return obj;
  }, {});
  response.ACL = { type: 'ACL' };
  response.createdAt = { type: 'Date' };
  response.updatedAt = { type: 'Date' };
  response.objectId = { type: 'String' };
  return response;
}

function oracleSchemaToParseSchema(oracleSchema) {
  let clps = defaultCLPS;
  let indexes = {};
  if (oracleSchema._metadata) {
    if (oracleSchema._metadata.class_permissions) {
      clps = { ...emptyCLPS, ...oracleSchema._metadata.class_permissions };
    }
    if (oracleSchema._metadata.indexes) {
      indexes = { ...oracleSchema._metadata.indexes };
    }
  }
  return {
    className: oracleSchema._id,
    fields: oracleSchemaFieldsToParseSchemaFields(oracleSchema),
    classLevelPermissions: clps,
    indexes: indexes,
  };
}

function _oracleSchemaQueryFromNameQuery(name: string, query) {
  const object = { _id: name };
  if (query) {
    Object.keys(query).forEach(key => {
      object[key] = query[key];
    });
  }
  return object;
}

class OracleSchemaCollection {
  _collection: OracleCollection;

  constructor(collection: OracleCollection) {
    this._collection = collection;
  }

  async _fetchAllSchemasFrom_SCHEMA() {
    let theSchemas;

    return this._collection._fetchAllSchemasFrom_SCHEMA().then(schemas => {
      theSchemas = schemas.map(oracleSchemaToParseSchema);
      return theSchemas;
    });
  }

  _fetchOneSchemaFrom_SCHEMA(name: string) {
    return this._collection
      ._rawFind(_oracleSchemaQueryFromNameQuery(name), { type: 'content' }, { limit: 1 })
      .then(results => {
        if (results.length === 1) {
          const contentOfSchema = oracleSchemaToParseSchema(results[0]);
          return contentOfSchema;
        } else {
          throw undefined;
        }
      });
  }

  insertSchema(schema: any) {
    logger.verbose('entered insertSchema for ' + schema);
    return this._collection
      .insertOne(schema)
      .then(() => {
        return oracleSchemaToParseSchema(schema);
      })
      .catch(error => {
        logger.error('got error ' + error);
        if (error.message.includes('ORA-00001')) {
          throw new Parse.Error(Parse.Error.DUPLICATE_VALUE, 'Class already exists.');
        }
        throw error;
      });
  }

  async updateSchema(name: string, update) {
    return await this._collection.updateOne(_oracleSchemaQueryFromNameQuery(name), update);
  }

  upsertSchema(name: string, query: string, update) {
    logger.verbose('in upsertSchema query = ' + JSON.stringify(query));

    return this._collection
      .findOneAndUpdate(_oracleSchemaQueryFromNameQuery(name, query), update, null)
      .then(promise => {
        if (promise === 'retry') {
          return this.upsertSchema(name, query, update);
        }
        return promise;
      })
      .catch(error => {
        logger.error('in upsertSchema caught error ' + error);
        throw error;
      });
  }

  async updateSchemaIndexes(name: string, update) {
    return this._collection
      .updateSchemaIndexes(_oracleSchemaQueryFromNameQuery(name), update)
      .then(promise => {
        if (promise === 'retry') {
          return this.updateSchemaIndexes(_oracleSchemaQueryFromNameQuery(name), update);
        }
        return promise;
      })
      .catch(error => {
        logger.error('SchemaCollection updateSchemaIndexes caught error ' + error);
        throw error;
      });
  }

  // Find and delete Schema Fields
  async deleteSchemaFields(className: string, fieldNames: Array<string>) {
    var promises = Array();
    // Rewriting like createIndexes, Collection method will just delete a field
    logger.verbose(
      'DeleteSchema Fields ' + JSON.stringify(fieldNames) + ' for Schema ' + className
    );
    for (let idx = 0; idx < fieldNames.length; idx++) {
      const fieldName = fieldNames[idx];
      logger.verbose('about to delete field ' + fieldName);
      const promise = this._collection
        .deleteSchemaField(_oracleSchemaQueryFromNameQuery(className), fieldName)
        .then(promise => {
          if (promise === 'retry') {
            return this._collection.deleteSchemaField(
              _oracleSchemaQueryFromNameQuery(className),
              fieldName
            );
          }
          return promise;
        })
        .catch(error => {
          logger.error('SchemaCollection deleteSchemaFields caught error ' + error.message);
          throw error;
        });
      promises.push(promise);
    }

    const results = await Promise.all(promises);
    logger.verbose('DeleteSchemaFields returns ' + results);
    return results;
  }

  // Find a delete Schema Document
  findAndDeleteSchema(name: string) {
    return this._collection.findOneAndDelete(_oracleSchemaQueryFromNameQuery(name));
  }
  // Add a field to the schema. If database does not support the field
  // type (e.g. mongo doesn't support more than one GeoPoint in a class) reject with an "Incorrect Type"
  // Parse error with a desciptive message. If the field already exists, this function must
  // not modify the schema, and must reject with DUPLICATE_VALUE error.
  // If this is called for a class that doesn't exist, this function must create that class.

  // TODO: throw an error if an unsupported field type is passed. Deciding whether a type is supported
  // should be the job of the adapter. Some adapters may not support GeoPoint at all. Others may
  // Support additional types that Mongo doesn't, like Money, or something.

  // TODO: don't spend an extra query on finding the schema if the type we are trying to add isn't a GeoPoint.
  addFieldIfNotExists(className: string, fieldName: string, fieldType: string) {
    logger.verbose('entered addFieldIfNotExists');
    return this._fetchOneSchemaFrom_SCHEMA(className)
      .then(
        schema => {
          // If a field with this name already exists, it will be handled elsewhere.
          if (schema.fields[fieldName] !== undefined) {
            return;
          }
          // The schema exists. Check for existing GeoPoints.
          if (fieldType.type === 'GeoPoint') {
            // Make sure there are not other geopoint fields
            if (
              Object.keys(schema.fields).some(
                existingField => schema.fields[existingField].type === 'GeoPoint'
              )
            ) {
              throw new Parse.Error(
                Parse.Error.INCORRECT_TYPE,
                'Parse only supports one GeoPoint field in a class.'
              );
            }
          }
          return;
        },
        error => {
          logger.error('SchemaCollection addFieldIfNotExists throws ' + error);
          // If error is undefined, the schema doesn't exist, and we can create the schema with the field.
          // If some other error, reject with it.
          if (error === undefined) {
            return;
          }
          throw error;
        }
      )
      .then(() => {
        // We use $exists and $set to avoid overwriting the field type if it
        // already exists. (it could have added inbetween the last query and the update)
        //if (fieldOptions && Object.keys(fieldOptions).length > 0) {

        const { type, targetClass } = fieldType,
          fieldOptions = _objectWithoutProperties(fieldType, ['type', 'targetClass']);

        if (fieldOptions && Object.keys(fieldOptions).length > 0) {
          return this.upsertSchema(
            className,
            {
              [fieldName]: {
                $exists: false,
              },
            },
            {
              [fieldName]: parseFieldTypeToOracleFieldType({
                type,
                targetClass,
              }),
              _metadata: {
                fields_options: { [`${fieldName}`]: fieldOptions },
              },
            }
          );
        } else {
          return this.upsertSchema(
            className,
            {
              [fieldName]: {
                $exists: false,
              },
            },
            {
              [fieldName]: parseFieldTypeToOracleFieldType({
                type,
                targetClass,
              }),
            }
          );
        }
      });
  }

  async updateFieldOptions(className: string, fieldName: string, fieldType: any) {
    const { ...fieldOptions } = fieldType;
    delete fieldOptions.type;
    delete fieldOptions.targetClass;

    await this.upsertSchema(
      className,
      { [fieldName]: { $exists: true } },
      {
        _metadata: {
          fields_options: { [`${fieldName}`]: fieldOptions },
        },
      }
    );
  }
}

OracleSchemaCollection.parseFieldTypeToOracleFieldType = parseFieldTypeToOracleFieldType;

export default OracleSchemaCollection;
