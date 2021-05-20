import { AzureDatalakeClient } from './client';
import { parse } from '@fast-csv/parse';
import * as I from './types';
import { pipeline } from 'stream';
import * as zlib from 'zlib';

import {
    TableServiceClient,
    TablesSharedKeyCredential,
    TableClient
} from '@azure/data-tables';
import { StorageSharedKeyCredential } from '@azure/storage-file-datalake';

export class AzureDatalakeExt {

    client:AzureDatalakeClient = null;

    constructor( props ) {

        const { client } = props;
        this.client = client;
    }

    async get( props, parserOptions:I.ExtendedParserOptionsArgs={} ):Promise<Array<any>> {

        const { url } = props;
        return this.mapSlices({ url, mapper: data => data}, parserOptions)
    }

    /**
     * 
     * @param props the property object
     * @param props.url the url of the data to perform this reduce upon.
     * @param props.reducer Function called on each row
     * @param parserOptions - see https://c2fo.github.io/fast-csv/docs/parsing/options for available options such as skipping headers, or objectmode
     */
    reduce( props:I.extReduceProps, parserOptions={} ):Promise<any> {

        return new Promise( async (resolve, reject) => {
    
            try {

                const {
                    url,
                    reducer
                } = props;

                let { accumulator } = props,
                    i=0,
                    keys;

                parserOptions['key_values'] = parserOptions['key_values'] === undefined
                    ? true : parserOptions['key_values'];

                let stream;
                try {
                    stream = await this.client.readableStream({url});
                    if(url.substr(-2) === 'gz')
                        stream = stream.pipe(zlib.createGunzip())

                } catch( err ){
                    return reject(err);
                }

                pipeline(
                    stream,
                    parse(parserOptions)
                        .on('data', data => {

                        try {

                            if(parserOptions['key_values']) {
                                if(i === 0 && !keys) {
                                    keys = data;
                                    return;    
                                }

                                const keyed_data = keys.reduce((acc, key, i) => {
                                    acc[key] = data[i];
                                    return acc;
                                }, {})

                                accumulator = reducer(accumulator, keyed_data, i);

                            }
                            else {

                                accumulator = reducer(accumulator, data, i);
                            }
                        } catch( err ) {
                            
                            throw err;
                        }

                        })
                        .on('error', err => {
                            return reject(err)
                        })
                        .on('end', () => resolve(accumulator))
                    ,
                    err => reject
                );

                return accumulator;

            }
            catch( err ) {

                return reject(Error(`SimpleDatalakeClient::ext.reduce has failed - ${err.message}`))
            }
        });

    }

    /**
     * 
     * @param props the property object
     * @param props.url the url of the CSV file we'll be mapping over.
     * @param props.eachRow Function called on each row
     * @param parserOptions - see https://c2fo.github.io/fast-csv/docs/parsing/options for available options such as skipping headers, or objectmode
     */
    async map( props, parserOptions={} ):Promise<Array<any>> {

        return new Promise( async (resolve, reject) => {

            try {
                const { url } = props;
                let { mapper } = props,
                    i=0,
                    promises=[],
                    keys;

                parserOptions['key_values'] = parserOptions['key_values'] === undefined
                    ? true : parserOptions['key_values'];

                let stream;
                try {
                    stream = await this.client.readableStream({url});
                    if(url.substr(-2) === 'gz')
                        stream = stream.pipe(zlib.createGunzip())
                }
                catch( err ){
                    return reject(err);
                }

                try {

                    pipeline(
                        stream,
                        parse(parserOptions)
                            .on('data', data => {

                                if(parserOptions['key_values']) {
                                    if(i === 0 && !keys) {
                                        keys = data;
                                        return;    
                                    }
        
                                    const keyed_data = keys.reduce((acc, key, i) => {
                                        acc[key] = data[i];
                                        return acc;
                                    }, {})
        
                                    promises.push(mapper(keyed_data, i));
        
                                }
                                else {

                                    promises.push(mapper(data, i));
                                    i++;
                                }
                            })
                            .on('error', reject)
                            .on('end', async () => {
                                const result = await Promise.all(promises)
                                resolve(result);
                            })
                        ,
                        err => reject
                    );
                }
                catch( err ) {
                    return reject(err);
                }
            }
            catch( err ) {

                reject(Error(`SimpleDatalakeClient::ext.map has failed ${err.message}`));
            }
        });
    }

    /**
     * Iterate a CSV datafile, optionally blocking I/O (default true)
     * 
     * @param props the argument object
     * @param props.url String - the url of the file
     * @param props.fn Function - the function to call for each row
     * @param props.block Boolean - wait until all rows have finished, default true
     * @param parserOptions -see https://c2fo.github.io/fast-csv/docs/parsing/options for available options such as skipping headers, or objectmode
     */
    forEach( props, parserOptions={} ):Promise<void> {

        return new Promise( async (resolve, reject) => {

            const {
                url,
                fn
            } = props;

            let { block } = props,
                i=0,
                promises=[],
                keys;

            parserOptions['key_values'] = parserOptions['key_values'] === undefined
                ? true : parserOptions['key_values'];

            block = block === undefined ? true : block;

            const finalize = () => resolve();

            let stream;
            try {
                stream = await this.client.readableStream({url});
                if(url.substr(-2) === 'gz')
                    stream = stream.pipe(zlib.createGunzip())
            } catch( err ){
                return reject(err);
            }

            try {
            
                pipeline(
                    stream,
                    parse(parserOptions)
                        .on('data', data => {

                            if(parserOptions['key_values']) {
                                if(i === 0 && !keys) {
                                    keys = data;
                                    return;    
                                }
    
                                const keyed_data = keys.reduce((acc, key, i) => {
                                    acc[key] = data[i];
                                    return acc;
                                }, {})
    
                                promises.push(fn(keyed_data, i));
    
                            }
                            else {

                                promises.push(fn(data, i));
                                i++;
                            }
                        })
                        .on('error', reject)
                        .on('end', async () => {
                            if(block) {
                                await Promise.all(promises);
                                finalize();
                            }
                        })
                    ,
                    err => reject
                )                
            }
            catch( err ) {
                return reject(err);
            }
            
            if(!block)
                finalize();
                
        });
    }

    /**
     * Map over data in slices. Useful for batch operations such as insertion.
     * 
     * @param props the argument object
     * @param props.mapper the mapping function
     * @param props.size the size of each slice, default: 1000; 
     * @param parserOptions - see https://c2fo.github.io/fast-csv/docs/parsing/options for available options.
     */
    mapSlices( props, parserOptions:I.ExtendedParserOptionsArgs={} ):Promise<Array<any>> {

        return new Promise( async (resolve, reject) => {

            const {
                url,
                mapper
            } = props;

            let i=0,
                size,
                slice=[],
                promises=[],
                keys,
                result = [];

            size = props.size || 1000;

            parserOptions['key_values'] = parserOptions['key_values'] === undefined
                ? true : parserOptions['key_values'];

            let stream;
            try {
                stream = await this.client.readableStream({url});
                if(url.substr(-2) === 'gz')
                    stream = stream.pipe(zlib.createGunzip())

            } catch( err ){
                return reject(err);
            }

            try {

                pipeline(
                    stream,
                    parse(parserOptions)
                        .on('data', data => {

                            if(parserOptions['key_values']) {
                                if(i === 0 && !keys) {
                                    keys = data;
                                    return;    
                                }
    
                                const keyed_data = keys.reduce((acc, key, i) => {
                                    if(parserOptions['nullifyEmptyColumns'] && typeof data[i] === 'string' && !data[i].length)
                                        data[i] = null;
                                    acc[key] = data[i];
                                    return acc;
                                }, {})
                                data = keyed_data;
                            }
                            else {
                                data = data.map(value => {
                                    if(parserOptions['nullifyEmptyColumns'] && typeof value === 'string' && !value.length)
                                        value = null;
                                    return value;
                                })
                            }
                            slice.push(data);
                            if(slice.length !== size)
                                return;
                            
                            const ret = mapper(slice);
                            if(ret instanceof Promise)
                                promises.push(ret);
                            
                            slice = [];
                        })
                        .on('end', async () => {

                            if(slice.length) {
                                const ret = mapper(slice);
                                if(ret instanceof Promise)
                                    promises.push(ret);
                                result = result.concat(ret);
                            }
                            await Promise.all(promises);
                            resolve(result);
                        })
                        .on('error', err => reject),
                    err => reject
                )
            }
            catch( err ) {
                return reject(err);
            }

            
        });
    }

    /**
     * Get the number of rows in this datafile
     * 
     * Remember, this will count the headers unless the parserOptions has headers set to false.
     * 
     * @param props
     * @param parserOptions @see https://c2fo.github.io/fast-csv/docs/parsing/options/
     */
    async count( props, parserOptions:any={} ):Promise<number> {

        return new Promise( async (resolve, reject) => {

            try {

                const {
                    url
                } = props;

                let stream,
                    i=0,
                    headersIgnored=false;

                stream = await this.client.readableStream({url});
                if(url.substr(-2) === 'gz')
                    stream = stream.pipe(zlib.createGunzip())           

                pipeline(
                    stream,
                    parse(parserOptions)
                        .on('data', data => {
                            // if(parserOptions.headers && !headersIgnored) {
                            //     headersIgnored = true;
                            //     return;
                            // }
                            i++
                        })
                        .on('error', err => {
                            reject(err)
                        })
                        .on('end', () => {
                            resolve(i)
                        })
                        , err => {
                            if(err)
                                return reject(err)
                        }
                );

            }
            catch( err ) {

                reject(Error(`SimpleDatalakeClient::ext.count has failed ${err.message}`));
            }

        });
    }

    /**
     * Cache a CSV file to a azure storage table.
     * 
     * Temporary solution for a project i'm on that uses azure storage but will migrate to datalake tables, so we'll replace this very shortly.
     * Due to the lack of AAD support with azure storage tables this is going to be a bit ugly so the faster we move to datalake tables the better.
     * 
     * @param props the argument object
     * @param props.url string the url of the datalake file
     * @param props.table string the target tablename
     * @param props.partitionKey string the field to use for a partiton key
     * @param props.rowKey string the field to use for the row key
     * @param props.replaceIfExists boolean replace the table if one exists (this suffers waiting around in the azure queue), default false
     * @param props.types object a key value object where the key is the property name and the value is the Odata Edm type - eg { field_one: "double", field_two: "Int32" }
     * @param parserOptions
     * @todo allow paritionKey and rowKey to be argued as a function.  
     */
    cache( props, parserOptions={} ):Promise<I.extCacheReturn> {

        return new Promise( async (resolve, reject) => {

            try {

                const {
                    url,
                    table,
                    delimiter = ',',
                    partitionKey,
                    rowKey,
                    types
                } = props;

                let {
                    replaceIfExists
                } = props;

                replaceIfExists = replaceIfExists === undefined ? false : replaceIfExists;

                const {
                    STORAGE_ACCOUNT,
                    STORAGE_ACCOUNT_KEY
                } = process.env;

                if(!url)
                    throw Error('argue a url.');

                if(typeof STORAGE_ACCOUNT !== "string" || !STORAGE_ACCOUNT.length)
                    throw Error(`simple_datalake_client::cache failed - missing environment variable STORAGE_ACCOUNT`);

                if(typeof STORAGE_ACCOUNT_KEY !== "string" || !STORAGE_ACCOUNT_KEY.length)
                    throw Error(`simple_datalake_client::cache failed - missing environment variable STORAGE_ACCOUNT_KEY`);

                const credential = new TablesSharedKeyCredential(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY);
                const serviceClient = new TableServiceClient(`https://${STORAGE_ACCOUNT}.table.core.windows.net`, credential);
                const transactClient = new TableClient(
                    `https://${STORAGE_ACCOUNT}.table.core.windows.net`,
                    table,
                    credential
                )

                try {

                    await serviceClient.createTable(table);

                } catch( err ) {

                    if(err.message.includes('TableBeingDeleted')) {
                        console.warn(`table ${table} is queued for deletion by azure, retrying momentarily...`)
                        await new Promise(r => setTimeout(() => r(true), 2000));
                        let result;
                        try {
                            result = await this.cache(props, parserOptions)
                        }
                        catch( err ) {
                            return reject(err);
                        }
                        return resolve(result);
                    }
                    else if(err.message.includes('TableAlreadyExists') && replaceIfExists) {
                        console.warn(`table ${table} exists - dropping by request.`);
                        await serviceClient.deleteTable(table);
                        let result;
                        try {
                            result = await this.cache(props, parserOptions)
                        }
                        catch( err ) {
                            return reject(err);
                        }
                        return resolve(result);
                    }
                    else {
                        return reject(Error(`SimpleDatalakeClient:ext::cache failed to build target table ${table} using credentials [${STORAGE_ACCOUNT}][${STORAGE_ACCOUNT_KEY}] ${err.message}`));
                    }
                }

                let numRowsInserted = 0;

                try {
                    await this.map({
                        url,
                        mapper: async (row, i) => {
                            let result;
                            try {
                                row.PartitionKey = typeof partitionKey === 'function'
                                    ? partitionKey(row)
                                    : row[partitionKey];
                                row.RowKey = typeof rowKey === 'function'
                                    ? rowKey(row)
                                    :row[rowKey];
                                if(types && Object.keys(types).length)
                                    row = _castKeywordObject(row, types);
                                result = await transactClient.createEntity(row)
                                numRowsInserted++;
                            } catch( err ) {
                                await serviceClient.deleteTable(table);
                                return reject(Error(`Purged table ${table} - data extraction failed - ${err.message}`));
                            }
                        }
                    }, parserOptions);
                }
                catch( err ) {
                    reject(err);
                }

                return resolve({
                    numRowsInserted
                });

            }
            catch( err ) {

                reject(Error(`SimpleDatalakeClient::ext.cache has failed ${err.message}`));
            }

        })

    }
}

/**
 * Allows recasting of a keyword object's values. Useful being our parser will always return strings for its values.
 * 
 * of note: cannot think of a use case for the BINARY Edm Type so it is excluded at this point.
 * 
 * @param obj Object a keywork object
 * @param definitions Object a keyword object where the key is a property in the obj param and the value is a valid odata Edm type specified as a string - eg "DOUBLE" valid types are "Boolean" | "DateTime" | "Double" | "Guid" | "Int32" | "Int64" | "String"
 * 
 * @returns Object with values recast as specified by the defintions. 
 */
function _castKeywordObject( obj, definitions ) {

    return Object.keys(obj).reduce((acc, key) => {

        if(!definitions.hasOwnProperty(key)) {
            acc[key] = obj[key];
            return acc;
        }

        if(!obj.hasOwnProperty(key)) {
            console.warn(`SimpleDatalakClient::_castKeywordObject - invalid key ${key} does not exist on the argued object - skipping...`);
            return acc;
        }

        let value;

        switch(definitions[key].toLowerCase()) {

            case 'number':
                //let azure tables guess in this case.
                acc[key] = parseFloat(obj[key].toString()).toString();
                break;

            case 'double':
            case 'float':
                value = parseFloat(obj[key].toString())
                //for azure tables not including the key at all is assigning a null when reading rows back and is done because the datatables
                //library can't handle the fact null is an object in javascript.
                if(isNaN(value))
                    break
                acc[key] = {
                    type: "Double",
                    value: value.toString()
                }
                break;

            case 'integer':
            case 'int':
            case 'int32':
                value = parseInt(obj[key].toString())
                //for azure tables not including the key at all is assigning a null when reading rows back and is done because the datatables
                //library can't handle the fact null is an object in javascript.
                if(isNaN(value))
                    break;
                acc[key] = {
                    type: "Int32",
                    value: value.toString()
                }
                break;

            case 'bigint':
                try {
                    acc[key] = {
                        type: "Int64",
                        value: BigInt(obj[key].toString()).toString()
                    }
                }
                catch( err ) {
                    if(err.message.includes("Cannot convert") && obj[key].toString().includes('.')) {
                        
                        console.warn(`impleDatalakClient::_castKeywordObject received a value [${obj[key]}] which cannot be cast to a BigInt, resolving by clipping the decimal digits`);
                        const rational = obj[key].split('.')[0];
                        acc[key] = {
                            type: "Int64",
                            value: BigInt(rational).toString()
                        }
                    }
                } 
                break;

            case 'string':
                acc[key] = {
                    type: "String",
                    value: obj[key].toString()
                }
                break;

            case 'boolean':
                value = obj[key] !== '0' && obj[key] !== 'false'
                acc[key] = {
                    type: "Boolean",
                    value: value.toString()
                }
                break;

            default:
                throw Error(`SimpleDatalakClient::_castKeywordObject key "${key}" has invalid type "${definitions[key]}"`);
        }

        return acc;
    }, {});
}