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

    /**
     * 
     * @param props the property object
     * @param props.url the url of the data to perform this reduce upon.
     * @param props.reducer Function called on each row
     * @param parserOptions - see https://c2fo.github.io/fast-csv/docs/parsing/options for available options such as skipping headers, or objectmode
     */
    reduce( props:I.extReduceProps, parserOptions={} ):Promise<any> {

        return new Promise( async (resolve, reject) => {
    
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

                    })
                    .on('error', err => reject)
                    .on('end', () => resolve(accumulator))
                ,
                err => reject
            );

            return accumulator;
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
     * @param parserOptions 
     */
    mapSlices( props, parserOptions={} ) {

        return new Promise( async (resolve, reject) => {

            const {
                url,
                mapper
            } = props;

            let i=0,
                size,
                slice=[],
                promises=[];

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
                            }
                            await Promise.all(promises);
                            resolve(true);
                        })
                        .on('error', err => reject)
                )
            }
            catch( err ) {
                return reject(err);
            }

            
        });
    }

    /**
     * Get the number of rows in this datafile
     * @param props 
     */
    async count( props, parserOptions={} ):Promise<number> {

        return new Promise( async (resolve, reject) => {

            const { url } = props;

            let stream,
                i=0;

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
                console.log('>>>')
            }
        });
    }

    /**
     * Temporary solution for a project i'm on that uses azure storage but will migrate to datalake tables, so we'll replace this very shortly.
     * Due to the lack of AAD support with azure storage tables this is going to be a bit ugly so the faster we move to datalake tables the better.
     * 
     * @param props the argument object
     * @param props.url string the url of the datalake file
     * @param props.table string the target tablename
     * @param props.partitionKey string the field to use for a partiton key
     * @param props.rowKey string the field to use for the row key
     * 
     * @todo allow paritionKey and rowKey to be argued as a function.  
     */
    cache( props, parserOptions={} ):Promise<I.extCacheReturn> {

        return new Promise( async (resolve, reject) => {

            const {
                url,
                table,
                delimiter = ',',
                partitionKey,
                rowKey

            } = props;

            const {
                STORAGE_ACCOUNT,
                STORAGE_ACCOUNT_KEY
            } = process.env;

            if(typeof STORAGE_ACCOUNT !== "string" || !STORAGE_ACCOUNT.length)
                throw `simple_datalake_client::cache failed - missing environment variable STORAGE_ACCOUNT`;

            if(typeof STORAGE_ACCOUNT_KEY !== "string" || !STORAGE_ACCOUNT_KEY.length)
                throw `simple_datalake_client::cache failed - missing environment variable STORAGE_ACCOUNT_KEY`;

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
                switch(err.statusCode) {
                    case 409: //table already exists
                        try {
                            await serviceClient.deleteTable(table);
                            //azure wont allow you to create a table you've recently deleted for about 20 seconds.
                            //attempting to do so produces an error indicating it is in the process of deleting.
                            await new Promise(r => setTimeout(e => r(true), 45000));
                            await serviceClient.createTable(table);
                        } catch( err ){
                            return reject(`SimpleDatalakeClient:ext::cache has failed replacing table ${table} - ${err.message}`);
                        }

                        break;
                    default:
                        return reject(`SimpleDatalakeClient:ext::cache failed to build target table ${table} - ${err.message}`);
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

                            result = await transactClient.createEntity(row)
                            numRowsInserted++;
                        } catch( err ) {

                            await serviceClient.deleteTable(table);
                            return reject(`Purged table ${table} - data extraction failed - ${err.message}`);
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

        });

    }
}