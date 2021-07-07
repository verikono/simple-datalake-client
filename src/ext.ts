import { AzureDatalakeClient } from './client';
import { parse } from '@fast-csv/parse';
import * as I from './types';
import { pipeline } from 'stream';
import * as zlib from 'zlib';
import { EOL } from 'os';

import {
    AzureDataTablesClient
} from 'verikono-azure-datatable-tools';

import {
    TableServiceClient,
    AzureNamedKeyCredential,
    TableClient
} from '@azure/data-tables';

import {
    CSVStreamToKeywordObjects,
    applyMutations,
    keywordArrayToCSV,
    eachRow,
    inspect,
    transformKeywordObjects,
    addColumns
} from './transforms';

import {
    fromAzureDatalake,
    toAzureDatalake
} from './loaders';

import {
    nullTerminator,
    toAzureDataTables
} from './terminators';

export class AzureDatalakeExt {

    client:AzureDatalakeClient = null;

    constructor( props ) {

        const { client } = props;
        this.client = client;
    }

    /**
     * Download and parse a CSV to memory
     * 
     * @param props Object the argument keyword object
     * @param props.url String the url
     * @param parserOptions ExtendedParserOptionsArgs parser options.
     * 
     * @returns Promise with a parsed CSV (ie an array of the rows) 
     */
    async get( props, parserOptions:I.ExtendedParserOptionsArgs={} ):Promise<Array<any>> {

        const { url } = props;
        return this.map({ url, mapper: data => data}, parserOptions)
    }


    async find():Promise<any> {

    }


    /**
     * Perform a reduce operation upon a stored CSV file in the datalake, optionally storing and overwriting the result.
     * 
     * @param props the property object
     * @param props.url the url of the data to perform this reduce upon.
     * @param props.reducer Function called on each row
     * @param props.persist Boolean persist the result back upon the file. Default false.
     * @param parserOptions - see https://c2fo.github.io/fast-csv/docs/parsing/options for available options such as skipping headers, or objectmode
     */
    reduce( props:I.extReduceProps, parserOptions:I.ExtendedParserOptionsArgs={} ):Promise<any> {

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
     * Map through a stored CSV file in the datalake, optionally storing and overwriting the result.
     * 
     * @param props the property object
     * @param props.url the url of the CSV file we'll be mapping over.
     * @param props.mapper Function called on each row
     * @param props.persist Boolean persist the result back upon the file. Default: false.
     * @param parserOptions - see https://c2fo.github.io/fast-csv/docs/parsing/options for available options such as skipping headers, or objectmode
     */
    async map( props:I.extMapProps, parserOptions:I.ExtendedParserOptionsArgs={} ):Promise<any[]> {

        return new Promise( async (resolve, reject) => {

            try {

                const { url } = props;
                let {
                    mapper,
                    persist
                } = props;

                const parserOptions = props['parserOptions'] || {};
                parserOptions.header = parserOptions.hasOwnProperty('key_values')
                    ? parserOptions.key_values
                    : true;

                const promises = [];

                const zipped = url.substr(-2) === 'gz';

                try {

                    pipeline(
                        await fromAzureDatalake({url}),
                        eachRow({
                            parserOptions,
                            onRow: (data, i) => {
                                const result = mapper(data,i);
                                promises.push(result);
                            },
                            onError: reject,
                            onEnd: async meta => {
                             
                                let result;
                                try {
                                //await all promises in the mapped stack
                                    result = await Promise.all(promises);
                                }
                                catch( err ) {
                                    if(!err.message.includes(`Cannot read property 'ERROR' of undefined`))
                                        throw err;
                                }
                                const { delimiter } = meta;

                                //if we're saving the result back to the datalake overwriting what WAS there.
                                if(persist) {

                                    //CSVify the result
                                    let headings,
                                        rows,
                                        content;

                                    //if we mapped over key/value objects, CSVify based of an array of keyvalue objects.
                                    if(parserOptions['header']) {

                                        try {
                                            headings = Object.keys(result[0]).join(delimiter);
                                            rows = result.map(data => Object.values(data).join(delimiter));
                                            content = [].concat(headings, rows).join(EOL);
                                        }
                                        catch( err ) {
                                            throw Error('Failed to construct the result to CSV.');
                                        }
                                    }
                                    //if we mapped over raw rows csvify that way.
                                    else {

                                        content = result.map(data => data.join(delimiter)).join(EOL);
                                    }
                                
                                    //push the CSV back up to the datalake
                                    try {

                                        //if the URL is a zip file, zip up the contents.
                                        if(zipped)
                                            throw Error(`Unimplemented - zip up a mapped result`);

                                        const client = this.client.getFileClient({url});
                                        await client.create();
                                        await client.append(content, 0, content.length);
                                        await client.flush(content.length);
                                    }
                                    catch( err ) {
                                        throw Error(`Failed uploading result to datalake - ${err.message}`)
                                    }
                                }

                                resolve(result);
                                
                            }
                        }),
                        nullTerminator(),
                        err => {
                            if(err)
                                reject(err) 
                        }
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
        
            try {

                const {
                    url,
                    fn
                } = props;

                let {
                    block
                } = props;

                parserOptions['header'] = parserOptions['header'] === undefined ? true : parserOptions['header'];
                block = block === undefined ? true : block;

                if(parserOptions['key_values'] !== undefined) {
                    console.warn('SimpleDatalakeClient::ext.forEach is depreacting the key_values option - use option header instead')
                    parserOptions['header'] = parserOptions['key_values'];
                    delete parserOptions['key_values'];
                }

                let promises = [];

                pipeline(
                    await fromAzureDatalake({url}),
                    eachRow({
                        parserOptions,
                        onRow: (data, i) => {
                            const result = fn(data,i);
                            promises.push(result);
                        },
                        onError: reject,
                        onEnd: async meta => {

                            if(block) {
                                
                                try {
                                    await Promise.all(promises);
                                }
                                catch( err ) {
                                    if(!err.message.includes(`Cannot read property 'ERROR' of undefined`))
                                        throw err;
                                }
                                resolve();
                            } else {
                                resolve();
                            }
                        }

                    }),
                    nullTerminator(),
                    err => {
                        if(err)
                            reject(err) 
                    }
                )
            }
            catch( err ) {

                reject(Error(`SimpleDatalakeClient::ext.forEach has failed ${err.message}`));
            }
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
     * @param props.includeHeadings boolean, include the headings column in the count, default false.
     * @param parserOptions @see https://c2fo.github.io/fast-csv/docs/parsing/options/
     * 
     */
    async count( props, parserOptions:any={} ):Promise<number> {

        return new Promise( async (resolve, reject) => {

            try {

                const {
                    url,
                    includeHeadings
                } = props;

                let i = 0,
                    processedHeadings = false;

                pipeline(
                    await fromAzureDatalake({url}),
                    parse(parserOptions)
                        .on('data', data => {
                            if( i===0 && !includeHeadings && !processedHeadings) {
                                processedHeadings = true;
                                return;
                            }
                            i++;
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
     * Iterate through a list of URLs where each is expected to be;
     *  - a CSV (gzipped or otherwise)
     *  - the same data with differences (eg. a list of friends for each month)
     * 
     * Each file will be overloaded upon the previous, applying changes and producing a diff.
     * 
     * @param props 
     * @param props.urls string[] - a list of urls which will be loaded in order
     * @param props.pk string|string[]|Function 
     * @param parserOptions 
     * 
     * @returns Object {data, diff} the data as an array of key/value objects, a diff structure.
     */
    compile( props:I.extCompileProps, parserOptions:I.ExtendedParserOptionsArgs={} ):Promise<any> {

        try {

            return new Promise( async (resolve, reject) => {

                const {
                    urls,
                    pk
                } = props;

                let {
                    delimiter
                } = parserOptions;

                delimiter = delimiter || ',';
                parserOptions['key_values'] = true;

                let columnNames = null;
                let rowNum = 0;
                let result = [];
                const diff = {};

                for(let fileItr=0; fileItr<urls.length; fileItr++) {

                    const url = urls[fileItr];
                    const zipped = url.substr(-2) === 'gz';
                    
                    let stream;
                    try {
                        stream = await this.client.readableStream({url});
                        if(zipped)
                            stream = stream.pipe(zlib.createGunzip())
                    }
                    catch( err ){

                        return reject(new Error(`AzureDatalakeClient::compiled failed to load data from ${url} - ${err.message}`));
                    }

                    //helper function which converts a row of values to a key/value object based from the
                    //column names accumulated in the first row.
                    const toKeyValueObject = data =>
                        columnNames.reduce((acc, col, i) => Object.assign(acc, {[col]:data[i]}), {});

                    const rowIsIdentical = (row1, row2) => 
                        JSON.stringify(row1) === JSON.stringify(row2);

                    await new Promise((res, rej) => {

                        try {

                            let deleteRows = [];
                            const newRows = [];

                            pipeline(
                                stream,
                                parse(parserOptions)
                                    .on('data', data => {

                                        //first row of the first file is assumed to contain the column names.
                                        if(rowNum === 0 && fileItr === 0) {
                                            columnNames = data;
                                        }
                                        //first row of 2nd+ file - assumed to be the column namesdat
                                        else if(rowNum === 0 && fileItr !== 0) {
                                            //ensure the keys are the same for each file, ensure this new file has the same columns
                                            if(data.length !== columnNames.length || !data.every(col => columnNames.includes(col))) {
                                                data.forEach(dataColumn => {
                                                    const isNewColumn = !columnNames.includes(dataColumn)

                                                })

                                                console.log('---');
                                                return reject(new Error(`Compile can only work for files with the same columns`));
                                            }
                                                
                                            //set up the dleteRows which we'll each one off as we iterate through them, leaving any
                                            //left which the 'end' event is fired to be scheduled for deletion.
                                            deleteRows = result.map(row => row._pk);
                                        }
                                        //2nd+ row from first file.
                                        else if(rowNum !== 0 && fileItr == 0) {

                                            //create a map of the data, and attach a PK.
                                            const keyed_data = toKeyValueObject(data); 
                                            keyed_data._pk = this.derivePk(pk, keyed_data);

                                            result.push(keyed_data);
                                        }
                                        //2nd+ row from 2nd+ file, here we merge.
                                        else if(rowNum !== 0 && fileItr !== 0) {
                                        
                                            //create a map of the data, and attach a PK.
                                            const keyed_data = toKeyValueObject(data); 
                                            keyed_data._pk = this.derivePk(pk, keyed_data);

                                            //find the row in the result.
                                            let resultIdx = result.findIndex(result => result._pk === keyed_data._pk);
                                            
                                            //if its a new row, then add it as a "new" to the rollup (performed when an end event is fired, see .on('end')).
                                            if(resultIdx === -1) {
                                                newRows.push(keyed_data);
                                                return;
                                            }

                                            deleteRows = deleteRows.filter(pk => pk !== keyed_data._pk);
                                            const noChangesInRow = rowIsIdentical(result[resultIdx], keyed_data);
                                            if(!noChangesInRow) {

                                                columnNames.reduce((acc, key) => {

                                                    try {

                                                        //if(newRow) return acc;
                                                        if(result[resultIdx][key].toString() === keyed_data[key].toString()) return acc;
                                                        if(!diff.hasOwnProperty(keyed_data._pk))
                                                            diff[keyed_data._pk] = {};
                                                        if(!diff[keyed_data._pk].hasOwnProperty(key))
                                                            diff[keyed_data._pk][key] = [];

                                                        diff[keyed_data._pk][key].push({
                                                            type: 'variation',
                                                            value: keyed_data[key],
                                                            value_from: result[resultIdx][key],
                                                            url: urls[fileItr]
                                                        });

                                                        result[resultIdx][key] = keyed_data[key];

                                                        return acc;

                                                    } catch( err ) {

                                                        throw new Error(`a problem was encountered whilst interpretting a change upon column:${key} on row ${rowNum} of ${urls[fileItr]}} - ${err.message}`);
                                                    }

                                                }, []);

                                            }

                                        }

                                        rowNum++;
                                    })
                                    .on('error', err => {
                                        return rej(err)
                                    })
                                    .on('end', async () => {
                                        rowNum = 0;

                                        deleteRows.forEach(pk => {
                                            const resultRow = result.find(res => res._pk === pk);
                                            if(!diff.hasOwnProperty(pk))
                                                diff[pk] = {};
                                            diff[pk] = Object.keys(resultRow).reduce((acc, key) => {
                                                acc[key] = {
                                                    type: 'delete',
                                                    value: null,
                                                    value_from: resultRow[key],
                                                    url: urls[fileItr]
                                                }
                                                return acc;
                                            }, {});
                                            result = result.filter(res => res._pk !== pk)
                                        })

                                        newRows.forEach(keyed_data => {
                                            diff[keyed_data._pk] = columnNames.reduce((acc,key) => {
                                                acc[key] = {
                                                    type: 'new',
                                                    value: keyed_data[key],
                                                    value_from: null,
                                                    url: urls[fileItr]
                                                }
                                                return acc;
                                            }, {});
                                            result.push(keyed_data);
                                        })
                                        return res(true);

                                    })
                                , err => rej
                            );

                        }
                        catch( err ) {
                            return rej(err);
                        }

                    })
                    .catch(err => reject(new Error(`AzureDatalake.ext has failed - ${err.message}`)));
                
                }

                resolve({ data: result, diff });
            });

        }
        catch( err ) {

            throw new Error(`SimpleDatalakeClient::ext.compiled has failed -  ${err.message}`)
        }
    }
z
    etl( props:I.extEtlProps, parserOptions:I.ExtendedParserOptionsArgs={}  ):Promise<any> {

        return new Promise(async (resolve, reject) => {

            try {

                const {
                    url,
                    transform,
                    target,
                    postFn
                } = props;

                let {
                    overwrite,
                    append
                } = props;

                overwrite = overwrite === undefined ? false : overwrite;
                append = append === undefined ? false : append;

                const parserOptions = props['parserOptions'] || {};
                parserOptions.header = parserOptions.hasOwnProperty('key_values')
                    ? parserOptions.key_values
                    : true;

                const promises = [];

                const zipped = url.substr(-2) === 'gz';

                try {

                    pipeline(
                        await fromAzureDatalake({url}),
                        CSVStreamToKeywordObjects(),
                        transformKeywordObjects({transformer: transform, initial: [], postFn}),
                        toAzureDataTables({table: target, overwrite, append}),
                        err => err ? reject(err): resolve(true)
                    );
                }
                catch( err ) {
                    return reject(err);
                }

            }
            catch( err ) {

                reject(new Error(`SimpleDatalakeClient::ext.etl has failed - ${err.message}`));
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
    cache( props:I.extCacheProps, parserOptions:I.ExtendedParserOptionsArgs={} ):Promise<I.extCacheReturn> {

        return new Promise(async (resolve, reject) => {

            try {

                const {
                    url,
                    table,
                    partitionKey,
                    rowKey,
                    types,
                    replaceIfExists
                } = props;

                let {
                    overwrite,
                    append
                } = props;

                overwrite = overwrite === undefined ? false : overwrite;
                append = append === undefined ? false : append;

                if(replaceIfExists) {
                    console.warn('deprecating ext.cache argument `replaceIfExists` - use `overwrite` instead');
                    overwrite = true;
                }

                const parserOptions = props['parserOptions'] || {};
                parserOptions.header = parserOptions.hasOwnProperty('key_values')
                    ? parserOptions.key_values
                    : true;

                const applyKeyProps = (key, row) => {

                    if(key !== 'partitionKey' && key !== 'rowKey')
                        throw new Error(`Cannot apply key prop ${key} - it must be either partitionKey or rowKey`);

                    if(props[key] === undefined && !row.hasOwnProperty(key))
                        throw new Error(`data does not have a partitionKey - add one, or use the cache function's ${key} argument`);

                    const keyVal = props[key];

                    //argued partition/row key is an array.
                    if(Array.isArray(keyVal))
                        return keyVal.map(ukey => row[ukey]).join('')

                    //argued partition/row key is a function.
                    if(typeof keyVal === 'function')
                        return keyVal(row);

                    //argued partition/row must be a string.
                    return row[keyVal];
                }

                let numRowsTransformed = 0;

                try {

                    pipeline(
                        await fromAzureDatalake({url}),
                        CSVStreamToKeywordObjects(),
                        transformKeywordObjects({
                            transformer: (acc, row) => {
                                row.partitionKey = applyKeyProps('partitionKey', row);
                                row.rowKey = applyKeyProps('rowKey', row);
                                if(types && Object.keys(types).length)
                                    row = _castKeywordObject(row, types);
                                numRowsTransformed++;
                                acc.push(row);
                                return acc;
                            },
                            initial: []
                        }),
                        toAzureDataTables({table, overwrite, append}),
                        err => {
                            if(err)
                                return reject(err);
                            
                            resolve({numRowsInserted: numRowsTransformed});
                        }
                    );
                }
                catch( err ) {
                    return reject(err);
                }

            }
            catch( err ) {

                reject(new Error(`SimpleDatalakeClient::ext.etl has failed - ${err.message}`));
            }
        });

    }

    async modify( props:I.modifyFileProps, parserOptions:I.ExtendedParserOptionsArgs={} ):Promise<any> {

        try {

            const {
                url,
                pk,
                modifications
            } = props;

            let {
                targetUrl
            } = props;

            let {
                delimiter
            } = parserOptions;

            const report = {};

            delimiter = delimiter || ',';
            targetUrl = targetUrl || `${url}.tmp`;
            parserOptions['key_values'] = true;

            await new Promise( async (resolve, reject) => {

                pipeline(
                    await fromAzureDatalake({url}),
                    CSVStreamToKeywordObjects(),
                    applyMutations({
                        pk,
                        modifications,
                        report
                    }),
                    keywordArrayToCSV({parserOptions:{delimiter}}),
                    await toAzureDatalake({url, replace:true}),
                    err => {
                        if(err)
                            return reject(err);
                        resolve(true);
                    }
                )

            })

            return report;
        }
        catch( err ) {

            throw new Error(`SimpleDatalakeClient::ext.modify has failed - ${err.message}`);
        }
    }

    async addNewColumns( props, parserOptions:I.ExtendedParserOptionsArgs={} ):Promise<boolean> {

        return new Promise(async (resolve, reject) => {

            try {

                const {
                    url,
                    columns
                } = props;

                const parserOptions = props['parserOptions'] || {};
                parserOptions.header = parserOptions.hasOwnProperty('key_values')
                    ? parserOptions.key_values
                    : true;

                let numRowsTransformed = 0;

                const detectedParserOptions = {
                    delimiter: null,
                    linebreak: null
                };

                const csvParserOptions = {
                    linebreak: null
                };

                const onFirstChunk = meta => {
                    detectedParserOptions.delimiter = meta.delimiter;
                    detectedParserOptions.linebreak = meta.linebreak;
                };

                try {

                    pipeline(
                        await fromAzureDatalake({url}),
                        CSVStreamToKeywordObjects({onFirstChunk}),
                        addColumns({
                            columns
                        }),
                        keywordArrayToCSV({parserOptions:detectedParserOptions}),
                        await toAzureDatalake({url, replace:true, parserOptions: detectedParserOptions}),
                        err => {
                            if(err)
                                return reject(err);
                            
                            resolve(true);
                        }
                    );
                }
                catch( err ) {
                    return reject(err);
                }

            }
            catch( err ) {

                reject(new Error(`SimpleDatalakeClient::ext.etl has failed - ${err.message}`));
            }
        });

    }

    derivePk( pk , keyed_row ):string {
        
        switch(typeof pk) {

            case 'string':
                return keyed_row['pk'];

            case 'object':
                if(!Array.isArray(pk))
                    throw Error(`unable to use argued pk - expected string|string[]|Function - got ${typeof pk}`);

                return pk.reduce((acc, key) => acc.concat(keyed_row['pk']), '');

            case 'function':
                try {
                    const result = pk(keyed_row);
                    if(!result)
                        throw new Error('PK functions must return a truthy result');
                    return result;
                }
                catch( err ) {
                    throw new Error(`pk function thew an error - ${err.message}`)
                }

            default:
                throw new Error(`unable to use argued pk - expected string | string[] | Function - got ${typeof pk}`);

        }

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
                acc[key] = obj[key].toString();
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

/**
 * Convert a list of transaction actions to a binned structure useful for TableClient.submitTransaction. Each bin
 * will be sorted into the transaction type (eg create, update etc) then the partitionKey as each action submitTransaction
 * accepts must be the same type and of the same partition.
 *  
 * @param actions Array of Transaction Actions - eg ['create', {partitionkey:'a', rowKey:'a', data:1}]
 * @param options Object a keyword options object 
 * 
 * @returns Object binned structure of { <transactionType> : { <partitionKey> : [ action, .. ] } } 
 */
function partitionActionsStack( actions ) {

    try {

        return actions.reduce((acc, txn) => {

            const [action, data] = txn;

            const {
                partitionKey,
                rowKey
            } = data;

            if(!partitionKey || !rowKey)
                throw new Error(`invalid row - missing partition or row key`);

            acc[action] = acc[action] || {};
            acc[action][partitionKey] = acc[action][partitionKey] || [];
            acc[action][partitionKey].push(txn);
            return acc;

        }, {});

    }
    catch( err ) {

        throw new Error(`partitionActionsStack has failed - ${err.message}`);
    }
}

/**
 * Submit a set of actions which are binned appropriately (@see partitionActionsStack output) - that is;
 * { <transactionType> : { <partitionKey> : [ action, .. ] } }
 * 
 * @param partitionedActions 
 * @param tableClient
 *  
 * @returns boolean if all transactions were successfull
 * @throws Error 
 */
async function submitPartitionedActions( partitionedActions, tableClient ) {

    try {

        const txns = [];

        Object.keys(partitionedActions).forEach(action => {
            Object.keys(partitionedActions[action]).forEach(partition => {
                if(partitionedActions[action][partition].length)
                    txns.push(partitionedActions[action][partition]);
            });
        })

        try {

            //txns.every(ktxn => allTxnsAreUnique(ktxn))
            
            for(var i=0; i < txns.length; i++) {
                const actions = txns[i];
                await tableClient.submitTransaction(actions);
                setTimeout(() => {}, 100);
            }

            // when microsoft fix their stuff - use below:
            // await Promise.all(txns.map(actions => tableClient.submitTransaction(actions)}));
        }
        catch( err ) {

            throw new Error(`failed completing transactions - ${err.message}`);
        }

        return true;
    }
    catch( err ) {

        throw new Error(`submitPartitionedActions has failed - ${err.message}`);
    }



}

/**
 * Ensures all the transactions are unique.
 * 
 * @param txns 
 * 
 * @returns boolean
 * @throws Error - w/ a list of non-unique partitionKey/RowKey combinations 
 */
function allTxnsAreUnique( txns ) {

    const keys = [];
    const conflictedKeys = [];

    const unique = txns.forEach(txn => {
        const pk = [txn[1].partitionKey, txn[1].rowKey].join('|');
        const found = keys.includes(pk);
        if(found) {
            conflictedKeys.push(pk);
        }
        keys.push(pk);
    });

    if(conflictedKeys.length)
        throw new Error(`cannot perform transactions - attempting to create some entities twice (partitionKey|rowKey) ${conflictedKeys.join('\n')}`);

    return true;
}

async function emptyTable(client, table) {

    const result = await client.listEntities();
    let spool = {}

    for await (const entity of await client.listEntities()) {

        if(!spool.hasOwnProperty(entity.partitionKey)){
            spool[entity.partitionKey] = {currentBinIdx: 0, bins: [[]]};
        }

        let currentBinIdx = spool[entity.partitionKey].currentBinIdx;
        if(spool[entity.partitionKey].bins[currentBinIdx].length > 99){
            spool[entity.partitionKey].currentBinIdx++;
            currentBinIdx = spool[entity.partitionKey].currentBinIdx;
            spool[entity.partitionKey].bins.push([]);
        }
        spool[entity.partitionKey].bins[currentBinIdx].push(entity)

    }

    if(!Object.keys(spool).length)
        return true;

    const batchStack = Object.keys(spool).reduce((acc, pk) => {
        const batches = spool[pk].bins.map(bin => {
            const actions = [];
            bin.forEach(entity => actions.push(['delete', entity]))
            return actions;
        });
        acc = acc.concat(batches);
        return acc;
    }, []);

    await Promise.all(batchStack);
    for(let i=0; i<batchStack.length; i++) {
        await client.submitTransaction(batchStack[i]);
    }

    return true;
}