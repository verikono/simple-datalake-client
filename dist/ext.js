"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.AzureDatalakeExt = void 0;
const parse_1 = require("@fast-csv/parse");
const stream_1 = require("stream");
const zlib = __importStar(require("zlib"));
const os_1 = require("os");
const data_tables_1 = require("@azure/data-tables");
const transforms_1 = require("./transforms");
const loaders_1 = require("./loaders");
const terminators_1 = require("./terminators");
class AzureDatalakeExt {
    constructor(props) {
        this.client = null;
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
    get(props, parserOptions = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            const { url } = props;
            return this.map({ url, mapper: data => data }, parserOptions);
        });
    }
    find() {
        return __awaiter(this, void 0, void 0, function* () {
        });
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
    reduce(props, parserOptions = {}) {
        return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
            try {
                const { url, reducer } = props;
                let { accumulator } = props, i = 0, keys;
                parserOptions['key_values'] = parserOptions['key_values'] === undefined
                    ? true : parserOptions['key_values'];
                let stream;
                try {
                    stream = yield this.client.readableStream({ url });
                    if (url.substr(-2) === 'gz')
                        stream = stream.pipe(zlib.createGunzip());
                }
                catch (err) {
                    return reject(err);
                }
                stream_1.pipeline(stream, parse_1.parse(parserOptions)
                    .on('data', data => {
                    try {
                        if (parserOptions['key_values']) {
                            if (i === 0 && !keys) {
                                keys = data;
                                return;
                            }
                            const keyed_data = keys.reduce((acc, key, i) => {
                                acc[key] = data[i];
                                return acc;
                            }, {});
                            accumulator = reducer(accumulator, keyed_data, i);
                        }
                        else {
                            accumulator = reducer(accumulator, data, i);
                        }
                    }
                    catch (err) {
                        throw err;
                    }
                })
                    .on('error', err => {
                    return reject(err);
                })
                    .on('end', () => resolve(accumulator)), err => reject);
                return accumulator;
            }
            catch (err) {
                return reject(Error(`SimpleDatalakeClient::ext.reduce has failed - ${err.message}`));
            }
        }));
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
    map(props, parserOptions = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
                try {
                    const { url } = props;
                    let { mapper, persist } = props;
                    const parserOptions = props['parserOptions'] || {};
                    parserOptions.header = parserOptions.hasOwnProperty('key_values')
                        ? parserOptions.key_values
                        : true;
                    const promises = [];
                    const zipped = url.substr(-2) === 'gz';
                    try {
                        stream_1.pipeline(yield loaders_1.fromAzureDatalake({ url }), transforms_1.eachRow({
                            parserOptions,
                            onRow: (data, i) => {
                                const result = mapper(data, i);
                                promises.push(result);
                            },
                            onError: reject,
                            onEnd: (meta) => __awaiter(this, void 0, void 0, function* () {
                                let result;
                                try {
                                    //await all promises in the mapped stack
                                    result = yield Promise.all(promises);
                                }
                                catch (err) {
                                    if (!err.message.includes(`Cannot read property 'ERROR' of undefined`))
                                        throw err;
                                }
                                const { delimiter } = meta;
                                //if we're saving the result back to the datalake overwriting what WAS there.
                                if (persist) {
                                    //CSVify the result
                                    let headings, rows, content;
                                    //if we mapped over key/value objects, CSVify based of an array of keyvalue objects.
                                    if (parserOptions['header']) {
                                        try {
                                            headings = Object.keys(result[0]).join(delimiter);
                                            rows = result.map(data => Object.values(data).join(delimiter));
                                            content = [].concat(headings, rows).join(os_1.EOL);
                                        }
                                        catch (err) {
                                            throw Error('Failed to construct the result to CSV.');
                                        }
                                    }
                                    //if we mapped over raw rows csvify that way.
                                    else {
                                        content = result.map(data => data.join(delimiter)).join(os_1.EOL);
                                    }
                                    //push the CSV back up to the datalake
                                    try {
                                        //if the URL is a zip file, zip up the contents.
                                        if (zipped)
                                            throw Error(`Unimplemented - zip up a mapped result`);
                                        const client = this.client.getFileClient({ url });
                                        yield client.create();
                                        yield client.append(content, 0, content.length);
                                        yield client.flush(content.length);
                                    }
                                    catch (err) {
                                        throw Error(`Failed uploading result to datalake - ${err.message}`);
                                    }
                                }
                                resolve(result);
                            })
                        }), terminators_1.nullTerminator(), err => {
                            if (err)
                                reject(err);
                        });
                    }
                    catch (err) {
                        return reject(err);
                    }
                }
                catch (err) {
                    reject(Error(`SimpleDatalakeClient::ext.map has failed ${err.message}`));
                }
            }));
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
    forEach(props, parserOptions = {}) {
        return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
            try {
                const { url, fn } = props;
                let { block } = props;
                parserOptions['header'] = parserOptions['header'] === undefined ? true : parserOptions['header'];
                block = block === undefined ? true : block;
                if (parserOptions['key_values'] !== undefined) {
                    console.warn('SimpleDatalakeClient::ext.forEach is depreacting the key_values option - use option header instead');
                    parserOptions['header'] = parserOptions['key_values'];
                    delete parserOptions['key_values'];
                }
                let promises = [];
                stream_1.pipeline(yield loaders_1.fromAzureDatalake({ url }), transforms_1.eachRow({
                    parserOptions,
                    onRow: (data, i) => {
                        const result = fn(data, i);
                        promises.push(result);
                    },
                    onError: reject,
                    onEnd: (meta) => __awaiter(this, void 0, void 0, function* () {
                        if (block) {
                            try {
                                yield Promise.all(promises);
                            }
                            catch (err) {
                                if (!err.message.includes(`Cannot read property 'ERROR' of undefined`))
                                    throw err;
                            }
                            resolve();
                        }
                        else {
                            resolve();
                        }
                    })
                }), terminators_1.nullTerminator(), err => {
                    if (err)
                        reject(err);
                });
            }
            catch (err) {
                reject(Error(`SimpleDatalakeClient::ext.forEach has failed ${err.message}`));
            }
        }));
    }
    /**
     * Map over data in slices. Useful for batch operations such as insertion.
     *
     * @param props the argument object
     * @param props.mapper the mapping function
     * @param props.size the size of each slice, default: 1000;
     * @param parserOptions - see https://c2fo.github.io/fast-csv/docs/parsing/options for available options.
     */
    mapSlices(props, parserOptions = {}) {
        return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
            const { url, mapper } = props;
            let i = 0, size, slice = [], promises = [], keys, result = [];
            size = props.size || 1000;
            parserOptions['key_values'] = parserOptions['key_values'] === undefined
                ? true : parserOptions['key_values'];
            let stream;
            try {
                stream = yield this.client.readableStream({ url });
                if (url.substr(-2) === 'gz')
                    stream = stream.pipe(zlib.createGunzip());
            }
            catch (err) {
                return reject(err);
            }
            try {
                stream_1.pipeline(stream, parse_1.parse(parserOptions)
                    .on('data', data => {
                    if (parserOptions['key_values']) {
                        if (i === 0 && !keys) {
                            keys = data;
                            return;
                        }
                        const keyed_data = keys.reduce((acc, key, i) => {
                            if (parserOptions['nullifyEmptyColumns'] && typeof data[i] === 'string' && !data[i].length)
                                data[i] = null;
                            acc[key] = data[i];
                            return acc;
                        }, {});
                        data = keyed_data;
                    }
                    else {
                        data = data.map(value => {
                            if (parserOptions['nullifyEmptyColumns'] && typeof value === 'string' && !value.length)
                                value = null;
                            return value;
                        });
                    }
                    slice.push(data);
                    if (slice.length !== size)
                        return;
                    const ret = mapper(slice);
                    if (ret instanceof Promise)
                        promises.push(ret);
                    slice = [];
                })
                    .on('end', () => __awaiter(this, void 0, void 0, function* () {
                    if (slice.length) {
                        const ret = mapper(slice);
                        if (ret instanceof Promise)
                            promises.push(ret);
                        result = result.concat(ret);
                    }
                    yield Promise.all(promises);
                    resolve(result);
                }))
                    .on('error', err => reject), err => reject);
            }
            catch (err) {
                return reject(err);
            }
        }));
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
    count(props, parserOptions = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
                try {
                    const { url, includeHeadings } = props;
                    let i = 0, processedHeadings = false;
                    stream_1.pipeline(yield loaders_1.fromAzureDatalake({ url }), parse_1.parse(parserOptions)
                        .on('data', data => {
                        if (i === 0 && !includeHeadings && !processedHeadings) {
                            processedHeadings = true;
                            return;
                        }
                        i++;
                    })
                        .on('error', err => {
                        reject(err);
                    })
                        .on('end', () => {
                        resolve(i);
                    }), err => {
                        if (err)
                            return reject(err);
                    });
                }
                catch (err) {
                    reject(Error(`SimpleDatalakeClient::ext.count has failed ${err.message}`));
                }
            }));
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
    cache(props, parserOptions = {}) {
        return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
            try {
                const { url, table, delimiter = ',', partitionKey, rowKey, types } = props;
                let { replaceIfExists } = props;
                replaceIfExists = replaceIfExists === undefined ? false : replaceIfExists;
                const { AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_ACCOUNT_KEY } = process.env;
                if (!url)
                    throw Error('argue a url.');
                if (typeof AZURE_STORAGE_ACCOUNT !== "string" || !AZURE_STORAGE_ACCOUNT.length)
                    throw Error(`simple_datalake_client::cache failed - missing environment variable STORAGE_ACCOUNT`);
                if (typeof AZURE_STORAGE_ACCOUNT_KEY !== "string" || !AZURE_STORAGE_ACCOUNT_KEY.length)
                    throw Error(`simple_datalake_client::cache failed - missing environment variable STORAGE_ACCOUNT_KEY`);
                const credential = new data_tables_1.AzureNamedKeyCredential(AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_ACCOUNT_KEY);
                const serviceClient = new data_tables_1.TableServiceClient(`https://${AZURE_STORAGE_ACCOUNT}.table.core.windows.net`, credential);
                const tableClient = new data_tables_1.TableClient(`https://${AZURE_STORAGE_ACCOUNT}.table.core.windows.net`, table, credential);
                if (!(yield this.client.exists({ url })))
                    throw new Error(`no file exists at ${url}`);
                try {
                    yield serviceClient.createTable(table);
                    yield emptyTable(tableClient, table);
                }
                catch (err) {
                    if (err.message.includes('TableBeingDeleted')) {
                        console.warn(`table ${table} is queued for deletion by azure, retrying momentarily...`);
                        yield new Promise(r => setTimeout(() => r(true), 2000));
                        let result;
                        try {
                            result = yield this.cache(props, parserOptions);
                        }
                        catch (err) {
                            return reject(err);
                        }
                        return resolve(result);
                    }
                    else if (err.message.includes('TableAlreadyExists') && replaceIfExists) {
                        console.warn(`table ${table} exists - dropping by request.`);
                        yield serviceClient.deleteTable(table);
                        let result;
                        try {
                            result = yield this.cache(props, parserOptions);
                        }
                        catch (err) {
                            return reject(err);
                        }
                        return resolve(result);
                    }
                    else {
                        return reject(Error(`SimpleDatalakeClient:ext::cache failed to build target table ${table} using credentials [${AZURE_STORAGE_ACCOUNT}][${AZURE_STORAGE_ACCOUNT_KEY}] ${err.message}`));
                    }
                }
                let numRowsProcessed = 0;
                let transactions = [];
                try {
                    yield this.forEach({
                        url,
                        fn: (row, i) => __awaiter(this, void 0, void 0, function* () {
                            let result;
                            try {
                                row.partitionKey = typeof partitionKey === 'function'
                                    ? partitionKey(row)
                                    : row[partitionKey];
                                row.rowKey = typeof rowKey === 'function'
                                    ? rowKey(row)
                                    : row[rowKey];
                                if (types && Object.keys(types).length)
                                    row = _castKeywordObject(row, types);
                                if (row.partitionKey && row.rowKey)
                                    transactions.push(['create', row]);
                                else {
                                    console.log('---');
                                }
                                numRowsProcessed++;
                            }
                            catch (err) {
                                yield serviceClient.deleteTable(table);
                                return reject(Error(`Purged table ${table} - data extraction failed - an error has occured in the mapper function provided to SimpleDatalakeClient::ext.cache - ${err.message}`));
                            }
                        })
                    }, parserOptions);
                }
                catch (err) {
                    reject(err);
                }
                const partitionedActions = partitionActionsStack(transactions);
                yield submitPartitionedActions(partitionedActions, tableClient);
                return resolve({
                    numRowsInserted: transactions.length
                });
            }
            catch (err) {
                reject(Error(`SimpleDatalakeClient::ext.cache has failed ${err.message}`));
            }
        }));
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
    compile(props, parserOptions = {}) {
        try {
            return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
                const { urls, pk } = props;
                let { delimiter } = parserOptions;
                delimiter = delimiter || ',';
                parserOptions['key_values'] = true;
                let columnNames = null;
                let rowNum = 0;
                let result = [];
                const diff = {};
                for (let fileItr = 0; fileItr < urls.length; fileItr++) {
                    const url = urls[fileItr];
                    const zipped = url.substr(-2) === 'gz';
                    let stream;
                    try {
                        stream = yield this.client.readableStream({ url });
                        if (zipped)
                            stream = stream.pipe(zlib.createGunzip());
                    }
                    catch (err) {
                        return reject(new Error(`AzureDatalakeClient::compiled failed to load data from ${url} - ${err.message}`));
                    }
                    //helper function which converts a row of values to a key/value object based from the
                    //column names accumulated in the first row.
                    const toKeyValueObject = data => columnNames.reduce((acc, col, i) => Object.assign(acc, { [col]: data[i] }), {});
                    const rowIsIdentical = (row1, row2) => JSON.stringify(row1) === JSON.stringify(row2);
                    yield new Promise((res, rej) => {
                        try {
                            let deleteRows = [];
                            const newRows = [];
                            stream_1.pipeline(stream, parse_1.parse(parserOptions)
                                .on('data', data => {
                                //first row of the first file is assumed to contain the column names.
                                if (rowNum === 0 && fileItr === 0) {
                                    columnNames = data;
                                }
                                //first row of 2nd+ file - assumed to be the column namesdat
                                else if (rowNum === 0 && fileItr !== 0) {
                                    //ensure the keys are the same for each file, ensure this new file has the same columns
                                    if (data.length !== columnNames.length || !data.every(col => columnNames.includes(col))) {
                                        data.forEach(dataColumn => {
                                            const isNewColumn = !columnNames.includes(dataColumn);
                                        });
                                        console.log('---');
                                        return reject(new Error(`Compile can only work for files with the same columns`));
                                    }
                                    //set up the dleteRows which we'll each one off as we iterate through them, leaving any
                                    //left which the 'end' event is fired to be scheduled for deletion.
                                    deleteRows = result.map(row => row._pk);
                                }
                                //2nd+ row from first file.
                                else if (rowNum !== 0 && fileItr == 0) {
                                    //create a map of the data, and attach a PK.
                                    const keyed_data = toKeyValueObject(data);
                                    keyed_data._pk = this.derivePk(pk, keyed_data);
                                    result.push(keyed_data);
                                }
                                //2nd+ row from 2nd+ file, here we merge.
                                else if (rowNum !== 0 && fileItr !== 0) {
                                    //create a map of the data, and attach a PK.
                                    const keyed_data = toKeyValueObject(data);
                                    keyed_data._pk = this.derivePk(pk, keyed_data);
                                    //find the row in the result.
                                    let resultIdx = result.findIndex(result => result._pk === keyed_data._pk);
                                    //if its a new row, then add it as a "new" to the rollup (performed when an end event is fired, see .on('end')).
                                    if (resultIdx === -1) {
                                        newRows.push(keyed_data);
                                        return;
                                    }
                                    deleteRows = deleteRows.filter(pk => pk !== keyed_data._pk);
                                    const noChangesInRow = rowIsIdentical(result[resultIdx], keyed_data);
                                    if (!noChangesInRow) {
                                        columnNames.reduce((acc, key) => {
                                            try {
                                                //if(newRow) return acc;
                                                if (result[resultIdx][key].toString() === keyed_data[key].toString())
                                                    return acc;
                                                if (!diff.hasOwnProperty(keyed_data._pk))
                                                    diff[keyed_data._pk] = {};
                                                if (!diff[keyed_data._pk].hasOwnProperty(key))
                                                    diff[keyed_data._pk][key] = [];
                                                diff[keyed_data._pk][key].push({
                                                    type: 'variation',
                                                    value: keyed_data[key],
                                                    value_from: result[resultIdx][key],
                                                    url: urls[fileItr]
                                                });
                                                result[resultIdx][key] = keyed_data[key];
                                                return acc;
                                            }
                                            catch (err) {
                                                throw new Error(`a problem was encountered whilst interpretting a change upon column:${key} on row ${rowNum} of ${urls[fileItr]}} - ${err.message}`);
                                            }
                                        }, []);
                                    }
                                }
                                rowNum++;
                            })
                                .on('error', err => {
                                return rej(err);
                            })
                                .on('end', () => __awaiter(this, void 0, void 0, function* () {
                                rowNum = 0;
                                deleteRows.forEach(pk => {
                                    const resultRow = result.find(res => res._pk === pk);
                                    if (!diff.hasOwnProperty(pk))
                                        diff[pk] = {};
                                    diff[pk] = Object.keys(resultRow).reduce((acc, key) => {
                                        acc[key] = {
                                            type: 'delete',
                                            value: null,
                                            value_from: resultRow[key],
                                            url: urls[fileItr]
                                        };
                                        return acc;
                                    }, {});
                                    result = result.filter(res => res._pk !== pk);
                                });
                                newRows.forEach(keyed_data => {
                                    diff[keyed_data._pk] = columnNames.reduce((acc, key) => {
                                        acc[key] = {
                                            type: 'new',
                                            value: keyed_data[key],
                                            value_from: null,
                                            url: urls[fileItr]
                                        };
                                        return acc;
                                    }, {});
                                    result.push(keyed_data);
                                });
                                return res(true);
                            })), err => rej);
                        }
                        catch (err) {
                            return rej(err);
                        }
                    })
                        .catch(err => reject(new Error(`AzureDatalake.ext has failed - ${err.message}`)));
                }
                resolve({ data: result, diff });
            }));
        }
        catch (err) {
            throw new Error(`SimpleDatalakeClient::ext.compiled has failed -  ${err.message}`);
        }
    }
    modify(props, parserOptions = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const { url, pk, modifications } = props;
                let { targetUrl } = props;
                let { delimiter } = parserOptions;
                const report = {};
                delimiter = delimiter || ',';
                targetUrl = targetUrl || `${url}.tmp`;
                parserOptions['key_values'] = true;
                yield new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
                    stream_1.pipeline(yield loaders_1.fromAzureDatalake({ url }), transforms_1.CSVStreamToKeywordObjects(), transforms_1.applyMutations({
                        pk,
                        modifications,
                        report
                    }), transforms_1.keywordArrayToCSV({ delimiter }), yield loaders_1.toAzureDatalake({ url, replace: true }), err => {
                        if (err)
                            return reject(err);
                        resolve(true);
                    });
                }));
                return report;
            }
            catch (err) {
                throw new Error(`SimpleDatalakeClient::ext.modify has failed - ${err.message}`);
            }
        });
    }
    derivePk(pk, keyed_row) {
        switch (typeof pk) {
            case 'string':
                return keyed_row['pk'];
            case 'object':
                if (!Array.isArray(pk))
                    throw Error(`unable to use argued pk - expected string|string[]|Function - got ${typeof pk}`);
                return pk.reduce((acc, key) => acc.concat(keyed_row['pk']), '');
            case 'function':
                try {
                    const result = pk(keyed_row);
                    if (!result)
                        throw new Error('PK functions must return a truthy result');
                    return result;
                }
                catch (err) {
                    throw new Error(`pk function thew an error - ${err.message}`);
                }
            default:
                throw new Error(`unable to use argued pk - expected string | string[] | Function - got ${typeof pk}`);
        }
    }
}
exports.AzureDatalakeExt = AzureDatalakeExt;
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
function _castKeywordObject(obj, definitions) {
    return Object.keys(obj).reduce((acc, key) => {
        if (!definitions.hasOwnProperty(key)) {
            acc[key] = obj[key];
            return acc;
        }
        if (!obj.hasOwnProperty(key)) {
            console.warn(`SimpleDatalakClient::_castKeywordObject - invalid key ${key} does not exist on the argued object - skipping...`);
            return acc;
        }
        let value;
        switch (definitions[key].toLowerCase()) {
            case 'number':
                //let azure tables guess in this case.
                acc[key] = parseFloat(obj[key].toString()).toString();
                break;
            case 'double':
            case 'float':
                value = parseFloat(obj[key].toString());
                //for azure tables not including the key at all is assigning a null when reading rows back and is done because the datatables
                //library can't handle the fact null is an object in javascript.
                if (isNaN(value))
                    break;
                acc[key] = {
                    type: "Double",
                    value: value.toString()
                };
                break;
            case 'integer':
            case 'int':
            case 'int32':
                value = parseInt(obj[key].toString());
                //for azure tables not including the key at all is assigning a null when reading rows back and is done because the datatables
                //library can't handle the fact null is an object in javascript.
                if (isNaN(value))
                    break;
                acc[key] = {
                    type: "Int32",
                    value: value.toString()
                };
                break;
            case 'bigint':
                try {
                    acc[key] = {
                        type: "Int64",
                        value: BigInt(obj[key].toString()).toString()
                    };
                }
                catch (err) {
                    if (err.message.includes("Cannot convert") && obj[key].toString().includes('.')) {
                        console.warn(`impleDatalakClient::_castKeywordObject received a value [${obj[key]}] which cannot be cast to a BigInt, resolving by clipping the decimal digits`);
                        const rational = obj[key].split('.')[0];
                        acc[key] = {
                            type: "Int64",
                            value: BigInt(rational).toString()
                        };
                    }
                }
                break;
            case 'string':
                acc[key] = {
                    type: "String",
                    value: obj[key].toString()
                };
                break;
            case 'boolean':
                value = obj[key] !== '0' && obj[key] !== 'false';
                acc[key] = {
                    type: "Boolean",
                    value: value.toString()
                };
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
function partitionActionsStack(actions) {
    try {
        return actions.reduce((acc, txn) => {
            const [action, data] = txn;
            const { partitionKey, rowKey } = data;
            if (!partitionKey || !rowKey)
                throw new Error(`invalid row - missing partition or row key`);
            acc[action] = acc[action] || {};
            acc[action][partitionKey] = acc[action][partitionKey] || [];
            acc[action][partitionKey].push(txn);
            return acc;
        }, {});
    }
    catch (err) {
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
function submitPartitionedActions(partitionedActions, tableClient) {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            const txns = [];
            Object.keys(partitionedActions).forEach(action => {
                Object.keys(partitionedActions[action]).forEach(partition => {
                    if (partitionedActions[action][partition].length)
                        txns.push(partitionedActions[action][partition]);
                });
            });
            try {
                //txns.every(ktxn => allTxnsAreUnique(ktxn))
                for (var i = 0; i < txns.length; i++) {
                    const actions = txns[i];
                    yield tableClient.submitTransaction(actions);
                    setTimeout(() => { }, 100);
                }
                // when microsoft fix their stuff - use below:
                // await Promise.all(txns.map(actions => tableClient.submitTransaction(actions)}));
            }
            catch (err) {
                throw new Error(`failed completing transactions - ${err.message}`);
            }
            return true;
        }
        catch (err) {
            throw new Error(`submitPartitionedActions has failed - ${err.message}`);
        }
    });
}
/**
 * Ensures all the transactions are unique.
 *
 * @param txns
 *
 * @returns boolean
 * @throws Error - w/ a list of non-unique partitionKey/RowKey combinations
 */
function allTxnsAreUnique(txns) {
    const keys = [];
    const conflictedKeys = [];
    const unique = txns.forEach(txn => {
        const pk = [txn[1].partitionKey, txn[1].rowKey].join('|');
        const found = keys.includes(pk);
        if (found) {
            conflictedKeys.push(pk);
        }
        keys.push(pk);
    });
    if (conflictedKeys.length)
        throw new Error(`cannot perform transactions - attempting to create some entities twice (partitionKey|rowKey) ${conflictedKeys.join('\n')}`);
    return true;
}
function emptyTable(client, table) {
    var e_1, _a;
    return __awaiter(this, void 0, void 0, function* () {
        const result = yield client.listEntities();
        let spool = {};
        try {
            for (var _b = __asyncValues(yield client.listEntities()), _c; _c = yield _b.next(), !_c.done;) {
                const entity = _c.value;
                if (!spool.hasOwnProperty(entity.partitionKey)) {
                    spool[entity.partitionKey] = { currentBinIdx: 0, bins: [[]] };
                }
                let currentBinIdx = spool[entity.partitionKey].currentBinIdx;
                if (spool[entity.partitionKey].bins[currentBinIdx].length > 99) {
                    spool[entity.partitionKey].currentBinIdx++;
                    currentBinIdx = spool[entity.partitionKey].currentBinIdx;
                    spool[entity.partitionKey].bins.push([]);
                }
                spool[entity.partitionKey].bins[currentBinIdx].push(entity);
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (_c && !_c.done && (_a = _b.return)) yield _a.call(_b);
            }
            finally { if (e_1) throw e_1.error; }
        }
        if (!Object.keys(spool).length)
            return true;
        const batchStack = Object.keys(spool).reduce((acc, pk) => {
            const batches = spool[pk].bins.map(bin => {
                const actions = [];
                bin.forEach(entity => actions.push(['delete', entity]));
                return actions;
            });
            acc = acc.concat(batches);
            return acc;
        }, []);
        yield Promise.all(batchStack);
        for (let i = 0; i < batchStack.length; i++) {
            yield client.submitTransaction(batchStack[i]);
        }
        return true;
    });
}
//# sourceMappingURL=ext.js.map