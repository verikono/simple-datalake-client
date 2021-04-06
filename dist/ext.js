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
Object.defineProperty(exports, "__esModule", { value: true });
exports.AzureDatalakeExt = void 0;
const parse_1 = require("@fast-csv/parse");
const stream_1 = require("stream");
const zlib = __importStar(require("zlib"));
const data_tables_1 = require("@azure/data-tables");
class AzureDatalakeExt {
    constructor(props) {
        this.client = null;
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
     *
     * @param props the property object
     * @param props.url the url of the CSV file we'll be mapping over.
     * @param props.eachRow Function called on each row
     * @param parserOptions - see https://c2fo.github.io/fast-csv/docs/parsing/options for available options such as skipping headers, or objectmode
     */
    map(props, parserOptions = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
                try {
                    const { url } = props;
                    let { mapper } = props, i = 0, promises = [], keys;
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
                                    acc[key] = data[i];
                                    return acc;
                                }, {});
                                promises.push(mapper(keyed_data, i));
                            }
                            else {
                                promises.push(mapper(data, i));
                                i++;
                            }
                        })
                            .on('error', reject)
                            .on('end', () => __awaiter(this, void 0, void 0, function* () {
                            const result = yield Promise.all(promises);
                            resolve(result);
                        })), err => reject);
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
            const { url, fn } = props;
            let { block } = props, i = 0, promises = [], keys;
            parserOptions['key_values'] = parserOptions['key_values'] === undefined
                ? true : parserOptions['key_values'];
            block = block === undefined ? true : block;
            const finalize = () => resolve();
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
                            acc[key] = data[i];
                            return acc;
                        }, {});
                        promises.push(fn(keyed_data, i));
                    }
                    else {
                        promises.push(fn(data, i));
                        i++;
                    }
                })
                    .on('error', reject)
                    .on('end', () => __awaiter(this, void 0, void 0, function* () {
                    if (block) {
                        yield Promise.all(promises);
                        finalize();
                    }
                })), err => reject);
            }
            catch (err) {
                return reject(err);
            }
            if (!block)
                finalize();
        }));
    }
    /**
     * Map over data in slices. Useful for batch operations such as insertion.
     *
     * @param props the argument object
     * @param props.mapper the mapping function
     * @param props.size the size of each slice, default: 1000;
     * @param parserOptions
     */
    mapSlices(props, parserOptions = {}) {
        return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
            const { url, mapper } = props;
            let i = 0, size, slice = [], promises = [], keys;
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
                            acc[key] = data[i];
                            return acc;
                        }, {});
                        data = keyed_data;
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
                    }
                    yield Promise.all(promises);
                    resolve(true);
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
     * @param parserOptions @see https://c2fo.github.io/fast-csv/docs/parsing/options/
     */
    count(props, parserOptions = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
                try {
                    const { url } = props;
                    let stream, i = 0, headersIgnored = false;
                    stream = yield this.client.readableStream({ url });
                    if (url.substr(-2) === 'gz')
                        stream = stream.pipe(zlib.createGunzip());
                    stream_1.pipeline(stream, parse_1.parse(parserOptions)
                        .on('data', data => {
                        // if(parserOptions.headers && !headersIgnored) {
                        //     headersIgnored = true;
                        //     return;
                        // }
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
     * @param props.types object a key value object where the key is the property name and the value is the type - eg { field_one: "number", field_two: "string" }
     * @param parserOptions
     * @todo allow paritionKey and rowKey to be argued as a function.
     */
    cache(props, parserOptions = {}) {
        return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
            try {
                const { url, table, delimiter = ',', partitionKey, rowKey, types } = props;
                let { replaceIfExists } = props;
                replaceIfExists = replaceIfExists === undefined ? false : replaceIfExists;
                const { STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY } = process.env;
                if (!url)
                    throw Error('argue a url.');
                if (typeof STORAGE_ACCOUNT !== "string" || !STORAGE_ACCOUNT.length)
                    throw Error(`simple_datalake_client::cache failed - missing environment variable STORAGE_ACCOUNT`);
                if (typeof STORAGE_ACCOUNT_KEY !== "string" || !STORAGE_ACCOUNT_KEY.length)
                    throw Error(`simple_datalake_client::cache failed - missing environment variable STORAGE_ACCOUNT_KEY`);
                const credential = new data_tables_1.TablesSharedKeyCredential(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY);
                const serviceClient = new data_tables_1.TableServiceClient(`https://${STORAGE_ACCOUNT}.table.core.windows.net`, credential);
                const transactClient = new data_tables_1.TableClient(`https://${STORAGE_ACCOUNT}.table.core.windows.net`, table, credential);
                try {
                    yield serviceClient.createTable(table);
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
                        return reject(Error(`SimpleDatalakeClient:ext::cache failed to build target table ${table} using credentials [${STORAGE_ACCOUNT}][${STORAGE_ACCOUNT_KEY}] ${err.message}`));
                    }
                }
                let numRowsInserted = 0;
                try {
                    yield this.map({
                        url,
                        mapper: (row, i) => __awaiter(this, void 0, void 0, function* () {
                            let result;
                            try {
                                row.PartitionKey = typeof partitionKey === 'function'
                                    ? partitionKey(row)
                                    : row[partitionKey];
                                row.RowKey = typeof rowKey === 'function'
                                    ? rowKey(row)
                                    : row[rowKey];
                                if (types && Object.keys(types).length)
                                    row = _castKeywordObject(row, types);
                                result = yield transactClient.createEntity(row);
                                numRowsInserted++;
                            }
                            catch (err) {
                                yield serviceClient.deleteTable(table);
                                return reject(Error(`Purged table ${table} - data extraction failed - ${err.message}`));
                            }
                        })
                    }, parserOptions);
                }
                catch (err) {
                    reject(err);
                }
                return resolve({
                    numRowsInserted
                });
            }
            catch (err) {
                reject(Error(`SimpleDatalakeClient::ext.cache has failed ${err.message}`));
            }
        }));
    }
}
exports.AzureDatalakeExt = AzureDatalakeExt;
/**
 * Allows recasting of a keyword object's values. Useful being our parser will always return strings for its values.
 *
 * @param obj Object a keywork object
 * @param definitions Object a keyword object where the key is a property in the obj param and the value is a type specified as a string - eg "number"
 *
 * @returns Object with values recast as specified by the defintions.
 */
function _castKeywordObject(obj, definitions) {
    return Object.keys(obj).reduce((acc, key) => {
        if (!definitions.hasOwnProperty(key)) {
            acc[key] = obj[key];
            return acc;
        }
        switch (definitions[key]) {
            //azure keeps this really simple.
            case 'number':
            case 'float':
            case 'integer':
                acc[key] = parseFloat(obj[key].toString());
                break;
            case 'string':
                acc[key] = obj[key].toString();
                break;
            default:
                throw Error(`SimpleDatalakClient::_castKeywordObject key "${key}" has invalid type "${definitions[key]}`);
        }
        return acc;
    }, {});
}
//# sourceMappingURL=ext.js.map