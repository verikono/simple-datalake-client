"use strict";
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
exports.toAzureDataTables = exports.TrmAzureDataTables = void 0;
const stream_1 = require("stream");
const data_tables_1 = require("@azure/data-tables");
class TrmAzureDataTables extends stream_1.Writable {
    constructor(props) {
        try {
            super();
            this.targetTable = props.table;
            this.AZURE_STORAGE_ACCOUNT = props.AZURE_STORAGE_ACCOUNT;
            this.AZURE_STORAGE_ACCOUNT_KEY = props.AZURE_STORAGE_ACCOUNT_KEY;
            this.attemptedTableCreation = false;
            this.purgeIfExists = props.overwrite === undefined ? false : props.overwrite;
            this.appendExistingData = props.append === undefined ? false : props.append;
            this.credential = this.getCredential();
            this.result = [];
        }
        catch (err) {
            throw new Error(`toAzureDataTables has failed to construct - ${err.message}`);
        }
    }
    _write(chunk, encoding, callback) {
        try {
            let data;
            try {
                data = JSON.parse(chunk.toString());
            }
            catch (err) {
                throw new Error(`failedconst transaction = new TableTransaction(); parsing data chunk - expected data to be a JSON described array of keyword objects`);
            }
            if (!Array.isArray(data) || !data.length || !Object.keys(data[0]).length)
                throw new Error(`expected data to be a JSON described array of keyword objects`);
            if (!data[0].hasOwnProperty('partitionKey'))
                throw new Error(`data should have its partition and row keys set prior to this module`);
            this.result = this.result.concat(data);
            callback();
        }
        catch (err) {
            callback(new Error(`toAzureDataTables has failed - ${err.message}`));
        }
    }
    _final(callback) {
        const client = this.client || new data_tables_1.TableClient(this.tableUrl(), this.targetTable, this.credential);
        new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
            try {
                yield this.createTableIfNotExists();
                const pkBins = this.result.reduce((acc, entity) => {
                    const { partitionKey } = entity;
                    if (!acc.hasOwnProperty(partitionKey))
                        acc[partitionKey] = [];
                    acc[partitionKey].push(entity);
                    return acc;
                }, {});
                let bins = [];
                Object.keys(pkBins).forEach(partitionKey => {
                    const binFeed = pkBins[partitionKey].slice();
                    while (binFeed.length) {
                        bins.push(binFeed.splice(0, 99));
                    }
                });
                const txnStart = new Date().getTime();
                this.allBinsUnique(bins);
                for (var i = 0; i < bins.length; i++) {
                    const bin = bins[i];
                    const txns = bin.map(itm => ['create', itm]);
                    yield client.submitTransaction(txns);
                }
                resolve(true);
            }
            catch (err) {
                return reject(err);
            }
        }))
            .then(result => {
            callback();
        })
            .catch(err => {
            callback(err);
        });
    }
    createTableIfNotExists() {
        var e_1, _a;
        return __awaiter(this, void 0, void 0, function* () {
            if (this.attemptedTableCreation)
                return;
            const service = this.service || new data_tables_1.TableServiceClient(this.tableUrl(), this.credential);
            let tablesIter = service.listTables();
            try {
                //iterate through the tables
                for (var tablesIter_1 = __asyncValues(tablesIter), tablesIter_1_1; tablesIter_1_1 = yield tablesIter_1.next(), !tablesIter_1_1.done;) {
                    const table = tablesIter_1_1.value;
                    if (table.name === this.targetTable) {
                        //found!
                        if (this.purgeIfExists) {
                            //we purge all data, and return - leaving us with an empty table
                            yield this.emptyTableOfContents();
                            return;
                        }
                        else if (this.appendExistingData) {
                            //we will be appending existing data
                            return;
                        }
                        else {
                            //error off, the table exists and the overwrite flag is false and we're not appending data.
                            throw new Error(`Table ${this.targetTable} exists - either argue "overwrite":true if thats what you want.`);
                        }
                    }
                }
            }
            catch (e_1_1) { e_1 = { error: e_1_1 }; }
            finally {
                try {
                    if (tablesIter_1_1 && !tablesIter_1_1.done && (_a = tablesIter_1.return)) yield _a.call(tablesIter_1);
                }
                finally { if (e_1) throw e_1.error; }
            }
            //nothing found, create the table.
            yield service.createTable(this.targetTable);
        });
    }
    //used instead of deletion which will take 30seconds to queue at table deletion.
    emptyTableOfContents() {
        var e_2, _a;
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const { targetTable: table } = this;
                if (!table || !table.length)
                    throw Error(`invalid keyword "targetTable" argued`);
                const client = this.client || new data_tables_1.TableClient(this.tableUrl(), this.targetTable, this.credential);
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
                        const { partitionKey, rowKey } = entity;
                        spool[entity.partitionKey].bins[currentBinIdx].push([
                            'delete',
                            {
                                partitionKey,
                                rowKey
                            }
                        ]);
                    }
                }
                catch (e_2_1) { e_2 = { error: e_2_1 }; }
                finally {
                    try {
                        if (_c && !_c.done && (_a = _b.return)) yield _a.call(_b);
                    }
                    finally { if (e_2) throw e_2.error; }
                }
                try {
                    for (var i = 0; i < Object.keys(spool).length; i++) {
                        const partitionKey = Object.keys(spool)[i];
                        for (var ii = 0; ii < spool[partitionKey].bins.length; ii++) {
                            yield client.submitTransaction(spool[partitionKey].bins[ii]);
                        }
                    }
                }
                catch (err) {
                    throw new Error(`Failed emptying table ${this.targetTable} - ${err.message}`);
                }
                //the below method replaces the block above and should work however it appears there's
                //issues with azure tables when it comes to processing a lot of transactions simultaneously.
                //boo! :(
                // const promises = Object.keys(spool).map(partitionKey => {
                //     return Promise.all(spool[partitionKey].bins.map(async set => {
                //         try {
                //             return await client.submitTransaction(set);
                //         }
                //         catch( err ) {
                //             console.log('#################33');
                //         }
                //     }));
                // });
                //await Promise.all(promises);
                return true;
            }
            catch (err) {
                throw Error(`toAzureDataTables::emptyTableOfContents has failed - ${err.message}`);
            }
        });
    }
    allBinsUnique(bins) {
        const chk = [];
        bins.forEach((bin, binIdx) => {
            bin.forEach((entity, entityIdx) => {
                const key = `${entity.partitionKey}${entity.rowKey}`;
                if (chk.includes(key))
                    throw new Error(`a duplicate partition/row key combination was found for partitionKey:${entity.partitionKey}/rowKey:${entity.rowKey} in bin #${binIdx + 1}`);
                chk.push(key);
            });
        });
    }
    tableUrl() {
        if (!this.AZURE_STORAGE_ACCOUNT)
            throw new Error(`cannot resolve tableUrl - AZURE_STORAGE_ACCOUNT is not set.`);
        return `https://${this.AZURE_STORAGE_ACCOUNT}.table.core.windows.net`;
    }
    getCredential() {
        const ASA = this.AZURE_STORAGE_ACCOUNT || process.env.AZURE_STORAGE_ACCOUNT || process.env.STORAGE_ACCOUNT;
        const ASAK = this.AZURE_STORAGE_ACCOUNT_KEY || process.env.AZURE_STORAGE_ACCOUNT_KEY || process.env.STORAGE_ACCOUNT_KEY;
        if (!ASA || !ASAK)
            throw new Error(`Failed gaining azure credentials - either argue toAzureDataTables with a AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_ACCOUNT_KEY or set these as environment variables.`);
        this.AZURE_STORAGE_ACCOUNT = ASA;
        this.AZURE_STORAGE_ACCOUNT_KEY = ASAK;
        this.credential = new data_tables_1.AzureNamedKeyCredential(ASA, ASAK);
        return this.credential;
    }
}
exports.TrmAzureDataTables = TrmAzureDataTables;
const toAzureDataTables = (props = {}) => new TrmAzureDataTables(props);
exports.toAzureDataTables = toAzureDataTables;
//# sourceMappingURL=AzureDataTables.js.map