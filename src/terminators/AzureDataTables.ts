import {Writable} from 'stream';
import {
    TableServiceClient,
    AzureNamedKeyCredential,
    TransactionAction,
    TableClient
} from '@azure/data-tables';

export class TrmAzureDataTables extends Writable {

    credential:AzureNamedKeyCredential;
    service: TableServiceClient;
    client: TableClient;
    targetTable:string;

    AZURE_STORAGE_ACCOUNT:string;
    AZURE_STORAGE_ACCOUNT_KEY:string;

    attemptedTableCreation: boolean;
    purgeIfExists: boolean;
    appendExistingData: boolean;

    result: Array<any>;

    objectMode: boolean;
    retriedForOpsQueue: boolean;

    constructor( props ) {

        try {
            super();
            this.targetTable = props.table;
            this.AZURE_STORAGE_ACCOUNT = props.AZURE_STORAGE_ACCOUNT;
            this.AZURE_STORAGE_ACCOUNT_KEY = props.AZURE_STORAGE_ACCOUNT_KEY;

            this.attemptedTableCreation = false;
            this.purgeIfExists = props.overwrite === undefined ? false : props.overwrite;
            this.appendExistingData = props.append === undefined ? false : props.append;
            this.credential = this.getCredential();
            this.retriedForOpsQueue = false;
            this.result = [];
        }
        catch( err ) {
            throw new Error(`toAzureDataTables has failed to construct - ${err.message}`);
        }
    }

    _write( chunk, encoding, callback ) {
        
        try {


            let data;

            try {
                data = JSON.parse(chunk.toString());
            }
            catch( err ) {
                throw new Error(`failedconst transaction = new TableTransaction(); parsing data chunk - expected data to be a JSON described array of keyword objects`);
            }

            if(!Array.isArray(data) || !data.length || !Object.keys(data[0]).length)
                throw new Error(`expected data to be a JSON described array of keyword objects`);

            if(!data[0].hasOwnProperty('partitionKey'))
                throw new Error(`data should have its partition and row keys set prior to this module`);

            this.result = this.result.concat(data);

            callback();

        }
        catch( err ) {

            callback(new Error(`toAzureDataTables has failed - ${err.message}`));
        }
    }

    _final( callback ) {

        new Promise( async (resolve, reject) => {

            try {

                const client = this.client || new TableClient(this.tableUrl(), this.targetTable, this.credential);

                await this.createTableIfNotExists();

                const pkBins = this.result.reduce((acc, entity) => {
                    const {partitionKey} = entity;
                    if(!acc.hasOwnProperty(partitionKey))
                        acc[partitionKey] = [];
                    acc[partitionKey].push(entity);
                    return acc;
                }, {});

                let bins = [];
                Object.keys(pkBins).forEach(partitionKey => {
                    const binFeed = pkBins[partitionKey].slice();
                    while(binFeed.length) {
                        bins.push(binFeed.splice(0, 99));
                    }        
                })

                const txnStart = new Date().getTime();
                
                this.allBinsUnique(bins);

                for(var i=0; i<bins.length; i++) {
                    const bin = bins[i];
                    const txns:Array<TransactionAction> = bin.map(itm => ['create', itm]);
                    await client.submitTransaction(txns);
                }

                resolve(true);
            }
            catch( err ) {

                //this error can occur because the target table has freshly been deleted and is stuck in Azures operations queue; give in 30 seconds and go again.
                if(err.code === 'TableNotFound') {
                    
                    if(!this.retriedForOpsQueue) {
                        console.log(`toAzureDataTables recevied an error which it believes may be a freshly deleted table ${this.targetTable} - retrying in 30 seconds.`);
                        this.retriedForOpsQueue = true;
                        setTimeout(() => {
                            try {
                                this._final(callback)
                            }
                            catch( err ) {
                                reject(err);
                            }
                        }, 30000);
                        return;
                    }
                }
                return reject(err);
            }
        })
        .then(result => {
            callback();
        })
        .catch(err => {
            callback(err);
        })
    }

    async createTableIfNotExists() {

        try {
            if(this.attemptedTableCreation)
                return;

            const service = this.service || new TableServiceClient(this.tableUrl(), this.credential);
            let tablesIter = service.listTables();

            //iterate through the tables
            for await (const table of tablesIter) {
                if(table.name === this.targetTable) {
                    //found!
                    if(this.purgeIfExists) {
                        //we purge all data, and return - leaving us with an empty table
                        await this.emptyTableOfContents();
                        return;
                    }
                    else if(this.appendExistingData) {
                        //we will be appending existing data
                        return;
                    }
                    else {
                        //error off, the table exists and the overwrite flag is false and we're not appending data.
                        throw new Error(`Table ${this.targetTable} exists - either argue "overwrite":true if thats what you want.`)
                    }
                }
            }
            //nothing found, create the table.
            await service.createTable(this.targetTable);
        }
        catch( err ) {

            console.log(`toAzureDataTables.createTableIfExists has failed - ${err.message}`);
        }
    }

    //used instead of deletion which will take 30seconds to queue at table deletion.
    async emptyTableOfContents() {

        try {
            
            const {
                targetTable:table
            } = this;

            if(!table || !table.length)
                throw Error(`invalid keyword "targetTable" argued`);

            const client = this.client || new TableClient(this.tableUrl(), this.targetTable, this.credential);

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
                const { partitionKey, rowKey } = entity;
                spool[entity.partitionKey].bins[currentBinIdx].push([
                    'delete',
                    {
                        partitionKey,
                        rowKey
                    }
                ]);

            }

            try {
                for(var i=0; i < Object.keys(spool).length; i++) {
                    const partitionKey = Object.keys(spool)[i];
                    for(var ii=0; ii < spool[partitionKey].bins.length; ii++) {
                        await client.submitTransaction(spool[partitionKey].bins[ii]);
                    }
                }
            }
            catch( err ) {
                
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
        catch( err ) {

            throw Error(`toAzureDataTables::emptyTableOfContents has failed - ${err.message}`);
        }
    }

    allBinsUnique( bins ) {
        const chk = [];
        bins.forEach((bin, binIdx) => {
            bin.forEach((entity, entityIdx) => {
                const key = `${entity.partitionKey}${entity.rowKey}`;
                if(chk.includes(key))
                    throw new Error(`a duplicate partition/row key combination was found for partitionKey:${entity.partitionKey}/rowKey:${entity.rowKey} in bin #${binIdx+1}`);
                chk.push(key);
            });
        });
    }

    tableUrl() {

        if(!this.AZURE_STORAGE_ACCOUNT)
            throw new Error(`cannot resolve tableUrl - AZURE_STORAGE_ACCOUNT is not set.`);

        return `https://${this.AZURE_STORAGE_ACCOUNT}.table.core.windows.net`;
    }

    getCredential():AzureNamedKeyCredential{

        const ASA = this.AZURE_STORAGE_ACCOUNT || process.env.AZURE_STORAGE_ACCOUNT || process.env.STORAGE_ACCOUNT;
        const ASAK = this.AZURE_STORAGE_ACCOUNT_KEY || process.env.AZURE_STORAGE_ACCOUNT_KEY || process.env.STORAGE_ACCOUNT_KEY;

        if(!ASA || !ASAK)
            throw new Error(`Failed gaining azure credentials - either argue toAzureDataTables with a AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_ACCOUNT_KEY or set these as environment variables.`);

        this.AZURE_STORAGE_ACCOUNT = ASA;
        this.AZURE_STORAGE_ACCOUNT_KEY = ASAK;

        this.credential = new AzureNamedKeyCredential(ASA, ASAK);
        return this.credential;
    }

}

export const toAzureDataTables = (props={}) => new TrmAzureDataTables(props); 