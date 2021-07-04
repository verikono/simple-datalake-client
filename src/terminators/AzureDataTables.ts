import {Writable} from 'stream';
import {
    TableServiceClient,
    AzureNamedKeyCredential,
    TableTransaction,
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
                throw new Error(`failed parsing data chunk - expected data to be a JSON described array of keyword objects`);
            }

            if(!Array.isArray(data) || !data.length || !Object.keys(data[0]).length)
                throw new Error(`expected data to be a JSON described array of keyword objects`);

            if(!data[0].hasOwnProperty('partitionKey'))
                throw new Error(`data should have its partition and row keys set prior to this module`);

            const client = this.client || new TableClient(this.tableUrl(), this.targetTable, this.credential);

            new Promise( async (resolve, reject) => {

                try {
                    await this.createTableIfNotExists();
                    const txns = data.map(itm => ['create', itm]);
                    await client.submitTransaction(txns);
                    resolve(true);
                }
                catch( err ) {
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
        catch( err ) {
            callback(new Error(`toAzureDataTables has failed - ${err.message}`));
        }
    }

    async createTableIfNotExists() {

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

    tableUrl() {

        if(!this.AZURE_STORAGE_ACCOUNT)
            throw new Error(`cannot resolve tableUrl - AZURE_STORAGE_ACCOUNT is not set.`);

        return `https://${this.AZURE_STORAGE_ACCOUNT}.table.core.windows.net`;
    }

    _final( callback ) {
        callback();
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