import {
    DataLakeServiceClient,
    DataLakeFileClient
} from '@azure/storage-file-datalake';

import {
    DefaultAzureCredential
} from '@azure/identity';

import {
    Writable
} from 'stream';

import * as zlib from 'zlib';

/**
 * Load data to a stream from the Azure Datalake Gen2
 * 
 * @param options the keyword argument object
 * @param options.url String the Datalake URL 
 * @param options.report Object an empty object which this loader will write output meta to.
 * @returns 
 */
export async function fromAzureDatalake( options ) {

    const {
        url
    } = options;

    const reporter = options.reporter || {};

    const client = new DataLakeFileClient(url, new DefaultAzureCredential());
    const downloadResponse = await client.read();
    let stream = downloadResponse.readableStreamBody;
    const zipped = url.substr(-2) === 'gz';
    reporter.sourceIsGzipped = true;
    if(zipped)
        stream = stream.pipe(zlib.createGunzip())
    return stream;
}

interface toAzureDataLakeOptions{ url: string; replace?: boolean, parserOptions?:any }
export async function toAzureDatalake( options:toAzureDataLakeOptions ) {

    const {
        url
    } = options;

    class ToAzureDataLake extends Writable {

        url;
        client;
        spool = [];
        cnt = 0;
        offset=0;
        chunks = [];
        content = '';
        okToReplace = false;
        parserOptions = {
            linebreak:null
        };

        constructor( options:toAzureDataLakeOptions ) {

            super();
            this.url = options.url;
            this.okToReplace = options.replace === undefined ? false : options.replace;
            if(options.parserOptions)
                this.parserOptions = options.parserOptions;
        }

        async connect() {

            try {

                this.client = new DataLakeFileClient(this.url, new DefaultAzureCredential());

                if(await this.client.exists(this.url)) {
                    if(!this.okToReplace) {
                        throw new Error(`data exists at at ${this.url} : Argue {replace:true} if this is ok.`);
                    }
                    await this.client.delete();
                }

                await this.client.create();
                


            }
            catch( err ) {

                throw new Error(`ToAzureDataLake has failed building the datalake client - ${err.message}`);
            }
        }

        _write( chunk, encoding, callback ) {
            
            try {

                this.chunks.push(chunk.toString());
                callback();
                
            }
            catch( err ) {

                callback(new Error(`Loader toAzureDataLake has failed preparing its output during process - ${err.message}`));
            }

        }

        _final( callback ) {

            try {

                const content = this.chunks.join(this.parserOptions.linebreak || '\n\r');
                const zipped = this.url.substr(-2) === 'gz';

                if(zipped) {
                    zlib.gzip(content, (err, result) => {
                        if(err)
                            return callback(new Error(`ToAzureDataLake has failed compressing the output - ${err.message}`));
                        this.client.append(result, 0, result.length)
                            .then( resp => {
                                this.client.flush(result.length)
                                    .then( resp => callback())
                                    .catch( err =>
                                        callback(new Error(`ToAzureDataLake has failed flushing the output to the datalake - ${err.message}`)))
                            })
                            .catch( err =>
                                callback(new Error(`ToAzureDataLake has failed uploading the output to the datalake - ${err.message}`)));
                    })
                }
                else {

                    this.client.append(content, 0, content.length)
                        .then( resp => {
                            this.client.flush(content.length)
                                .then( resp => callback())
                                .catch( err =>
                                    callback(new Error(`ToAzureDataLake has failed flushing the output to the datalake - ${err.message}`)))
                        });

                }

            }
            catch( err ) {

                callback(err);
            }
        }

    }

    const instance = new ToAzureDataLake(options);
    await instance.connect();
    return instance;
}

