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

export async function fromAzureDatalake( options ) {

    const {
        url
    } = options;

    const client = new DataLakeFileClient(url, new DefaultAzureCredential());
    const downloadResponse = await client.read();
    let stream = downloadResponse.readableStreamBody;
    const zipped = url.substr(-2) === 'gz';
    if(zipped)
        stream = stream.pipe(zlib.createGunzip())
    return stream;
}

interface toAzureDataLakeOptions{ url: string; spoolsize?: number; }
export async function toAzureDatalake( options:toAzureDataLakeOptions ) {

    const {
        url
    } = options;

    class ToAzureDataLake extends Writable {

        url;
        client;
        spool = [];
        cnt = 0;
        spoolsize;
        offset=0;
        chunks = [];
        content = '';
        constructor( options:toAzureDataLakeOptions ) {

            super();
            this.url = options.url;
            this.spoolsize = options.spoolsize || 100;
        }

        async connect() {

            try {

                this.client = new DataLakeFileClient(this.url, new DefaultAzureCredential());
                await this.client.create();
            }
            catch( err ) {

                console.log('-')
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

                const content = this.chunks.join('\n\r');
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