import {
    DefaultAzureCredential
} from '@azure/identity';

import {
    DataLakeServiceClient,
    DataLakeFileClient
} from '@azure/storage-file-datalake';

import * as fs from 'fs';
import * as path from 'path';
import { pipeline } from 'stream';

import { AzureDatalakeExt } from './ext';

import * as I from './types';

export class AzureDatalakeClient {

    savePath='.';
    serviceClients:I.serviceClients = {};
    ext:AzureDatalakeExt = new AzureDatalakeExt({client: this});

    constructor() {

        this._isValidConfig();
    }

    /**
     * Check file existence from a url
     * 
     * @param props
     * @param props.url string the url of the file to check 
     */
    async exists( props:I.existsProps ):Promise<boolean> {

        const { url } = props;
        const client = this.getFileClient({url})
        return client.exists();
    }

    /**
     * Stream a file from it's URL
     * 
     * @param props the argument object
     * @param props.onData Function invoked upon receipt of a chunk
     * @param props.onEnd Function invoked upon conclusion of this stream
     * @param props.onError Function invoked upon error during stream
     */
    async stream( props:I.streamProps ):Promise<boolean> {

        const {
            url,
            onData,
            onEnd,
            onError
        } = props;

        function streamToBuffer( readableStream ) {

            return new Promise((resolve, reject) => {
                readableStream.on('data', onData),
                readableStream.on('end', _ => {
                    typeof onEnd ===  'function' ? onEnd() : null;
                    resolve(true);
                });
                readableStream.on('error', reject);
            });
        }

        const client = this.getFileClient({url});
        const downloadResponse = await client.read();
        const result = await streamToBuffer(downloadResponse.readableStreamBody);

        return true;
    }

    async readableStream( props:I.readStream ):Promise<fs.ReadStream> {

        const {
            url
        } = props;

        if(!await this.exists({url}))
            throw `AzureDatalakeClient::readableStream received an invalid URL`;

        const client = this.getFileClient({url});
        const downloadResponse = await client.read();
        return downloadResponse.readableStreamBody as fs.ReadStream; 
    }

    /**
     * Download a file to memory/variable
     * 
     * @param props 
     */
    download( props:I.downloadProps ):Promise<string> {

        return new Promise( async (resolve, reject) => {

            const { url } = props;
            const chunks = [];
            await this.stream({
                url,
                onData: data => chunks.push(data instanceof Buffer ? data : Buffer.from(data)),
                onEnd: _ => resolve(Buffer.concat(chunks).toString()),
                onError: err => reject(err)
            });
        });
    }

    /**
     * Save a file to local storage
     * 
     * @param props the argument object
     * @param props.file filepath relative to the runtime root, default is the filename at project root.
     * 
     * @returns Promise<boolean>
     */
    save( props:I.saveProps ):Promise<boolean> {

        return new Promise<boolean>(  async (resolve, reject) => {

            const { url } = props;

            let { file } = props;
            file = file || this._parseURL(url).file;

            const fileparts = path.parse(path.join(this.savePath, file));
            if(fileparts.dir.length) {
                await fs.promises.mkdir(fileparts.dir, {recursive: true})
                        .catch( err => reject);
            }

            const client = this.getFileClient({url});
            const download = await client.read();
            const fileStream = fs.createWriteStream(path.join(fileparts.dir, fileparts.name+fileparts.ext));

            pipeline(
                download.readableStreamBody,
                fileStream,
                err => {
                    err ? reject(err) : resolve(true)
                }
            );
        })
        .catch( err => {

            throw `AzureDatalakeClient::save failed - ${err.message}`;
        })


    }

    /**
     * Upload a file to a specific URL
     * 
     * @param props the argument object
     * @param props.url the target URL of this file upload.
     */
    upload( props:I.uploadProps ) {

        const {url} = props;

    }

    /**
     * Set a path as the root directory for all uploads and downloads.
     *  
     * @param props 
     * @param props.path the path relative to the project root.
     * 
     * @returns Void
     */
    setSaveRoot( props ) {

        const { path } = props;
        this.savePath = path;
    }

    /**
     * 
     * @param props url string the url to gain a service client for.
     */
    getServiceClient( props:I.getServiceClientProps ):DataLakeServiceClient {

        const { url } = props;
        const { hostURL } = this._parseURL(url);

        if(!this.serviceClients[hostURL]) {
            this.serviceClients[hostURL] = new DataLakeServiceClient(
                hostURL,
                this.getCredential()
            )
        }

        return this.serviceClients[hostURL]        
    }

    getFileClient( props ):DataLakeFileClient {

        const { url } = props;
        const fileClient = new DataLakeFileClient(url, this.getCredential());
        return fileClient;

    }

    getCredential():DefaultAzureCredential{

        return new DefaultAzureCredential();
    }

    _isValidConfig() {

        try {

            if(!Boolean(process.env.AZURE_TENANT_ID)) throw 'invalid AZURE_TENANT_ID, set it as an environment variable';
            if(!Boolean(process.env.AZURE_CLIENT_ID)) throw 'invalid AZURE_CLIENT_ID, set it as an environment variable';
            if(!Boolean(process.env.AZURE_CLIENT_SECRET)) throw 'invalid AZURE_CLIENT_SECRET, set it as an environment variable';

        }
        catch( err ) {
            
            throw `AzureDatalakeClient::isValidConfig failed - ${err}`;
        }

    }

    /**
     * From an AZURE** url, parse to a set of useful values properties
     * 
     * @param url string the AZURE url such as one extracted from azure storage explorer.
     * 
     * @returns parsedURL
     */
    _parseURL( url:string ):I.parsedURL {

        try {

            const spl = url.split('/');

            return {
                protocol: spl[0].substring(0,spl[0].length-1) as I.protocol,
                host: spl[2],
                storageType: spl[2].split('.')[1],
                hostURL: spl.slice(0, 3).join('/'),
                account: spl[2].split('.')[0],
                file: spl.slice(3).join('/'),
                path: spl.slice(3,-1).join('/'),
                filename: spl[spl.length-1]
            }

        }
        catch( err ) {
            throw `AzureDatalakeClient::_parseURL failed parsing url:${url} - ${err.message}`;
        }

    }

}