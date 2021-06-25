import {
    DefaultAzureCredential
} from '@azure/identity';

import {
    DataLakeServiceClient,
    DataLakeFileSystemClient,
    DataLakeFileClient,
    DataLakeDirectoryClient
} from '@azure/storage-file-datalake';

import * as zlib from 'zlib';

import * as fs from 'fs';
import * as path from 'path';

import { pipeline } from 'stream';

import { AzureDatalakeExt } from './ext';
import { AzureDatalakeStreams } from './streams';

import * as I from './types';

export class AzureDatalakeClient {

    savePath='.';
    serviceClients:I.serviceClients = {};
    ext:AzureDatalakeExt = new AzureDatalakeExt({client: this});
    streams:AzureDatalakeStreams = new AzureDatalakeStreams({client: this});

    constructor() {

        this._isValidConfig();
    }

    /**
     * Check file/blob existence from a url
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
     * Stream a file/blob from it's URL, consuming it with callbacks a page at a time. 
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

    /**
     * Get a datalake file as a readable stream
     * 
     * @param props 
     */
    async readableStream( props:I.readStream ):Promise<fs.ReadStream> {

        const {
            url
        } = props;

        if(!await this.exists({url}))
            throw Error(`AzureDatalakeClient::readableStream received an invalid URL`);

        const client = this.getFileClient({url});
        const downloadResponse = await client.read();
        return downloadResponse.readableStreamBody as fs.ReadStream; 
    }


    /**
     * Download a file to memory/variable
     * 
     * @param props 
     */
    get( props:I.downloadProps ):Promise<string> {

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
     * @param props.url the target url for the contents
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

            throw Error(`AzureDatalakeClient::save failed - ${err.message}`);
        })


    }


    /**
     * Copy the contents at a URL to another URL
     * 
     * If the target is a .gz file and the source is not, the method will gzip compress it.
     * 
     * @param props
     * @param props.source the URL of the source file or directory (copying from source to target)
     * @param props.target the URL of the target file or directory (copying from source to target)
     * 
     * @returns Promise<boolean>
     */
    async copy( props:I.copyProps ):Promise<boolean> {

        try {

            let {
                source,
                target
            } = props;

            const isDirectory = await this._isURLDirectory(source);

            if(isDirectory) {

                source = source.substr(-1) === '/' ? source.substr(0, source.length-1) : source;
                target = target.substr(-1) === '/' ? target.substr(0, target.length-1) : target;

                const parsedSource = this._parseURL(source);
                const parsedTarget = this._parseURL(target);

                const fsclient = this.getFileSystemClient({url: parsedSource.filesystem});


                let itr= 1;
                const files = [];
                for await (const path of fsclient.listPaths({recursive: true})) {
                    if(!path.isDirectory && path.name.includes(parsedSource.file)) {
                        //trim to filename relative to url
                        const relativeSource = path.name.replace(parsedSource.file, '');

                        //listPaths also includes DELETED items(like really!?) without the option to exlude them..
                        //this we need to make sure we are copying a file which is there according to the user.
                        const sourceExists = await this.exists({url:parsedSource.url+relativeSource});
                        if(sourceExists) {
                            files.push({
                                source: parsedSource.url+relativeSource,
                                target: parsedTarget.url+relativeSource
                            });
                        }
                    }
                }

                if(files.length) {
                    await Promise.all(files.map(({source, target}) =>
                        this.copy({source, target})
                    ));
                    return true;
                }
                else {
                    throw new Error(``)
                }

            }
            else { //assumed to be a file.
                
                const gzipTarget = source.substr(-3) !== '.gz' && target.substr(-3) === '.gz'

                const sourceClient = this.getFileClient({url: source});
                const targetClient = this.getFileClient({url: target});

                await new Promise(async (resolve, reject) => {

                    await targetClient.create();
                    const readStream = await sourceClient.read();

                    const chunks = [];

                    readStream.readableStreamBody.on('data', async data => {
                        chunks.push(data);
                    });

                    readStream.readableStreamBody.on('end', async () => {

                        try {

                            let totalBuffer = Buffer.concat(chunks);
                            if(gzipTarget) {
                                totalBuffer = await new Promise((resolve, reject) => {
                                    zlib.gzip(totalBuffer, (err, result) => {
                                        if(err)
                                            return reject(err);
                                        resolve(result);
                                    });
                                });
                            }
                            await targetClient.upload(totalBuffer);
                            resolve(true);
                        }
                        catch( err ) {
                            reject(err);
                        }
                    });

                    readStream.readableStreamBody.on('error', reject);
                });
            }

            return true;
        }
        catch( err ) {

            throw Error(`AzureDatalakeClient::copy failed - ${err.message}`);
        }
    }

    /**
     * Upload data to a file/blob at a specific URL
     * 
     * @param props Object the argument object
     * @param props.url String the target URL of this file upload.
     * @param props.content String the file contents
     * @param props.overwrite Boolean overwrite existing data at the url. Default False.
     * 
     * @todo replace this flush mechanism with a ReadWriteStream as to make large uploads happy.
     */
    async put( props:I.putProps ) {

        try {

            const {
                url,
                content,
                overwrite
            } = props;

            if(!overwrite && await this.exists({url}))
                throw Error(`File exists at url - ${url}`);

            const client = this.getFileClient({url});

            await client.create();
            await client.append(content, 0, content.length);
            await client.flush(content.length);

            return true;
        }
        catch( err ) {

            if(err.code === 'AuthorizationPermissionMismatch')
                throw Error(`AzureDatalakeClient::put has failed due to a permissions issue - ${err.message}`);

            throw Error(`AzureDatalakeClient::put failed - ${err.message}`);
        }
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
     * Get a Service Client for this Azure Datalake Service
     * 
     * @param props url string the url to gain a service client for.
     * 
     * @returns DataLakeServiceClient
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

    /**
     * Get an AzureFileClient for the provided URL
     * 
     * @param props 
     * @param props.url string the url of the file/blob
     * 
     * @returns DataLakeFileClient.
     * 
     */
    getFileClient( props ):DataLakeFileClient {

        const { url } = props;
        const fileClient = new DataLakeFileClient(url, this.getCredential());
        return fileClient;

    }

    getFileSystemClient( props ):DataLakeFileSystemClient {

        const { url } = props;
        try {
            const fileSystemClient = new DataLakeFileSystemClient(url, this.getCredential());
            return fileSystemClient;
        }
        catch( err ) {
            throw new Error(`AzureDatalakeClient::getFileSystemClient failed - ${err.message}`);
        } 
    }

    getDirectoryClient( props ):DataLakeDirectoryClient {

        const { url } = props;
        try {
            const datalakeDirectoryClient = new DataLakeDirectoryClient(url, this.getCredential());
            return datalakeDirectoryClient;
        }
        catch( err ) {
            throw new Error(`AzureDatalakeClient::getDirectoryClient failed - ${err.message}`);
        }
    }

    /**
     * Get the Azure credential. Currently only gains it via environment variables.
     * 
     * @todo offer multiple authenication strategies.
     * 
     * @returns DefaultAzureCredential
     */
    getCredential():DefaultAzureCredential{

        return new DefaultAzureCredential();
    }

    /**
     * Validates we have a valid environment/configuration to transact with the azure cloud. nb. does NOT verify the credentials are correct though.
     */
    _isValidConfig() {

        try {

            if(!Boolean(process.env.AZURE_TENANT_ID))
                throw Error('invalid AZURE_TENANT_ID, set it as an environment variable');
            if(!Boolean(process.env.AZURE_CLIENT_ID))
                throw Error('invalid AZURE_CLIENT_ID, set it as an environment variable');
            if(!Boolean(process.env.AZURE_CLIENT_SECRET))
                throw Error('invalid AZURE_CLIENT_SECRET, set it as an environment variable');

        }
        catch( err ) {
            
            throw Error(`AzureDatalakeClient::isValidConfig failed - ${err}`);
        }

    }

    /**
     * Determine if a URL is a directory/folder on the Azure datalake
     * 
     * @param url string the url being evaluated
     * 
     * @returns <Promise<boolean>> true if the url is determined to actaully be a folder/directory.
     */
    async _isURLDirectory( url:string ) {

        try {

            const client = this.getDirectoryClient({url});
            const { metadata } = await client.getProperties();
            //nb true is a the string 'true'
            return metadata.hasOwnProperty('hdi_isfolder') && metadata.hdi_isfolder == 'true';
        }
        catch( err ) {
            throw new Error(`AzureDatalakeClient::_isURLDirectory has failed - ${err.message}`);
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
                file: spl.slice(4).join('/'),
                path: spl.slice(3,-1).join('/'),
                filename: spl[spl.length-1],
                filesystem: spl.splice(0, 4).join('/'),
                url
            }

        }
        catch( err ) {
            throw `AzureDatalakeClient::_parseURL failed parsing url:${url} - ${err.message}`;
        }

    }

}