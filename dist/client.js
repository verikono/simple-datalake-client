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
exports.AzureDatalakeClient = void 0;
const identity_1 = require("@azure/identity");
const storage_file_datalake_1 = require("@azure/storage-file-datalake");
const fs = __importStar(require("fs"));
const path = __importStar(require("path"));
const stream_1 = require("stream");
const ext_1 = require("./ext");
const streams_1 = require("./streams");
class AzureDatalakeClient {
    constructor() {
        this.savePath = '.';
        this.serviceClients = {};
        this.ext = new ext_1.AzureDatalakeExt({ client: this });
        this.streams = new streams_1.AzureDatalakeStreams({ client: this });
        this._isValidConfig();
    }
    /**
     * Check file/blob existence from a url
     *
     * @param props
     * @param props.url string the url of the file to check
     */
    exists(props) {
        return __awaiter(this, void 0, void 0, function* () {
            const { url } = props;
            const client = this.getFileClient({ url });
            return client.exists();
        });
    }
    /**
     * Stream a file/blob from it's URL, consuming it with callbacks a page at a time.
     *
     * @param props the argument object
     * @param props.onData Function invoked upon receipt of a chunk
     * @param props.onEnd Function invoked upon conclusion of this stream
     * @param props.onError Function invoked upon error during stream
     */
    stream(props) {
        return __awaiter(this, void 0, void 0, function* () {
            const { url, onData, onEnd, onError } = props;
            function streamToBuffer(readableStream) {
                return new Promise((resolve, reject) => {
                    readableStream.on('data', onData),
                        readableStream.on('end', _ => {
                            typeof onEnd === 'function' ? onEnd() : null;
                            resolve(true);
                        });
                    readableStream.on('error', reject);
                });
            }
            const client = this.getFileClient({ url });
            const downloadResponse = yield client.read();
            const result = yield streamToBuffer(downloadResponse.readableStreamBody);
            return true;
        });
    }
    /**
     * Get a datalake file as a readable stream
     *
     * @param props
     */
    readableStream(props) {
        return __awaiter(this, void 0, void 0, function* () {
            const { url } = props;
            if (!(yield this.exists({ url })))
                throw Error(`AzureDatalakeClient::readableStream received an invalid URL`);
            const client = this.getFileClient({ url });
            const downloadResponse = yield client.read();
            return downloadResponse.readableStreamBody;
        });
    }
    /**
     * Download a file to memory/variable
     *
     * @param props
     */
    get(props) {
        return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
            const { url } = props;
            const chunks = [];
            yield this.stream({
                url,
                onData: data => chunks.push(data instanceof Buffer ? data : Buffer.from(data)),
                onEnd: _ => resolve(Buffer.concat(chunks).toString()),
                onError: err => reject(err)
            });
        }));
    }
    /**
     * Save a file to local storage
     *
     * @param props the argument object
     * @param props.file filepath relative to the runtime root, default is the filename at project root.
     *
     * @returns Promise<boolean>
     */
    save(props) {
        return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
            const { url } = props;
            let { file } = props;
            file = file || this._parseURL(url).file;
            const fileparts = path.parse(path.join(this.savePath, file));
            if (fileparts.dir.length) {
                yield fs.promises.mkdir(fileparts.dir, { recursive: true })
                    .catch(err => reject);
            }
            const client = this.getFileClient({ url });
            const download = yield client.read();
            const fileStream = fs.createWriteStream(path.join(fileparts.dir, fileparts.name + fileparts.ext));
            stream_1.pipeline(download.readableStreamBody, fileStream, err => {
                err ? reject(err) : resolve(true);
            });
        }))
            .catch(err => {
            throw Error(`AzureDatalakeClient::save failed - ${err.message}`);
        });
    }
    /**
     * Upload a data to a file/blob at a specific URL
     *
     * @param props the argument object
     * @param props.url the target URL of this file upload.
     */
    put(props) {
        const { url } = props;
    }
    /**
     * Set a path as the root directory for all uploads and downloads.
     *
     * @param props
     * @param props.path the path relative to the project root.
     *
     * @returns Void
     */
    setSaveRoot(props) {
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
    getServiceClient(props) {
        const { url } = props;
        const { hostURL } = this._parseURL(url);
        if (!this.serviceClients[hostURL]) {
            this.serviceClients[hostURL] = new storage_file_datalake_1.DataLakeServiceClient(hostURL, this.getCredential());
        }
        return this.serviceClients[hostURL];
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
    getFileClient(props) {
        const { url } = props;
        const fileClient = new storage_file_datalake_1.DataLakeFileClient(url, this.getCredential());
        return fileClient;
    }
    /**
     * Get the Azure credential. Currently only gains it via environment variables.
     *
     * @todo offer multiple authenication strategies.
     *
     * @returns DefaultAzureCredential
     */
    getCredential() {
        return new identity_1.DefaultAzureCredential();
    }
    /**
     * Validates we have a valid environment/configuration to transact with the azure cloud. nb. does NOT verify the credentials are correct though.
     */
    _isValidConfig() {
        try {
            if (!Boolean(process.env.AZURE_TENANT_ID))
                throw Error('invalid AZURE_TENANT_ID, set it as an environment variable');
            if (!Boolean(process.env.AZURE_CLIENT_ID))
                throw Error('invalid AZURE_CLIENT_ID, set it as an environment variable');
            if (!Boolean(process.env.AZURE_CLIENT_SECRET))
                throw Error('invalid AZURE_CLIENT_SECRET, set it as an environment variable');
        }
        catch (err) {
            throw Error(`AzureDatalakeClient::isValidConfig failed - ${err}`);
        }
    }
    /**
     * From an AZURE** url, parse to a set of useful values properties
     *
     * @param url string the AZURE url such as one extracted from azure storage explorer.
     *
     * @returns parsedURL
     */
    _parseURL(url) {
        try {
            const spl = url.split('/');
            return {
                protocol: spl[0].substring(0, spl[0].length - 1),
                host: spl[2],
                storageType: spl[2].split('.')[1],
                hostURL: spl.slice(0, 3).join('/'),
                account: spl[2].split('.')[0],
                file: spl.slice(3).join('/'),
                path: spl.slice(3, -1).join('/'),
                filename: spl[spl.length - 1]
            };
        }
        catch (err) {
            throw `AzureDatalakeClient::_parseURL failed parsing url:${url} - ${err.message}`;
        }
    }
}
exports.AzureDatalakeClient = AzureDatalakeClient;
//# sourceMappingURL=client.js.map