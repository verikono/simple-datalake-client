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
     * @param props.url the target url for the contents
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
     * Copy the contents at a URL to another URL
     *
     * @param props
     * @param props.source the URL of the source file or directory (copying from source to target)
     * @param props.target the URL of the target file or directory (copying from source to target)
     *
     * @returns Promise<boolean>
     */
    copy(props) {
        var e_1, _a;
        return __awaiter(this, void 0, void 0, function* () {
            try {
                let { source, target } = props;
                const isDirectory = yield this._isURLDirectory(source);
                if (isDirectory) {
                    source = source.substr(-1) === '/' ? source.substr(0, source.length - 1) : source;
                    target = target.substr(-1) === '/' ? target.substr(0, target.length - 1) : target;
                    const parsedSource = this._parseURL(source);
                    const parsedTarget = this._parseURL(target);
                    const fsclient = this.getFileSystemClient({ url: parsedSource.filesystem });
                    let itr = 1;
                    const files = [];
                    try {
                        for (var _b = __asyncValues(fsclient.listPaths({ recursive: true })), _c; _c = yield _b.next(), !_c.done;) {
                            const path = _c.value;
                            if (!path.isDirectory && path.name.includes(parsedSource.file)) {
                                //trim to filename relative to url
                                const relativeSource = path.name.replace(parsedSource.file, '');
                                //listPaths also includes DELETED items(like really!?) without the option to exlude them..
                                //this we need to make sure we are copying a file which is there according to the user.
                                const sourceExists = yield this.exists({ url: parsedSource.url + relativeSource });
                                if (sourceExists) {
                                    files.push({
                                        source: parsedSource.url + relativeSource,
                                        target: parsedTarget.url + relativeSource
                                    });
                                }
                            }
                        }
                    }
                    catch (e_1_1) { e_1 = { error: e_1_1 }; }
                    finally {
                        try {
                            if (_c && !_c.done && (_a = _b.return)) yield _a.call(_b);
                        }
                        finally { if (e_1) throw e_1.error; }
                    }
                    if (files.length) {
                        yield Promise.all(files.map(({ source, target }) => this.copy({ source, target })));
                        return true;
                    }
                    else {
                        throw new Error(``);
                    }
                }
                else { //assumed to be a file.
                    const sourceClient = this.getFileClient({ url: source });
                    const targetClient = this.getFileClient({ url: target });
                    yield new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
                        yield targetClient.create();
                        const readStream = yield sourceClient.read();
                        const chunks = [];
                        readStream.readableStreamBody.on('data', (data) => __awaiter(this, void 0, void 0, function* () {
                            chunks.push(data);
                        }));
                        readStream.readableStreamBody.on('end', () => __awaiter(this, void 0, void 0, function* () {
                            try {
                                const totalBuffer = Buffer.concat(chunks);
                                yield targetClient.upload(totalBuffer);
                                resolve(true);
                            }
                            catch (err) {
                                reject(err);
                            }
                        }));
                        readStream.readableStreamBody.on('error', reject);
                    }));
                }
                return true;
            }
            catch (err) {
                throw Error(`AzureDatalakeClient::copy failed - ${err.message}`);
            }
        });
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
    put(props) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const { url, content, overwrite } = props;
                if (!overwrite && (yield this.exists({ url })))
                    throw Error(`File exists at url - ${url}`);
                const client = this.getFileClient({ url });
                yield client.create();
                yield client.append(content, 0, content.length);
                yield client.flush(content.length);
                return true;
            }
            catch (err) {
                if (err.code === 'AuthorizationPermissionMismatch')
                    throw Error(`AzureDatalakeClient::put has failed due to a permissions issue - ${err.message}`);
                throw Error(`AzureDatalakeClient::put failed - ${err.message}`);
            }
        });
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
    getFileSystemClient(props) {
        const { url } = props;
        try {
            const fileSystemClient = new storage_file_datalake_1.DataLakeFileSystemClient(url, this.getCredential());
            return fileSystemClient;
        }
        catch (err) {
            throw new Error(`AzureDatalakeClient::getFileSystemClient failed - ${err.message}`);
        }
    }
    getDirectoryClient(props) {
        const { url } = props;
        try {
            const datalakeDirectoryClient = new storage_file_datalake_1.DataLakeDirectoryClient(url, this.getCredential());
            return datalakeDirectoryClient;
        }
        catch (err) {
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
     * Determine if a URL is a directory/folder on the Azure datalake
     *
     * @param url string the url being evaluated
     *
     * @returns <Promise<boolean>> true if the url is determined to actaully be a folder/directory.
     */
    _isURLDirectory(url) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const client = this.getDirectoryClient({ url });
                const { metadata } = yield client.getProperties();
                //nb true is a the string 'true'
                return metadata.hasOwnProperty('hdi_isfolder') && metadata.hdi_isfolder == 'true';
            }
            catch (err) {
                throw new Error(`AzureDatalakeClient::_isURLDirectory has failed - ${err.message}`);
            }
        });
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
                file: spl.slice(4).join('/'),
                path: spl.slice(3, -1).join('/'),
                filename: spl[spl.length - 1],
                filesystem: spl.splice(0, 4).join('/'),
                url
            };
        }
        catch (err) {
            throw `AzureDatalakeClient::_parseURL failed parsing url:${url} - ${err.message}`;
        }
    }
}
exports.AzureDatalakeClient = AzureDatalakeClient;
//# sourceMappingURL=client.js.map