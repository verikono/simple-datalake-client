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
exports.toAzureDatalake = exports.fromAzureDatalake = void 0;
const storage_file_datalake_1 = require("@azure/storage-file-datalake");
const identity_1 = require("@azure/identity");
const stream_1 = require("stream");
const zlib = __importStar(require("zlib"));
function fromAzureDatalake(options) {
    return __awaiter(this, void 0, void 0, function* () {
        const { url } = options;
        const client = new storage_file_datalake_1.DataLakeFileClient(url, new identity_1.DefaultAzureCredential());
        const downloadResponse = yield client.read();
        let stream = downloadResponse.readableStreamBody;
        const zipped = url.substr(-2) === 'gz';
        if (zipped)
            stream = stream.pipe(zlib.createGunzip());
        return stream;
    });
}
exports.fromAzureDatalake = fromAzureDatalake;
function toAzureDatalake(options) {
    return __awaiter(this, void 0, void 0, function* () {
        const { url } = options;
        class ToAzureDataLake extends stream_1.Writable {
            constructor(options) {
                super();
                this.spool = [];
                this.cnt = 0;
                this.offset = 0;
                this.chunks = [];
                this.content = '';
                this.url = options.url;
                this.spoolsize = options.spoolsize || 100;
            }
            connect() {
                return __awaiter(this, void 0, void 0, function* () {
                    try {
                        this.client = new storage_file_datalake_1.DataLakeFileClient(this.url, new identity_1.DefaultAzureCredential());
                        yield this.client.create();
                    }
                    catch (err) {
                        console.log('-');
                    }
                });
            }
            _write(chunk, encoding, callback) {
                try {
                    this.chunks.push(chunk.toString());
                    callback();
                }
                catch (err) {
                    callback(new Error(`Loader toAzureDataLake has failed preparing its output during process - ${err.message}`));
                }
            }
            _final(callback) {
                try {
                    const content = this.chunks.join('\n\r');
                    const zipped = this.url.substr(-2) === 'gz';
                    if (zipped) {
                        zlib.gzip(content, (err, result) => {
                            if (err)
                                return callback(new Error(`ToAzureDataLake has failed compressing the output - ${err.message}`));
                            this.client.append(result, 0, result.length)
                                .then(resp => {
                                this.client.flush(result.length)
                                    .then(resp => callback())
                                    .catch(err => callback(new Error(`ToAzureDataLake has failed flushing the output to the datalake - ${err.message}`)));
                            })
                                .catch(err => callback(new Error(`ToAzureDataLake has failed uploading the output to the datalake - ${err.message}`)));
                        });
                    }
                    else {
                        this.client.append(content, 0, content.length)
                            .then(resp => {
                            this.client.flush(content.length)
                                .then(resp => callback())
                                .catch(err => callback(new Error(`ToAzureDataLake has failed flushing the output to the datalake - ${err.message}`)));
                        });
                    }
                }
                catch (err) {
                    callback(err);
                }
            }
        }
        const instance = new ToAzureDataLake(options);
        yield instance.connect();
        return instance;
    });
}
exports.toAzureDatalake = toAzureDatalake;
//# sourceMappingURL=Azure.js.map