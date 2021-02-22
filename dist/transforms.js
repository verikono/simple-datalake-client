"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.unzipIfZipped = void 0;
const stream_1 = require("stream");
const zlib_1 = require("zlib");
//this needs better approach, will straight up not work for multi-chunk files..
function unzipIfZipped() {
    return new stream_1.Transform({
        objectMode: true,
        transform: (data, encoding, done) => {
            //using RFC 1952, 2.3.1 [ID2] - @see https://www.ietf.org/rfc/rfc1952.txt
            data[0] === 31 && data[1] === 139
                ? zlib_1.unzip(data, (err, unzippedData) => done(null, unzippedData))
                : done(null, data);
        }
    });
}
exports.unzipIfZipped = unzipIfZipped;
//# sourceMappingURL=transforms.js.map