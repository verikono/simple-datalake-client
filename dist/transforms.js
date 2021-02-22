"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.unzipIfZipped = void 0;
const stream_1 = require("stream");
const zlib_1 = require("zlib");
function unzipIfZipped() {
    let isGZipped = undefined;
    return new stream_1.Transform({
        objectMode: true,
        transform: (data, encoding, done) => {
            //using RFC 1952, 2.3.1 [ID2] - @see https://www.ietf.org/rfc/rfc1952.txt
            if (isGZipped === undefined)
                isGZipped = data[0] === 31 && data[1] === 139;
            isGZipped
                ? zlib_1.unzip(data, (err, unzippedData) => done(null, unzippedData))
                : done(null, data);
        }
    });
}
exports.unzipIfZipped = unzipIfZipped;
//# sourceMappingURL=transforms.js.map