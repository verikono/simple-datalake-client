import { Transform } from 'stream';
import { unzip } from 'zlib';

export function unzipIfZipped() {
    let isGZipped = undefined;
    return new Transform({
        objectMode: true,
        transform: ( data, encoding, done ) => {

            //using RFC 1952, 2.3.1 [ID2] - @see https://www.ietf.org/rfc/rfc1952.txt
            if(isGZipped === undefined)
                isGZipped = data[0] === 31 && data[1] === 139;

            isGZipped
                ? unzip(data, (err, unzippedData) => done(null, unzippedData))
                : done(null, data);

        }
    });
}
