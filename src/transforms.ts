import { Transform } from 'stream';
import { unzip } from 'zlib';


//this needs better approach, will straight up not work for multi-chunk files..
export function unzipIfZipped() {
    return new Transform({
        objectMode: true,
        transform: ( data, encoding, done ) => {

            //using RFC 1952, 2.3.1 [ID2] - @see https://www.ietf.org/rfc/rfc1952.txt
            data[0] === 31 && data[1] === 139
                ? unzip(data, (err, unzippedData) => done(null, unzippedData))
                : done(null, data);

        }
    });
}
