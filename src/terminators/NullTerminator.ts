import {
    Writable
} from 'stream';

export class TrmNullTerminator extends Writable {

    _write( chunk, encoding, callback ) {
        
        callback();
    }

    _final( callback ) {
        callback();
    }

}

export const nullTerminator = () => new TrmNullTerminator(); 