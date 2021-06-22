import { Transform } from 'stream';

interface TxSpoolProps {
    spoolsize:number;
}

/**
 * Parse a CSV stream, consequent transforms being called for each row.
 */
export class TxSpool extends Transform {

    stack = [];
    spoolSize = 100;

    constructor( options:TxSpoolProps ) {
        super();
        this.spoolSize = options.spoolsize;
    }

    _transform( chunk, encoding, callback ) {

        try {
            this.stack.push(chunk);
            if(this.stack.length === this.spoolSize) {
                this.push(JSON.stringify(this.spoolSize));
                this.stack = [];
            }
        }
        catch( err ) {
            callback(new Error(`TxSpool has failed - ${err.message}`));     
        }
    }
}

export const spool = (options:TxSpoolProps) => new TxSpool(options);