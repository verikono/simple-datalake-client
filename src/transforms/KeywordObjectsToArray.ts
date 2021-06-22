import { Transform } from 'stream';

interface TxSpoolProps {
    spoolsize?:number;
}

/**
 * Parse a CSV stream, consequent transforms being called for each row.
 */
export class TxKeywordObjectsToArray extends Transform {

    stack = [];
    spoolSize:number;

    constructor( options:TxSpoolProps ) {
        super();
        this.spoolSize = options.spoolsize || 100;
    }

    _transform( chunk, encoding, callback ) {

        try {
 
            let obj;
 
            try {
                obj = JSON.parse(chunk.toString());
            }
            catch( err ) {
                throw new Error(`Failed parsing an inbound chunk >> ${chunk.toString()} <<`)
            }

            this.stack.push(obj);

            if(this.stack.length === this.spoolSize) {
                this.push(JSON.stringify(this.stack));
                this.stack = [];
            }
            callback();
        }
        catch( err ) {
            callback(new Error(`TxSpool has failed - ${err.message}`));     
        }
    }
}

export const keywordObjectsToArray = (options:TxSpoolProps= {}) => new TxKeywordObjectsToArray(options);