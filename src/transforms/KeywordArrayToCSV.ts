import { Transform } from 'stream';
import * as Papa from 'papaparse';
import { ToDfsEndpointHostMappings } from '@azure/storage-file-datalake';

interface parserOptions {
    delimiter?:string;
    linebreak?:string;
}

interface TxKeywordArrayToCSVProps {
    parserOptions?:any;
}

export class TxKeywordArrayToCSV extends Transform {

    firstChunk=true;
    options={};
    parserOptions={};
    matches = [];

    constructor( options:TxKeywordArrayToCSVProps ) {

        super();
        this.options = options;
        this.parserOptions = options.hasOwnProperty('parserOptions') ? options.parserOptions : {};
    }

    _transform( chunk, encoding, callback ) {

        try {

            const parseOptions = Object.assign({}, this.parserOptions, {header:this.firstChunk});
            const csv = Papa.unparse(chunk.toString(), parseOptions);
            this.push(csv);
            this.firstChunk=false;
            callback();
        }
        catch( err ) {

            callback(new Error(`TxKeywrodArrayToCSV has failed - ${err.message}`));
        }

    }
}

export const keywordArrayToCSV = (options:TxKeywordArrayToCSVProps = {}) => new TxKeywordArrayToCSV(options); 