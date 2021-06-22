import { Transform } from 'stream';
import * as Papa from 'papaparse';

interface TxKeywordArrayToCSVProps {
    delimiter?:string;
}

export class TxKeywordArrayToCSV extends Transform {

    firstChunk=true;
    parseOptions={};
    matches = [];

    constructor( options:TxKeywordArrayToCSVProps ) {

        super();
        if(options.delimiter)
            this.parseOptions['delimiter'] = options.delimiter;
    }

    _transform( chunk, encoding, callback ) {

        try {

            const parseOptions = Object.assign({}, this.parseOptions, {header:this.firstChunk});
            const csv = Papa.unparse(chunk.toString(), parseOptions);

            // const p = JSON.parse(chunk.toString());
            // p.forEach(obj => {
            //     const conflict = this.matches.find(m => m === obj.promo_id)
            //     if(conflict) {
            //         console.log('#######');
            //     }
            //     this.matches.push(obj.promo_id);
            // })
            // console.log('chunk');
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