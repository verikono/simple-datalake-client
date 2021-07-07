import { Transform } from 'stream';
import * as Papa from 'papaparse';
import { ToDfsEndpointHostMappings } from '@azure/storage-file-datalake';

interface TxKeywordArrayToCSVProps {
    delimiter?:string;
}

export class TxKeywordArrayToCSV extends Transform {

    firstChunk=true;
    options={};
    parserOptions={};
    matches = [];

    constructor( options:TxKeywordArrayToCSVProps, parserOptions={} ) {

        super();
        this.options = options;
        this.parserOptions = parserOptions;
    }

    _transform( chunk, encoding, callback ) {

        try {

            if(this.options['delimiter'])
                this.parserOptions['delimiter'] = this.options['delimiter'];

                const parseOptions = Object.assign({}, this.parserOptions, {header:this.firstChunk});
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