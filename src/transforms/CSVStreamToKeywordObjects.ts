import { Transform } from 'stream';
import * as Papa from 'papaparse';

interface CSVStreamToKeywordObjectsProps {
    dispatchRows?:boolean;
}
/**
 * Parse a CSV stream, consequent transforms being called for each row.
 * 
 * Limitations : the first row in the CSV file MUST be the column names.
 * 
 */
export class TxCSVStreamToKeywordObjects extends Transform {

    options;
    firstChunk=true;

    delimiter;
    lineSplit;
    delimiterCount=0;
    headings;
    lastPartial;

    constructor( options:CSVStreamToKeywordObjectsProps ) {
        super();
        this.options = options;
    }

    _transform( chunk, encoding, callback ) {

        try {

            const lines = chunk.toString().split(/\r?\n/);

            if(this.firstChunk) {
                this.headings = lines[0];
                const parse = Papa.parse(this.headings);
                this.delimiter = parse.meta.delimiter;
                this.lineSplit = parse.meta.linebreak;
                this.delimiterCount = parse.data[0].length;
                this.firstChunk = false;
            }
            else {
                lines[0] = this.lastPartial+lines[0];
                lines.unshift(this.headings)
            }

            const endline = lines[lines.length-1];
            const endIsPartial = !(endline.split(this.delimiter).length === this.delimiterCount && endline.substr(-2) === "\n");
            this.lastPartial = endIsPartial ? lines.pop() : '';

            const parse = Papa.parse(lines.join(this.lineSplit), {header:true});

            if(this.options.dispatchRows) {
                parse.data.forEach(row => {
                    this.push(JSON.stringify(row));
                })
            }
            else {
                this.push(JSON.stringify(parse.data));
            }
            callback();
        }
        catch( err ) {
            callback(new Error(`CSVStreamToKeywordObjects has failed - ${err.message}`))
        }
    }
}

export const CSVStreamToKeywordObjects = (options:CSVStreamToKeywordObjectsProps = {}) => new TxCSVStreamToKeywordObjects(options);