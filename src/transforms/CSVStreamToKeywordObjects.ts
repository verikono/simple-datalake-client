import { Transform } from 'stream';
import * as Papa from 'papaparse';
import { isEmptyObject } from './util';

interface CSVStreamToKeywordObjectsProps {
    dispatchRows?:boolean;
    report?:any; //an empty object which can be used to gain information durying process. eg. the delimiter that wa used.
    onFirstChunk?:Function
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
    onFirstChunk;

    delimiter;
    lineSplit;
    delimiterCount=0;
    headings;
    lastPartial;
    report;

    constructor( options:CSVStreamToKeywordObjectsProps ) {

        super();

        try{

            this.options = options;

            if(options.report) {
                if(!isEmptyObject(options.report))
                    throw new Error(`constructor argument "report" should be an empty object for this transformer to populate with processing information`);
                this.report = options.report;
            }

            if(options.onFirstChunk)
                this.onFirstChunk = options.onFirstChunk
        }
        catch( err ) {

            throw new Error(`TxCSVStreamToKeywordObjects has failed to construct - ${err.message}`);
        }
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
                if(this.report)
                    this.report.parse = parse.meta;
                if(this.onFirstChunk)
                    this.onFirstChunk(parse.meta)
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