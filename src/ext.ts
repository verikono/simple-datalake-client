import { AzureDatalakeClient } from './client';
import { parseStream, parse } from '@fast-csv/parse';
import * as I from './types';
import { pipeline } from 'stream';
import { Transform } from 'stream';
import { unzip } from 'zlib';
import { unzipIfZipped } from './transforms';

export class AzureDatalakeExt {

    client:AzureDatalakeClient = null;

    constructor( props ) {

        const { client } = props;
        this.client = client;
    }

    /**
     * 
     * @param props the property object
     * @param props.url the url of the data to perform this reduce upon.
     * @param props.reducer Function called on each row
     * @param parserOptions - see https://c2fo.github.io/fast-csv/docs/parsing/options for available options such as skipping headers, or objectmode
     */
    reduce( props:I.extReduceProps, parserOptions={} ):Promise<any> {

        return new Promise( async (resolve, reject) => {
    
            const {
                url,
                reducer
            } = props;

            let { accumulator } = props, i=0;

            let stream;
            try {
                stream = await this.client.readableStream({url});
            } catch( err ){
                return reject(err);
            }

            pipeline(
                stream,
                unzipIfZipped(),
                parse(parserOptions)
                    .on('data', data => {
                        accumulator = reducer(accumulator, data, i);
                    })
                    .on('error', err => reject)
                    .on('end', () => resolve(accumulator))
                ,
                err => reject
            );

            return accumulator;
        });

    }

    /**
     * 
     * @param props the property object
     * @param props.eachRow Function called on each row
     * @param parserOptions - see https://c2fo.github.io/fast-csv/docs/parsing/options for available options such as skipping headers, or objectmode
     */
    async map( props, parserOptions={} ):Promise<Array<any>> {

        return new Promise( async (resolve, reject) => {

            const { url } = props;
            let { mapper } = props, i=0;

            let stream;
            try {
                stream = await this.client.readableStream({url});
            } catch( err ){
                return reject(err);
            }

            let promises = [];

            try {

                pipeline(
                    stream,
                    unzipIfZipped(),
                    parse(parserOptions)
                        .on('data', data => {
                            promises.push(mapper(data, i));
                            i++;
                        })
                        .on('error', reject)
                        .on('end', async () => {
                            const result = await Promise.all(promises)
                            resolve(result);
                        })
                    ,
                    err => reject
                );
            }
            catch( err ) {
                return reject(err);
            }

        });
    }

    /**
     * Iterate a CSV datafile, optionally blocking I/O (default true)
     * 
     * @param props the argument object
     * @param props.url String - the url of the file
     * @param props.fn Function - the function to call for each row
     * @param props.block Boolean - wait until all rows have finished, default true
     * @param parserOptions -see https://c2fo.github.io/fast-csv/docs/parsing/options for available options such as skipping headers, or objectmode
     */
    forEach( props, parserOptions={} ):Promise<void> {

        return new Promise( async (resolve, reject) => {

            const {
                url,
                fn
            } = props;

            let { block } = props,
                i=0,
                promises=[];

            block = block === undefined ? true : block;

            const finalize = () => resolve();

            let stream;
            try {
                stream = await this.client.readableStream({url});
            } catch( err ){
                return reject(err);
            }

            try {
            
                pipeline(
                    stream,
                    unzipIfZipped(),
                    parse(parserOptions)
                        .on('data', data => {
                            promises.push(fn(data, i));
                            i++;
                        })
                        .on('error', reject)
                        .on('end', async () => {
                            if(block) {
                                await Promise.all(promises);
                                finalize();
                            }
                        })
                    ,
                    err => reject
                )                
            }
            catch( err ) {
                return reject(err);
            }
            
            if(!block)
                finalize();
                
        });
    }

    /**
     * Get the number of rows in this datafile
     * @param props 
     */
    async count( props, parserOptions={} ) {

        return new Promise( async (resolve, reject) => {

            const { url } = props;

            let stream, i=0;
            try {
                stream = await this.client.readableStream({url});
            } catch( err ){
                return reject(err);
            }

            parseStream(stream, parserOptions)
                .on('data', data => i++)
                .on('error', reject)
                .on('end', () => resolve(i));

        });

    }

}