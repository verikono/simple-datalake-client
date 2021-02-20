import { AzureDatalakeClient } from './client';
/**
 * A Streams manager for loading/storing and manipulating Datalake Based assets
 * 
 * @todo implement.
 * 
 */
export class AzureDatalakeStreams {

    client: AzureDatalakeClient = null;

    constructor( props ) {

        const { client } = props;
        this.client = client;
    }

    async process() {

    }
}