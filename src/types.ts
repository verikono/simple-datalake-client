import {
    DataLakeServiceClient
} from '@azure/storage-file-datalake';
import {ParserOptionsArgs} from '@fast-csv/parse';

export type protocol = 'http'|'https'
export interface serviceClients {
    [key:string]:DataLakeServiceClient;
}

/**
 * Method types
 */

//example input: https://myaccount.dfs.windows.core.net/path/to/myfile.txt
export interface parsedURL {

    //protocol, either http or https (from example: https)
    protocol: protocol;

    //the account name, azure storage account (from example: myaccount)
    account: string;

    //the storage mechanism such as dfs (datalake), blob, file etc (from example: blob)
    storageType: string;

    //the hostname of the azure storage account (from example: myacount.dfs.windows.core.net)
    host: string;

    //the host as a url (from example: https://myacount.dfs.windows.core.net)
    hostURL: string;

    //the directory for this file (from example: path/to)
    path:string;

    //the full filepath (from example: path/to/myfile.txt)
    file:string;

    //the filename (from example: myfile.txt)
    filename:string

    //the url of the filesystem
    filesystem:string;

    //the url as provided.
    url:string;

}


/**
 * Method props
 */
export interface fileAsStreamProps {

}

export interface uploadtoURLProps {
    url: string;
}

export interface getServiceClientProps {
    url: string;
}

export interface existsProps {
    url: string;
}

export interface streamProps {
    url: string;
    onData: Function;
    onEnd?: Function;
    onError?: Function;
}

export interface readStream {
    url: string;
}

export interface downloadProps {
    url: string;
}

export interface saveProps {
    url: string;
    file: string;
}

export interface putProps {
    url: string;
    content: string;
    overwrite?: boolean;
}

export interface extReduceProps {
    url: string;
    reducer: Function; //the fuction processed on each row, returning a new value for the accumulator
    accumulator?: any; //initial value, altered from every return of the reducer.
    persist?: boolean;
}

export interface extCacheReturn {
    numRowsInserted: number;
}

export interface extMapProps {
    url: string;
    mapper: Function;
    persist?: boolean;
}

export interface ExtendedParserOptionsArgs extends ParserOptionsArgs {
    nullifyEmptyColumns?:boolean;
}

export interface extCompileProps {
    urls: string[];
    pk: string|string[]|Function;
}

export interface modifyFileProps {
    url: string;
    targetUrl?: string;
    pk: string|string[]|Function;
    modifications:Array<any>;
    report?:{}; 
}

export interface copyProps {
    source: string;
    target: string;
    gzip?: boolean;
}