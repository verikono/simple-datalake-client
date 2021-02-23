import {
    DataLakeServiceClient
} from '@azure/storage-file-datalake';

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

export interface uploadProps {
    url: string;
}

export interface extReduceProps {
    url: string;
    reducer: Function; //the fuction processed on each row, returning a new value for the accumulator
    accumulator?: any; //initial value, altered from every return of the reducer.
}

export interface extCacheReturn {
    numRowsInserted: number;
}