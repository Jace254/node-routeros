/// <reference types="node" />
import { TlsOptions } from 'tls';
import { IRosOptions } from './IRosOptions';
import { RStream } from './RStream';
import { EventEmitter } from 'events';
import { IRosGenericResponse } from './IRosGenericResponse';
/**
 * Creates a connection object with the credentials provided
 */
export declare class RouterOSAPI extends EventEmitter {
    /**
     * Host to connect
     */
    host: string;
    /**
     * Username to use
     */
    user: string;
    /**
     * Password of the username
     */
    password: string;
    /**
     * Port of the API
     */
    port: number;
    /**
     * Timeout of the connection
     */
    timeout: number;
    /**
     * TLS Options to use, if any
     */
    tls: TlsOptions;
    /**
     * Connected flag
     */
    connected: boolean;
    /**
     * Connecting flag
     */
    connecting: boolean;
    /**
     * Closing flag
     */
    closing: boolean;
    /**
     * Keep connection alive
     */
    keepalive: boolean;
    /**
     * The connector which will be used
     */
    private connector;
    /**
     * The function timeout that will keep the connection alive
     */
    private keptaliveby;
    /**
     * Counter for channels open
     */
    private channelsOpen;
    /**
     * Flag if the connection was held by the keepalive parameter
     * or keepaliveBy function
     */
    private holdingConnectionWithKeepalive;
    /**
     * Store the timeout when holding the connection
     * when waiting for a channel response
     */
    private connectionHoldInterval;
    private registeredStreams;
    /**
     * Constructor, also sets the language of the thrown errors
     *
     * @param {Object} options
     */
    constructor(options: IRosOptions);
    /**
     * Set connection options, affects before connecting
     *
     * @param options connection options
     */
    setOptions(options: IRosOptions): void;
    /**
     * Tries a connection to the routerboard with the provided credentials
     *
     * @returns {Promise}
     */
    connect(): Promise<RouterOSAPI>;
    /**
     * Writes a command over the socket to the routerboard
     * on a new channel
     *
     * @param {string|Array} params
     * @param {Array<string|string[]>} moreParams
     * @returns {Promise}
     */
    write(params: string | string[], ...moreParams: Array<string | string[]>): Promise<IRosGenericResponse[]>;
    /**
     * Writes a command over the socket to the routerboard
     * on a new channel and return an event of what happens
     * with the responses. Listen for 'data', 'done', 'trap' and 'close'
     * events.
     *
     * @param {string|Array} params
     * @param {Array<string|string[]>} moreParams
     * @returns {RStream}
     */
    writeStream(params: string | string[], ...moreParams: Array<string | string[]>): RStream;
    /**
     * Returns a stream object for handling continuous data
     * flow.
     *
     * @param {string|Array} params
     * @param {function} callback
     * @returns {RStream}
     */
    stream(params?: string | string[], ...moreParams: any[]): RStream;
    /**
     * Keep the connection alive by running a set of
     * commands provided instead of the random command
     *
     * @param {string|Array} params
     * @param {function} callback
     */
    keepaliveBy(params?: string | string[], ...moreParams: any[]): void;
    /**
     * Closes the connection.
     * It can be openned again without recreating
     * an object from this class.
     *
     * @returns {Promise}
     */
    close(): Promise<RouterOSAPI>;
    /**
     * Opens a new channel either for just writing or streaming
     *
     * @returns {Channel}
     */
    private openChannel;
    private increaseChannelsOpen;
    private decreaseChannelsOpen;
    private registerStream;
    private unregisterStream;
    private stopAllStreams;
    /**
     * Holds the connection if keepalive wasn't set
     * so when a channel opens, ensure that we
     * receive a response before a timeout
     */
    private holdConnection;
    /**
     * Release the connection that was held
     * when waiting for responses from channels open
     */
    private releaseConnectionHold;
    /**
     * Login on the routerboard to provide
     * api functionalities, using the credentials
     * provided.
     *
     * @returns {Promise}
     */
    private login;
    private concatParams;
}
