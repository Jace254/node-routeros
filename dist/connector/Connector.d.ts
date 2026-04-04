/// <reference types="node" />
import { EventEmitter } from 'events';
/**
 * Connector class responsible for communicating with
 * the routeros via api, sending and receiving buffers.
 *
 * The main focus of this class is to be able to
 * construct and destruct dinamically by the RouterOSAPI class
 * when needed, so the authentication parameters don't
 * need to be changed every time we need to reconnect.
 */
export declare class Connector extends EventEmitter {
    /**
     * The host or address of where to connect to
     */
    host: string;
    /**
     * The port of the API
     */
    port: number;
    /**
     * The timeout in seconds of the connection
     */
    timeout: number;
    /**
     * The socket of the connection
     */
    private socket;
    /**
     * The transmitter object to write commands
     */
    private transmitter;
    /**
     * The receiver object to read commands
     */
    private receiver;
    /**
     * Connected status
     */
    private connected;
    /**
     * Connecting status
     */
    private connecting;
    /**
     * Closing status
     */
    private closing;
    /**
     * TLS data
     */
    private tls;
    /**
     * Constructor which receive the options of the connection
     *
     * @param {Object} options
     */
    constructor(options: any);
    /**
     * Connect to the routerboard
     *
     * @returns {Connector}
     */
    connect(): Connector;
    /**
     * Writes data through the open socket
     *
     * @param {Array} data
     * @returns {Connector}
     */
    write(data: string[]): Connector;
    /**
     * Register a tag to receive data
     *
     * @param {string} tag
     * @param {function} callback
     */
    read(tag: string, callback: (packet: string[]) => void): void;
    /**
     * Unregister a tag, so it no longer waits for data
     * @param {string} tag
     */
    stopRead(tag: string): void;
    /**
     * Start closing the connection
     */
    close(): void;
    /**
     * Destroy the socket, no more data
     * can be exchanged from now on and
     * this class itself must be recreated
     */
    destroy(): void;
    /**
     * Socket connection event listener.
     * After the connection is stablished,
     * ask the transmitter to run any
     * command stored over the pool
     *
     * @returns {function}
     */
    private onConnect;
    /**
     * Socket end event listener.
     * Terminates the connection after
     * the socket is released
     *
     * @returns {function}
     */
    private onEnd;
    /**
     * Socket error event listener.
     * Emmits the error while trying to connect and
     * destroys the socket.
     *
     * @returns {function}
     */
    private onError;
    /**
     * Socket timeout event listener
     * Emmits timeout error and destroys the socket
     *
     * @returns {function}
     */
    private onTimeout;
    /**
     * Socket data event listener
     * Receives the data and sends it to processing
     *
     * @returns {function}
     */
    private onData;
}
