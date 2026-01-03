/// <reference types="node" />
import { EventEmitter } from 'events';
import { Connector } from './connector/Connector';
import { IRosGenericResponse } from './IRosGenericResponse';
/**
 * Channel class is responsible for generating
 * ids for the channels and writing over
 * the ids generated, while listening for
 * their responses
 */
export declare class Channel extends EventEmitter {
    /**
     * Id of the channel
     */
    private id;
    /**
     * The connector object
     */
    private connector;
    /**
     * Data received related to the channel
     */
    private data;
    /**
     * If received a trap instead of a positive response
     */
    private trapped;
    /**
     * If is streaming content
     */
    private streaming;
    /**
     * Constructor
     *
     * @param {Connector} connector
     */
    constructor(connector: Connector);
    /**
     * Get the id of the channel
     *
     * @returns {string}
     */
    get Id(): string;
    /**
     * Get the connector used in the channel
     *
     * @returns {Connector}
     */
    get Connector(): Connector;
    /**
     * Organize the data to be written over the socket with the id
     * generated. Adds a reader to the id provided, so we wait for
     * the data.
     *
     * @param {Array} params
     * @returns {Promise}
     */
    write(params: string[], isStream?: boolean, returnPromise?: boolean): Promise<IRosGenericResponse[]>;
    /**
     * Closes the channel, algo asking for
     * the connector to remove the reader.
     * If streaming, not forcing will only stop
     * the reader, not the listeners of the events
     *
     * @param {boolean} force - force closing by removing all listeners
     */
    close(force?: boolean): void;
    /**
     * Register the reader for the tag and write the params over
     * the socket
     *
     * @param {Array} params
     */
    private readAndWrite;
    /**
     * Process the data packet received to
     * figure out the answer to give to the
     * channel listener, either if it's just
     * the data we were expecting or if
     * a trap was given.
     *
     * @param {Array} packet
     */
    private processPacket;
    /**
     * Parse the packet line, separating the key from the data.
     * Ex: transform '=interface=ether2' into object {interface:'ether2'}
     *
     * @param {Array} packet
     * @return {Object}
     */
    private parsePacket;
    /**
     * Waits for the unknown event.
     * It shouldn't happen, but if it does, throws the error and
     * stops the channel
     *
     * @param {string} reply
     * @returns {function}
     */
    private onUnknown;
}
