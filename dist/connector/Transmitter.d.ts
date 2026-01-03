/// <reference types="node" />
import { Socket } from 'net';
/**
 * Class responsible for transmitting data over the
 * socket to the routerboard
 */
export declare class Transmitter {
    /**
     * The socket which connects to the routerboard
     */
    private socket;
    /**
     * Pool of data to be sent after the socket connects
     */
    private pool;
    /**
     * Constructor
     *
     * @param socket
     */
    constructor(socket: Socket);
    /**
     * Write data over the socket, if it not writable yet,
     * save over the pool to be ran after
     *
     * @param {string} data
     */
    write(data: string): void;
    /**
     * Writes all data stored in the pool
     */
    runPool(): void;
    /**
     * Encode the string data that will
     * be sent over to the routerboard.
     *
     * It's encoded in win1252 so any accentuation on foreign languages
     * are displayed correctly when opened with winbox.
     *
     * Credits for George Joseph: https://github.com/gtjoseph
     * and for Brandon Myers: https://github.com/Trakkasure
     *
     * @param {string} str
     */
    private encodeString;
}
