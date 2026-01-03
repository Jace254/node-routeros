/// <reference types="node" />
import { EventEmitter } from 'events';
import { Channel } from './Channel';
/**
 * Stream class is responsible for handling
 * continuous data from some parts of the
 * routeros, like /ip/address/listen or
 * /tool/torch which keeps sending data endlessly.
 * It is also possible to pause/resume/stop generated
 * streams.
 */
export declare class RStream extends EventEmitter {
    /**
     * Main channel of the stream
     */
    private channel;
    /**
     * Parameters of the menu and search of
     * what to stream
     */
    private params;
    /**
     * The callback function sent to the
     * streaming listener, which will get an error
     * if any, or the packet received from the
     * command
     */
    private callback;
    /**
     * The function that will send empty data
     * unless debounced by real data from the command
     */
    private debounceSendingEmptyData;
    /** Flag for turning on empty data debouncing */
    private shouldDebounceEmptyData;
    /**
     * If is streaming flag
     */
    private streaming;
    /**
     * If is pausing flag
     */
    private pausing;
    /**
     * If is paused flag
     */
    private paused;
    /**
     * If is stopping flag
     */
    private stopping;
    /**
     * If is stopped flag
     */
    private stopped;
    /**
     * If got a trap error
     */
    private trapped;
    /**
     * Save the current section of the packet, if has any
     */
    private currentSection;
    private forcelyStop;
    /**
     * Store the current section in a single
     * array before sending when another section comes
     */
    private currentSectionPacket;
    /**
     * Waiting timeout before sending received section packets
     */
    private sectionPacketSendingTimeout;
    /**
     * Constructor, it also starts the streaming after construction
     *
     * @param {Channel} channel
     * @param {Array} params
     * @param {function} callback
     */
    constructor(channel: Channel, params: string[], callback?: (err: Error, packet?: any, stream?: RStream) => void);
    /**
     * Function to receive the callback which
     * will receive data, if not provided over the
     * constructor or changed later after the streaming
     * have started.
     *
     * @param {function} callback
     */
    data(callback: (err: Error, packet?: any, stream?: RStream) => void): void;
    /**
     * Resume the paused stream, using the same channel
     *
     * @returns {Promise}
     */
    resume(): Promise<void>;
    /**
     * Pause the stream, but don't destroy the channel
     *
     * @returns {Promise}
     */
    pause(): Promise<void>;
    /**
     * Stop the stream entirely, can't re-stream after
     * this if called directly.
     *
     * @returns {Promise}
     */
    stop(pausing?: boolean): Promise<void>;
    /**
     * Alias for stop()
     */
    close(): Promise<void>;
    /**
     * Write over the connection and start the stream
     */
    start(): void;
    prepareDebounceEmptyData(): void;
    /**
     * When receiving the stream packet, give it to
     * the callback
     *
     * @returns {function}
     */
    private onStream;
    /**
     * When receiving a trap over the connection,
     * when pausing, will receive a 'interrupted' message,
     * this will not be considered as an error but a flag
     * for the pause and resume function
     *
     * @returns {function}
     */
    private onTrap;
    /**
     * When the channel stops sending data.
     * It will close the channel if the
     * intention was stopping it.
     *
     * @returns {function}
     */
    private onDone;
}
