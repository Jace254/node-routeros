/// <reference types="node" />
import { Socket } from 'net';
export interface ISentence {
    sentence: string;
    hadMore: boolean;
}
/**
 * Interface of the callback which is stored
 * the tag readers with their respective callbacks
 */
export interface IReadCallback {
    name: string;
    callback: (data: string[]) => void;
}
/**
 * Class responsible for receiving and parsing the socket
 * data, sending to the readers and listeners
 */
export declare class Receiver {
    /**
     * The socket which connects to the routerboard
     */
    private socket;
    /**
     * The registered tags to answer data to
     */
    private tags;
    /**
     * The length of the current data chain received from
     * the socket
     */
    private dataLength;
    /**
     * A pipe of all responses received from the routerboard
     */
    private sentencePipe;
    /**
     * Flag if the sentencePipe is being processed to
     * prevent concurrent sentences breaking the pipe
     */
    private processingSentencePipe;
    /**
     * The current line being processed from the data chain
     */
    private currentLine;
    /**
     * The current reply received for the tag
     */
    private currentReply;
    /**
     * The current tag which the routerboard responded
     */
    private currentTag;
    /**
     * The current data chain or packet
     */
    private currentPacket;
    /**
     * Used to store a partial segment of the
     * length descriptor if it gets split
     * between tcp transmissions.
     */
    private lengthDescriptorSegment;
    /**
     * Receives the socket so we are able to read
     * the data sent to it, separating each tag
     * to the according listener.
     *
     * @param socket
     */
    constructor(socket: Socket);
    /**
     * Register the tag as a reader so when
     * the routerboard respond to the command
     * related to the tag, we know where to send
     * the data to
     *
     * @param {string} tag
     * @param {function} callback
     */
    read(tag: string, callback: (packet: string[]) => void): void;
    /**
     * Stop reading from a tag, removing it
     * from the tag mapping. Usually it is closed
     * after the command has being !done, since each command
     * opens a new auto-generated tag
     *
     * @param {string} tag
     */
    stop(tag: string): void;
    /**
     * Proccess the raw buffer data received from the routerboard,
     * decode using win1252 encoded string from the routerboard to
     * utf-8, so languages with accentuation works out of the box.
     *
     * After reading each sentence from the raw packet, sends it
     * to be parsed
     *
     * @param {Buffer} data
     */
    processRawData(data: Buffer): void;
    /**
     * Process each sentence from the data packet received.
     *
     * Detects the .tag of the packet, sending the data to the
     * related tag when another reply is detected or if
     * the packet had no more lines to be processed.
     *
     */
    private processSentence;
    /**
     * Send the data collected from the tag to the
     * tag reader
     */
    private sendTagData;
    /**
     * Clean the current packet, tag and reply state
     * to start over
     */
    private cleanUp;
    /**
     * Decodes the length of the buffer received
     *
     * Credits for George Joseph: https://github.com/gtjoseph
     * and for Brandon Myers: https://github.com/Trakkasure
     *
     * @param {Buffer} data
     */
    private decodeLength;
}
