"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const iconv = require('iconv-lite');
const debug = require("debug");
const RosException_1 = require("../RosException");
const info = debug('routeros-api:connector:receiver:info');
const error = debug('routeros-api:connector:receiver:error');
const nullBuffer = Buffer.from([0x00]);
/**
 * Class responsible for receiving and parsing the socket
 * data, sending to the readers and listeners
 */
class Receiver {
    /**
     * Receives the socket so we are able to read
     * the data sent to it, separating each tag
     * to the according listener.
     *
     * @param socket
     */
    constructor(socket) {
        /**
         * The registered tags to answer data to
         */
        this.tags = new Map();
        /**
         * The length of the current data chain received from
         * the socket
         */
        this.dataLength = 0;
        /**
         * A pipe of all responses received from the routerboard
         */
        this.sentencePipe = [];
        /**
         * Flag if the sentencePipe is being processed to
         * prevent concurrent sentences breaking the pipe
         */
        this.processingSentencePipe = false;
        /**
         * The current line being processed from the data chain
         */
        this.currentLine = '';
        /**
         * The current reply received for the tag
         */
        this.currentReply = '';
        /**
         * The current tag which the routerboard responded
         */
        this.currentTag = '';
        /**
         * The current data chain or packet
         */
        this.currentPacket = [];
        this.socket = socket;
    }
    /**
     * Register the tag as a reader so when
     * the routerboard respond to the command
     * related to the tag, we know where to send
     * the data to
     *
     * @param {string} tag
     * @param {function} callback
     */
    read(tag, callback) {
        info('Reader of %s tag is being set', tag);
        this.tags.set(tag, {
            name: tag,
            callback: callback,
        });
    }
    /**
     * Stop reading from a tag, removing it
     * from the tag mapping. Usually it is closed
     * after the command has being !done, since each command
     * opens a new auto-generated tag
     *
     * @param {string} tag
     */
    stop(tag) {
        info('Not reading from %s tag anymore', tag);
        this.tags.delete(tag);
    }
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
    processRawData(data) {
        if (this.lengthDescriptorSegment) {
            data = Buffer.concat([this.lengthDescriptorSegment, data]);
            this.lengthDescriptorSegment = null;
        }
        // Loop through the data we just received
        while (data.length > 0) {
            // If this does not contain the beginning of a packet...
            if (this.dataLength > 0) {
                // If the length of the data we have in our buffer
                // is less than or equal to that reported by the
                // bytes used to dermine length...
                if (data.length <= this.dataLength) {
                    // Subtract the data we are taking from the length we desire
                    this.dataLength -= data.length;
                    // Add this data to our current line
                    this.currentLine += iconv.decode(data, 'win1252');
                    // If there is no more desired data we want...
                    if (this.dataLength === 0) {
                        // Push the data to the sentance
                        this.sentencePipe.push({
                            sentence: this.currentLine,
                            hadMore: data.length !== this.dataLength,
                        });
                        // process the sentance and clear the line
                        this.processSentence();
                        this.currentLine = '';
                    }
                    // Break out of processRawData and wait for the next
                    // set of data from the socket
                    break;
                    // If we have more data than we desire...
                }
                else {
                    // slice off the part that we desire
                    const tmpBuffer = data.slice(0, this.dataLength);
                    // decode this segment
                    const tmpStr = iconv.decode(tmpBuffer, 'win1252');
                    // Add this to our current line
                    this.currentLine += tmpStr;
                    // save our line...
                    const line = this.currentLine;
                    // clear the current line
                    this.currentLine = '';
                    // cut off the line we just pulled out
                    data = data.slice(this.dataLength);
                    // determine the length of the next word. This method also
                    // returns the number of bytes it took to describe the length
                    const [descriptor_length, length] = this.decodeLength(data);
                    // If we do not have enough data to determine
                    // the length... we wait for the next loop
                    // and store the length descriptor segment
                    if (descriptor_length > data.length) {
                        this.lengthDescriptorSegment = data;
                    }
                    // Save this as our next desired length
                    this.dataLength = length;
                    // slice off the bytes used to describe the length
                    data = data.slice(descriptor_length);
                    // If we only desire one more and its the end of the sentance...
                    if (this.dataLength === 1 && data.equals(nullBuffer)) {
                        this.dataLength = 0;
                        data = data.slice(1); // get rid of excess buffer
                    }
                    this.sentencePipe.push({
                        sentence: line,
                        hadMore: data.length > 0,
                    });
                    this.processSentence();
                }
                // This is the beginning of this packet...
                // This ALWAYS gets run first
            }
            else {
                // returns back the start index of the data and the length
                const [descriptor_length, length] = this.decodeLength(data);
                // store how long our data is
                this.dataLength = length;
                // slice off the bytes used to describe the length
                data = data.slice(descriptor_length);
                if (this.dataLength === 1 && data.equals(nullBuffer)) {
                    this.dataLength = 0;
                    data = data.slice(1); // get rid of excess buffer
                }
            }
        }
    }
    /**
     * Process each sentence from the data packet received.
     *
     * Detects the .tag of the packet, sending the data to the
     * related tag when another reply is detected or if
     * the packet had no more lines to be processed.
     *
     */
    processSentence() {
        if (!this.processingSentencePipe) {
            info('Got asked to process sentence pipe');
            this.processingSentencePipe = true;
            const process = () => {
                if (this.sentencePipe.length > 0) {
                    const line = this.sentencePipe.shift();
                    if (!line.hadMore && this.currentReply === '!fatal') {
                        this.socket.emit('fatal');
                        return;
                    }
                    info('Processing line %s', line.sentence);
                    if (/^\.tag=/.test(line.sentence)) {
                        this.currentTag = line.sentence.substring(5);
                    }
                    else if (/^!/.test(line.sentence)) {
                        const tagToSend = this.currentTag;
                        const tagExists = this.tags.has(tagToSend);
                        if (tagExists) {
                            info('Received another response, sending current data to tag %s', tagToSend);
                            this.sendTagData(tagToSend);
                        }
                        else {
                            info('Tag %s is no longer registered, skipping send', tagToSend);
                        }
                        this.currentPacket.push(line.sentence);
                        this.currentReply = line.sentence;
                    }
                    else {
                        this.currentPacket.push(line.sentence);
                    }
                    // Check if we should process more sentences
                    if (this.sentencePipe.length === 0 &&
                        this.dataLength === 0) {
                        if (!line.hadMore && this.currentTag) {
                            info('No more sentences to process, will send data to tag %s', this.currentTag);
                            // Store the current tag before sending data
                            // as sendTagData may unregister it
                            const tagToSend = this.currentTag;
                            // Before we clean up or potentially destroy the tag reference
                            const tagExists = this.tags.has(tagToSend);
                            if (tagExists) {
                                this.sendTagData(tagToSend);
                            }
                            else {
                                info('Tag %s is no longer registered, skipping send', tagToSend);
                                this.cleanUp();
                            }
                        }
                        else {
                            info('No more sentences and no data to send');
                        }
                        this.processingSentencePipe = false;
                    }
                    else {
                        // Handle case where the tag might have been unregistered
                        // If we have another line to process after the tag has been unregistered
                        // Check if we still need to process remaining sentences
                        if (this.currentReply === '!done' || this.currentReply === '!empty') {
                            // Check if we should process more sentences or reset
                            // If there are more sentences referencing a tag that's already
                            // been closed, we should skip them or handle them differently
                            const nextLines = this.sentencePipe.filter(l => /^\.tag=/.test(l.sentence));
                            if (nextLines.length > 0) {
                                const nextTagLine = nextLines[0].sentence;
                                const nextTag = nextTagLine.substring(5);
                                // If the next tag is the same as the current one we just processed
                                // and the tag is not registered anymore, we should clear the pipe
                                if (nextTag === this.currentTag && !this.tags.has(this.currentTag)) {
                                    info('Detected unregistered tag %s in pipeline, clearing pipe', this.currentTag);
                                    this.sentencePipe = [];
                                    this.cleanUp();
                                    this.processingSentencePipe = false;
                                    return;
                                }
                            }
                        }
                        process();
                    }
                }
                else {
                    this.processingSentencePipe = false;
                }
            };
            process();
        }
    }
    /**
     * Send the data collected from the tag to the
     * tag reader
     */
    sendTagData(currentTag) {
        const tag = this.tags.get(currentTag);
        if (tag) {
            info('Sending to tag %s the packet %O', tag.name, this.currentPacket);
            tag.callback(this.currentPacket);
        }
        else {
            throw new RosException_1.RosException('UNREGISTEREDTAG');
        }
        this.cleanUp();
    }
    /**
     * Clean the current packet, tag and reply state
     * to start over
     */
    cleanUp() {
        this.currentPacket = [];
        this.currentTag = null;
        this.currentReply = null;
    }
    /**
     * Decodes the length of the buffer received
     *
     * Credits for George Joseph: https://github.com/gtjoseph
     * and for Brandon Myers: https://github.com/Trakkasure
     *
     * @param {Buffer} data
     */
    decodeLength(data) {
        let len;
        let idx = 0;
        const b = data[idx++];
        if (b & 128) {
            if ((b & 192) === 128) {
                len = ((b & 63) << 8) + data[idx++];
            }
            else {
                if ((b & 224) === 192) {
                    len = ((b & 31) << 8) + data[idx++];
                    len = (len << 8) + data[idx++];
                }
                else {
                    if ((b & 240) === 224) {
                        len = ((b & 15) << 8) + data[idx++];
                        len = (len << 8) + data[idx++];
                        len = (len << 8) + data[idx++];
                    }
                    else {
                        len = data[idx++];
                        len = (len << 8) + data[idx++];
                        len = (len << 8) + data[idx++];
                        len = (len << 8) + data[idx++];
                    }
                }
            }
        }
        else {
            len = b;
        }
        return [idx, len];
    }
}
exports.Receiver = Receiver;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiUmVjZWl2ZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi9zcmMvY29ubmVjdG9yL1JlY2VpdmVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7O0FBQ0EsTUFBTSxLQUFLLEdBQUcsT0FBTyxDQUFDLFlBQVksQ0FBQyxDQUFDO0FBQ3BDLCtCQUErQjtBQUMvQixrREFBK0M7QUFFL0MsTUFBTSxJQUFJLEdBQUcsS0FBSyxDQUFDLHNDQUFzQyxDQUFDLENBQUM7QUFDM0QsTUFBTSxLQUFLLEdBQUcsS0FBSyxDQUFDLHVDQUF1QyxDQUFDLENBQUM7QUFDN0QsTUFBTSxVQUFVLEdBQUcsTUFBTSxDQUFDLElBQUksQ0FBQyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUM7QUFnQnZDOzs7R0FHRztBQUNILE1BQWEsUUFBUTtJQXVEakI7Ozs7OztPQU1HO0lBQ0gsWUFBWSxNQUFjO1FBeEQxQjs7V0FFRztRQUNLLFNBQUksR0FBK0IsSUFBSSxHQUFHLEVBQUUsQ0FBQztRQUVyRDs7O1dBR0c7UUFDSyxlQUFVLEdBQVcsQ0FBQyxDQUFDO1FBRS9COztXQUVHO1FBQ0ssaUJBQVksR0FBZ0IsRUFBRSxDQUFDO1FBRXZDOzs7V0FHRztRQUNLLDJCQUFzQixHQUFZLEtBQUssQ0FBQztRQUVoRDs7V0FFRztRQUNLLGdCQUFXLEdBQVcsRUFBRSxDQUFDO1FBRWpDOztXQUVHO1FBQ0ssaUJBQVksR0FBVyxFQUFFLENBQUM7UUFFbEM7O1dBRUc7UUFDSyxlQUFVLEdBQVcsRUFBRSxDQUFDO1FBRWhDOztXQUVHO1FBQ0ssa0JBQWEsR0FBYSxFQUFFLENBQUM7UUFpQmpDLElBQUksQ0FBQyxNQUFNLEdBQUcsTUFBTSxDQUFDO0lBQ3pCLENBQUM7SUFFRDs7Ozs7Ozs7T0FRRztJQUNJLElBQUksQ0FBQyxHQUFXLEVBQUUsUUFBb0M7UUFDekQsSUFBSSxDQUFDLCtCQUErQixFQUFFLEdBQUcsQ0FBQyxDQUFDO1FBQzNDLElBQUksQ0FBQyxJQUFJLENBQUMsR0FB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