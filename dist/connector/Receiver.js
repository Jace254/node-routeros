"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const iconv = require('iconv-lite');
const debug = require("debug");
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
                        if (this.currentTag) {
                            info('Received another response, sending current data to tag %s', this.currentTag);
                            this.sendTagData(this.currentTag);
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
            info('Tag %s is no longer registered, discarding packet', currentTag);
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiUmVjZWl2ZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi9zcmMvY29ubmVjdG9yL1JlY2VpdmVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7O0FBQ0EsTUFBTSxLQUFLLEdBQUcsT0FBTyxDQUFDLFlBQVksQ0FBQyxDQUFDO0FBQ3BDLCtCQUErQjtBQUcvQixNQUFNLElBQUksR0FBRyxLQUFLLENBQUMsc0NBQXNDLENBQUMsQ0FBQztBQUMzRCxNQUFNLEtBQUssR0FBRyxLQUFLLENBQUMsdUNBQXVDLENBQUMsQ0FBQztBQUM3RCxNQUFNLFVBQVUsR0FBRyxNQUFNLENBQUMsSUFBSSxDQUFDLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQztBQWdCdkM7OztHQUdHO0FBQ0gsTUFBYSxRQUFRO0lBdURqQjs7Ozs7O09BTUc7SUFDSCxZQUFZLE1BQWM7UUF4RDFCOztXQUVHO1FBQ0ssU0FBSSxHQUErQixJQUFJLEdBQUcsRUFBRSxDQUFDO1FBRXJEOzs7V0FHRztRQUNLLGVBQVUsR0FBVyxDQUFDLENBQUM7UUFFL0I7O1dBRUc7UUFDSyxpQkFBWSxHQUFnQixFQUFFLENBQUM7UUFFdkM7OztXQUdHO1FBQ0ssMkJBQXNCLEdBQVksS0FBSyxDQUFDO1FBRWhEOztXQUVHO1FBQ0ssZ0JBQVcsR0FBVyxFQUFFLENBQUM7UUFFakM7O1dBRUc7UUFDSyxpQkFBWSxHQUFXLEVBQUUsQ0FBQztRQUVsQzs7V0FFRztRQUNLLGVBQVUsR0FBVyxFQUFFLENBQUM7UUFFaEM7O1dBRUc7UUFDSyxrQkFBYSxHQUFhLEVBQUUsQ0FBQztRQWlCakMsSUFBSSxDQUFDLE1BQU0sR0FBRyxNQUFNLENBQUM7SUFDekIsQ0FBQztJQUVEOzs7Ozs7OztPQVFHO0lBQ0ksSUFBSSxDQUFDLEdBQVcsRUFBRSxRQUFvQztRQUN6RCxJQUFJLENBQUMsK0JBQStCLEVBQUUsR0FBRyxDQUFDLENBQUM7UUFDM0MsSUFBSSxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsR0FBRyxFQUFFO1lBQ2YsSUFBSSxFQUFFLEdBQUc7WUFDVCxRQUFRLEVBQUUsUUFBUTtTQUNyQixDQUFDLENBQUM7SUFDUCxDQUFDO0lBRUQ7Ozs7Ozs7T0FPRztJQUNJLElBQUksQ0FBQyxHQUFXO1FBQ25CLElBQUksQ0FBQyxpQ0FBaUMsRUFBRSxHQUFHLENBQUMsQ0FBQztRQUM3QyxJQUFJLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQyxHQUFHLENBQUMsQ0FBQztJQUMxQixDQUFDO0lBRUQ7Ozs7Ozs7OztPQVNHO0lBQ0ksY0FBYyxDQUFDLElBQVk7UUFDOUIsSUFBSSxJQUFJLENBQUMsdUJBQXVCLEVBQUU7WUFDOUIsSUFBSSxHQUFHLE1BQU0sQ0FBQyxNQUFNLENBQUMsQ0FBQyxJQUFJLENBQUMsdUJBQXVCLEVBQUUsSUFBSSxDQUFDLENBQUMsQ0FBQztZQUMzRCxJQUFJLENBQUMsdUJBQXVCLEdBQUcsSUFBSSxDQUFDO1NBQ3ZDO1FBRUQseUNBQXlDO1FBQ3pDLE9BQU8sSUFBSSxDQUFDLE1BQU0sR0FBRyxDQUFDLEVBQUU7WUFDcEIsd0RBQXdEO1lBQ3hELElBQUksSUFBSSxDQUFDLFVBQVUsR0FBRyxDQUFDLEVBQUU7Z0JBQ3JCLGtEQUFrRDtnQkFDbEQsZ0RBQWdEO2dCQUNoRCxrQ0FBa0M7Z0JBQ2xDLElBQUksSUFBSSxDQUFDLE1BQU0sSUFBSSxJQUFJLENBQUMsVUFBVSxFQUFFO29CQUNoQyw0REFBNEQ7b0JBQzVELElBQUksQ0FBQyxVQUFVLElBQUksSUFBSSxDQUFDLE1BQU0sQ0FBQztvQkFFL0Isb0NBQW9DO29CQUNwQyxJQUFJLENBQUMsV0FBVyxJQUFJLEtBQUssQ0FBQyxNQUFNLENBQUMsSUFBSSxFQUFFLFNBQVMsQ0FBQyxDQUFDO29CQUVsRCw4Q0FBOEM7b0JBQzlDLElBQUksSUFBSSxDQUFDLFVBQVUsS0FBSyxDQUFDLEVBQUU7d0JBQ3ZCLGdDQUFnQzt3QkFDaEMsSUFBSSxDQUFDLFlBQVksQ0FBQyxJQUFJLENBQUM7NEJBQ25CLFFBQVEsRUFBRSxJQUFJLENBQUMsV0FBVzs0QkFDMUIsT0FBTyxFQUFFLElBQUksQ0FBQyxNQUFNLEtBQUssSUFBSSxDQUFDLFVBQVU7eUJBQzNDLENBQUMsQ0FBQzt3QkFFSCwwQ0FBMEM7d0JBQzFDLElBQUksQ0FBQyxlQUFlLEVBQUUsQ0FBQzt3QkFDdkIsSUFBSSxDQUFDLFdBQVcsR0FBRyxFQUFFLENBQUM7cUJBQ3pCO29CQUVELG9EQUFvRDtvQkFDcEQsOEJBQThCO29CQUM5QixNQUFNO29CQUVOLHlDQUF5QztpQkFDNUM7cUJBQU07b0JBQ0gsb0NBQW9DO29CQUNwQyxNQUFNLFNBQVMsR0FBRyxJQUFJLENBQUMsS0FBSyxDQUFDLENBQUMsRUFBRSxJQUFJLENBQUMsVUFBVSxDQUFDLENBQUM7b0JBRWpELHNCQUFzQjtvQkFDdEIsTUFBTSxNQUFNLEdBQUcsS0FBSyxDQUFDLE1BQU0sQ0FBQyxTQUFTLEVBQUUsU0FBUyxDQUFDLENBQUM7b0JBRWxELCtCQUErQjtvQkFDL0IsSUFBSSxDQUFDLFdBQVcsSUFBSSxNQUFNLENBQUM7b0JBRTNCLG1CQUFtQjtvQkFDbkIsTUFBTSxJQUFJLEdBQUcsSUFBSSxDQUFDLFdBQVcsQ0FBQztvQkFFOUIseUJBQXlCO29CQUN6QixJQUFJLENBQUMsV0FBVyxHQUFHLEVBQUUsQ0FBQztvQkFFdEIsc0NBQXNDO29CQUN0QyxJQUFJLEdBQUcsSUFBSSxDQUFDLEtBQUssQ0FBQyxJQUFJLENBQUMsVUFBVSxDQUFDLENBQUM7b0JBRW5DLDBEQUEwRDtvQkFDMUQsNkRBQTZEO29CQUM3RCxNQUFNLENBQUMsaUJBQWlCLEVBQUUsTUFBTSxDQUFDLEdBQUcsSUFBSSxDQUFDLFlBQVksQ0FBQyxJQUFJLENBQUMsQ0FBQztvQkFFNUQsNkNBQTZDO29CQUM3QywwQ0FBMEM7b0JBQzFDLDBDQUEwQztvQkFDMUMsSUFBSSxpQkFBaUIsR0FBRyxJQUFJLENBQUMsTUFBTSxFQUFFO3dCQUNqQyxJQUFJLENBQUMsdUJBQXVCLEdBQUcsSUFBSSxDQUFDO3FCQUN2QztvQkFFRCx1Q0FBdUM7b0JBQ3ZDLElBQUksQ0FBQyxVQUFVLEdBQUcsTUFBTSxDQUFDO29CQUV6QixrREFBa0Q7b0JBQ2xELElBQUksR0FBRyxJQUFJLENBQUMsS0FBSyxDQUFDLGlCQUFpQixDQUFDLENBQUM7b0JBRXJDLGdFQUFnRTtvQkFDaEUsSUFBSSxJQUFJLENBQUMsVUFBVSxLQUFLLENBQUMsSUFBSSxJQUFJLENBQUMsTUFBTSxDQUFDLFVBQVUsQ0FBQyxFQUFFO3dCQUNsRCxJQUFJLENBQUMsVUFBVSxHQUFHLENBQUMsQ0FBQzt3QkFDcEIsSUFBSSxHQUFHLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQywyQkFBMkI7cUJBQ3BEO29CQUNELElBQUksQ0FBQyxZQUFZLENBQUMsSUFBSSxDQUFDO3dCQUNuQixRQUFRLEVBQUUsSUFBSTt3QkFDZCxPQUFPLEVBQUUsSUFBSSxDQUFDLE1BQU0sR0FBRyxDQUFDO3FCQUMzQixDQUFDLENBQUM7b0JBQ0gsSUFBSSxDQUFDLGVBQWUsRUFBRSxDQUFDO2lCQUMxQjtnQkFFRCwwQ0FBMEM7Z0JBQzFDLDZCQUE2QjthQUNoQztpQkFBTTtnQkFDSCwwREFBMEQ7Z0JBQzFELE1BQU0sQ0FBQyxpQkFBaUIsRUFBRSxNQUFNLENBQUMsR0FBRyxJQUFJLENBQUMsWUFBWSxDQUFDLElBQUksQ0FBQyxDQUFDO2dCQUU1RCw2QkFBNkI7Z0JBQzdCLElBQUksQ0FBQyxVQUFVLEdBQUcsTUFBTSxDQUFDO2dCQUV6QixrREFBa0Q7Z0JBQ2xELElBQUksR0FBRyxJQUFJLENBQUMsS0FBSyxDQUFDLGlCQUFpQixDQUFDLENBQUM7Z0JBRXJDLElBQUksSUFBSSxDQUFDLFVBQVUsS0FBSyxDQUFDLElBQUksSUFBSSxDQUFDLE1BQU0sQ0FBQyxVQUFVLENBQUMsRUFBRTtvQkFDbEQsSUFBSSxDQUFDLFVBQVUsR0FBRyxDQUFDLENBQUM7b0JBQ3BCLElBQUksR0FBRyxJQUFJLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsMkJBQTJCO2lCQUNwRDthQUNKO1NBQ0o7SUFDTCxDQUFDO0lBRUQ7Ozs7Ozs7T0FPRztJQUNLLGVBQWU7UUFDbkIsSUFBSSxDQUFDLElBQUksQ0FBQyxzQkFBc0IsRUFBRTtZQUM5QixJQUFJLENBQUMsb0NBQW9DLENBQUMsQ0FBQztZQUUzQyxJQUFJLENBQUMsc0JBQXNCLEdBQUcsSUFBSSxDQUFDO1lBRW5DLE1BQU0sT0FBTyxHQUFHLEdBQUcsRUFBRTtnQkFDakIsSUFBSSxJQUFJLENBQUMsWUFBWSxDQUFDLE1BQU0sR0FBRyxDQUFDLEVBQUU7b0JBQzlCLE1BQU0sSUFBSSxHQUFHLElBQUksQ0FBQyxZQUFZLENBQUMsS0FBSyxFQUFFLENBQUM7b0JBRXZDLElBQUksQ0FBQyxJQUFJLENBQUMsT0FBTyxJQUFJLElBQUksQ0FBQyxZQUFZLEtBQUssUUFBUSxFQUFFO3dCQUNqRCxJQUFJLENBQUMsTUFBTSxDQUFDLElBQUksQ0FBQyxPQUFPLENBQUMsQ0FBQzt3QkFDMUIsT0FBTztxQkFDVjtvQkFFRCxJQUFJLENBQUMsb0JBQW9CLEVBQUUsSUFBSSxDQUFDLFFBQVEsQ0FBQyxDQUFDO29CQUUxQyxJQUFJLFNBQVMsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxFQUFFO3dCQUMvQixJQUFJLENBQUMsVUFBVSxHQUFHLElBQUksQ0FBQyxRQUFRLENBQUMsU0FBUyxDQUFDLENBQUMsQ0FBQyxDQUFDO3FCQUNoRDt5QkFBTSxJQUFJLElBQUksQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxFQUFFO3dCQUNqQyxJQUFJLElBQUksQ0FBQyxVQUFVLEVBQUU7NEJBQ2pCLElBQUksQ0FDQSwyREFBMkQsRUFDM0QsSUFBSSxDQUFDLFVBQVUsQ0FDbEIsQ0FBQzs0QkFDRixJQUFJLENBQUMsV0FBVyxDQUFDLElBQUksQ0FBQyxVQUFVLENBQUMsQ0FBQzt5QkFDckM7d0JBRUQsSUFBSSxDQUFDLGFBQWEsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxDQUFDO3dCQUN2QyxJQUFJLENBQUMsWUFBWSxHQUFHLElBQUksQ0FBQyxRQUFRLENBQUM7cUJBQ3JDO3lCQUFNO3dCQUNILElBQUksQ0FBQyxhQUFhLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsQ0FBQztxQkFDMUM7b0JBRUQsNENBQTRDO29CQUM1QyxJQUNJLElBQUksQ0FBQyxZQUFZLENBQUMsTUFBTSxLQUFLLENBQUM7d0JBQzlCLElBQUksQ0FBQyxVQUFVLEtBQUssQ0FBQyxFQUN2Qjt3QkFDRSxJQUFJLENBQUMsSUFBSSxDQUFDLE9BQU8sSUFBSSxJQUFJLENBQUMsVUFBVSxFQUFFOzRCQUNsQyxJQUFJLENBQ0Esd0RBQXdELEVBQ3hELElBQUksQ0FBQyxVQUFVLENBQ2xCLENBQUM7NEJBRUYsNENBQTRDOzRCQUM1QyxtQ0FBbUM7NEJBQ25DLE1BQU0sU0FBUyxHQUFHLElBQUksQ0FBQyxVQUFVLENBQUM7NEJBRWxDLDhEQUE4RDs0QkFDOUQsTUFBTSxTQUFTLEdBQUcsSUFBSSxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsU0FBUyxDQUFDLENBQUM7NEJBQzNDLElBQUksU0FBUyxFQUFFO2dDQUNYLElBQUksQ0FBQyxXQUFXLENBQUMsU0FBUyxDQUFDLENBQUM7NkJBQy9CO2lDQUFNO2dDQUNILElBQUksQ0FBQywrQ0FBK0MsRUFBRSxTQUFTLENBQUMsQ0FBQztnQ0FDakUsSUFBSSxDQUFDLE9BQU8sRUFBRSxDQUFDOzZCQUNsQjt5QkFDSjs2QkFBTTs0QkFDSCxJQUFJLENBQUMsdUNBQXVDLENBQUMsQ0FBQzt5QkFDakQ7d0JBQ0QsSUFBSSxDQUFDLHNCQUFzQixHQUFHLEtBQUssQ0FBQztxQkFDdkM7eUJBQU07d0JBQ0gseURBQXlEO3dCQUN6RCx5RUFBeUU7d0JBQ3pFLHdEQUF3RDt3QkFDeEQsSUFBSSxJQUFJLENBQUMsWUFBWSxLQUFLLE9BQU8sSUFBSSxJQUFJLENBQUMsWUFBWSxLQUFLLFFBQVEsRUFBRTs0QkFDakUscURBQXFEOzRCQUNyRCwrREFBK0Q7NEJBQy9ELDhEQUE4RDs0QkFDOUQsTUFBTSxTQUFTLEdBQUcsSUFBSSxDQUFDLFlBQVksQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxTQUFTLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxRQUFRLENBQUMsQ0FBQyxDQUFDOzRCQUM1RSxJQUFJLFNBQVMsQ0FBQyxNQUFNLEdBQUcsQ0FBQyxFQUFFO2dDQUN0QixNQUFNLFdBQVcsR0FBRyxTQUFTLENBQUMsQ0FBQyxDQUFDLENBQUMsUUFBUSxDQUFDO2dDQUMxQyxNQUFNLE9BQU8sR0FBRyxXQUFXLENBQUMsU0FBUyxDQUFDLENBQUMsQ0FBQyxDQUFDO2dDQUV6QyxtRUFBbUU7Z0NBQ25FLGtFQUFrRTtnQ0FDbEUsSUFBSSxPQUFPLEtBQUssSUFBSSxDQUFDLFVBQVUsSUFBSSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLElBQUksQ0FBQyxVQUFVLENBQUMsRUFBRTtvQ0FDaEUsSUFBSSxDQUFDLHlEQUF5RCxFQUFFLElBQUksQ0FBQyxVQUFVLENBQUMsQ0FBQztvQ0FDakYsSUFBSSxDQUFDLFlBQVksR0FBRyxFQUFFLENBQUM7b0NBQ3ZCLElBQUksQ0FBQyxPQUFPLEVBQUUsQ0FBQztvQ0FDZixJQUFJLENBQUMsc0JBQXNCLEdBQUcsS0FBSyxDQUFDO29DQUNwQyxPQUFPO2lDQUNWOzZCQUNKO3lCQUNKO3dCQUNELE9BQU8sRUFBRSxDQUFDO3FCQUNiO2lCQUNKO3FCQUFNO29CQUNILElBQUksQ0FBQyxzQkFBc0IsR0FBRyxLQUFLLENBQUM7aUJBQ3ZDO1lBQ0wsQ0FBQyxDQUFDO1lBRUYsT0FBTyxFQUFFLENBQUM7U0FDYjtJQUNMLENBQUM7SUFFRDs7O09BR0c7SUFDSyxXQUFXLENBQUMsVUFBa0I7UUFDbEMsTUFBTSxHQUFHLEdBQUcsSUFBSSxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsVUFBVSxDQUFDLENBQUM7UUFDdEMsSUFBSSxHQUFHLEVBQUU7WUFDTCxJQUFJLENBQ0EsaUNBQWlDLEVBQ2pDLEdBQUcsQ0FBQyxJQUFJLEVBQ1IsSUFBSSxDQUFDLGFBQWEsQ0FDckIsQ0FBQztZQUNGLEdBQUcsQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLGFBQWEsQ0FBQyxDQUFDO1NBQ3BDO2FBQU07WUFDSCxJQUFJLENBQUMsbURBQW1ELEVBQUUsVUFBVSxDQUFDLENBQUM7U0FDekU7UUFDRCxJQUFJLENBQUMsT0FBTyxFQUFFLENBQUM7SUFDbkIsQ0FBQztJQUVEOzs7T0FHRztJQUNLLE9BQU87UUFDWCxJQUFJLENBQUMsYUFBYSxHQUFHLEVBQUUsQ0FBQztRQUN4QixJQUFJLENBQUMsVUFBVSxHQUFHLElBQUksQ0FBQztRQUN2QixJQUFJLENBQUMsWUFBWSxHQUFHLElBQUksQ0FBQztJQUM3QixDQUFDO0lBRUQ7Ozs7Ozs7T0FPRztJQUNLLFlBQVksQ0FBQyxJQUFZO1FBQzdCLElBQUksR0FBRyxDQUFDO1FBQ1IsSUFBSSxHQUFHLEdBQUcsQ0FBQyxDQUFDO1FBQ1osTUFBTSxDQUFDLEdBQUcsSUFBSSxDQUFDLEdBQUcsRUFBRSxDQUFDLENBQUM7UUFFdEIsSUFBSSxDQUFDLEdBQUcsR0FBRyxFQUFFO1lBQ1QsSUFBSSxDQUFDLENBQUMsR0FBRyxHQUFHLENBQUMsS0FBSyxHQUFHLEVBQUU7Z0JBQ25CLEdBQUcsR0FBRyxDQUFDLENBQUMsQ0FBQyxHQUFHLEVBQUUsQ0FBQyxJQUFJLENBQUMsQ0FBQyxHQUFHLElBQUksQ0FBQyxHQUFHLEVBQUUsQ0FBQyxDQUFDO2FBQ3ZDO2lCQUFNO2dCQUNILElBQUksQ0FBQyxDQUFDLEdBQUcsR0FBRyxDQUFDLEtBQUssR0FBRyxFQUFFO29CQUNuQixHQUFHLEdBQUcsQ0FBQyxDQUFDLENBQUMsR0FBRyxFQUFFLENBQUMsSUFBSSxDQUFDLENBQUMsR0FBRyxJQUFJLENBQUMsR0FBRyxFQUFFLENBQUMsQ0FBQztvQkFDcEMsR0FBRyxHQUFHLENBQUMsR0FBRyxJQUFJLENBQUMsQ0FBQyxHQUFHLElBQUksQ0FBQyxHQUFHLEVBQUUsQ0FBQyxDQUFDO2lCQUNsQztxQkFBTTtvQkFDSCxJQUFJLENBQUMsQ0FBQyxHQUFHLEdBQUcsQ0FBQyxLQUFLLEdBQUcsRUFBRTt3QkFDbkIsR0FBRyxHQUFHLENBQUMsQ0FBQyxDQUFDLEdBQUcsRUFBRSxDQUFDLElBQUksQ0FBQyxDQUFDLEdBQUcsSUFBSSxDQUFDLEdBQUcsRUFBRSxDQUFDLENBQUM7d0JBQ3BDLEdBQUcsR0FBRyxDQUFDLEdBQUcsSUFBSSxDQUFDLENBQUMsR0FBRyxJQUFJLENBQUMsR0FBRyxFQUFFLENBQUMsQ0FBQzt3QkFDL0IsR0FBRyxHQUFHLENBQUMsR0FBRyxJQUFJLENBQUMsQ0FBQyxHQUFHLElBQUksQ0FBQyxHQUFHLEVBQUUsQ0FBQyxDQUFDO3FCQUNsQzt5QkFBTTt3QkFDSCxHQUFHLEdBQUcsSUFBSSxDQUFDLEdBQUcsRUFBRSxDQUFDLENBQUM7d0JBQ2xCLEdBQUcsR0FBRyxDQUFDLEdBQUcsSUFBSSxDQUFDLENBQUMsR0FBRyxJQUFJLENBQUMsR0FBRyxFQUFFLENBQUMsQ0FBQzt3QkFDL0IsR0FBRyxHQUFHLENBQUMsR0FBRyxJQUFJLENBQUMsQ0FBQyxHQUFHLElBQUksQ0FBQyxHQUFHLEVBQUUsQ0FBQyxDQUFDO3dCQUMvQixHQUFHLEdBQUcsQ0FBQyxHQUFHLElBQUksQ0FBQyxDQUFDLEdBQUcsSUFBSSxDQUFDLEdBQUcsRUFBRSxDQUFDLENBQUM7cUJBQ2xDO2lCQUNKO2FBQ0o7U0FDSjthQUFNO1lBQ0gsR0FBRyxHQUFHLENBQUMsQ0FBQztTQUNYO1FBRUQsT0FBTyxDQUFDLEdBQUcsRUFBRSxHQUFHLENBQUMsQ0FBQztJQUN0QixDQUFDO0NBQ0o7QUFoWUQsNEJBZ1lDIn0=