"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const iconv = require('iconv-lite');
const debug = require("debug");
const info = debug('routeros-api:connector:transmitter:info');
const error = debug('routeros-api:connector:transmitter:error');
/**
 * Class responsible for transmitting data over the
 * socket to the routerboard
 */
class Transmitter {
    /**
     * Constructor
     *
     * @param socket
     */
    constructor(socket) {
        /**
         * Pool of data to be sent after the socket connects
         */
        this.pool = [];
        this.socket = socket;
    }
    /**
     * Write data over the socket, if it not writable yet,
     * save over the pool to be ran after
     *
     * @param {string} data
     */
    write(data) {
        const encodedData = this.encodeString(data);
        if (!this.socket.writable || this.pool.length > 0) {
            info('Socket not writable, saving %o in the pool', data);
            this.pool.push(encodedData);
        }
        else {
            info('Writing command %s over the socket', data);
            this.socket.write(encodedData);
        }
    }
    /**
     * Writes all data stored in the pool
     */
    runPool() {
        info('Running stacked command pool');
        let data;
        while (this.pool.length > 0) {
            data = this.pool.shift();
            this.socket.write(data);
        }
    }
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
    encodeString(str) {
        if (str === null)
            return String.fromCharCode(0);
        const encoded = iconv.encode(str, 'win1252');
        let data;
        let len = encoded.length;
        let offset = 0;
        if (len < 0x80) {
            data = Buffer.alloc(len + 1);
            data[offset++] = len;
        }
        else if (len < 0x4000) {
            data = Buffer.alloc(len + 2);
            len |= 0x8000;
            data[offset++] = (len >> 8) & 0xff;
            data[offset++] = len & 0xff;
        }
        else if (len < 0x200000) {
            data = Buffer.alloc(len + 3);
            len |= 0xc00000;
            data[offset++] = (len >> 16) & 0xff;
            data[offset++] = (len >> 8) & 0xff;
            data[offset++] = len & 0xff;
        }
        else if (len < 0x10000000) {
            data = Buffer.alloc(len + 4);
            len |= 0xe0000000;
            data[offset++] = (len >> 24) & 0xff;
            data[offset++] = (len >> 16) & 0xff;
            data[offset++] = (len >> 8) & 0xff;
            data[offset++] = len & 0xff;
        }
        else {
            data = Buffer.alloc(len + 5);
            data[offset++] = 0xf0;
            data[offset++] = (len >> 24) & 0xff;
            data[offset++] = (len >> 16) & 0xff;
            data[offset++] = (len >> 8) & 0xff;
            data[offset++] = len & 0xff;
        }
        data.fill(encoded, offset);
        return data;
    }
}
exports.Transmitter = Transmitter;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiVHJhbnNtaXR0ZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi9zcmMvY29ubmVjdG9yL1RyYW5zbWl0dGVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7O0FBQ0EsTUFBTSxLQUFLLEdBQUcsT0FBTyxDQUFDLFlBQVksQ0FBQyxDQUFDO0FBQ3BDLCtCQUErQjtBQUUvQixNQUFNLElBQUksR0FBRyxLQUFLLENBQUMseUNBQXlDLENBQUMsQ0FBQztBQUM5RCxNQUFNLEtBQUssR0FBRyxLQUFLLENBQUMsMENBQTBDLENBQUMsQ0FBQztBQUVoRTs7O0dBR0c7QUFDSCxNQUFhLFdBQVc7SUFXcEI7Ozs7T0FJRztJQUNILFlBQVksTUFBYztRQVYxQjs7V0FFRztRQUNLLFNBQUksR0FBYSxFQUFFLENBQUM7UUFReEIsSUFBSSxDQUFDLE1BQU0sR0FBRyxNQUFNLENBQUM7SUFDekIsQ0FBQztJQUVEOzs7OztPQUtHO0lBQ0ksS0FBSyxDQUFDLElBQVk7UUFDckIsTUFBTSxXQUFXLEdBQUcsSUFBSSxDQUFDLFlBQVksQ0FBQyxJQUFJLENBQUMsQ0FBQztRQUM1QyxJQUFJLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQyxRQUFRLElBQUksSUFBSSxDQUFDLElBQUksQ0FBQyxNQUFNLEdBQUcsQ0FBQyxFQUFFO1lBQy9DLElBQUksQ0FBQyw0Q0FBNEMsRUFBRSxJQUFJLENBQUMsQ0FBQztZQUN6RCxJQUFJLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxXQUFXLENBQUMsQ0FBQztTQUMvQjthQUFNO1lBQ0gsSUFBSSxDQUFDLG9DQUFvQyxFQUFFLElBQUksQ0FBQyxDQUFDO1lBQ2pELElBQUksQ0FBQyxNQUFNLENBQUMsS0FBSyxDQUFDLFdBQVcsQ0FBQyxDQUFDO1NBQ2xDO0lBQ0wsQ0FBQztJQUVEOztPQUVHO0lBQ0ksT0FBTztRQUNWLElBQUksQ0FBQyw4QkFBOEIsQ0FBQyxDQUFDO1FBQ3JDLElBQUksSUFBSSxDQUFDO1FBQ1QsT0FBTyxJQUFJ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