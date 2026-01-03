"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const events_1 = require("events");
const RosException_1 = require("./RosException");
const debug = require("debug");
const info = debug('routeros-api:channel:info');
const error = debug('routeros-api:channel:error');
/**
 * Channel class is responsible for generating
 * ids for the channels and writing over
 * the ids generated, while listening for
 * their responses
 */
class Channel extends events_1.EventEmitter {
    /**
     * Constructor
     *
     * @param {Connector} connector
     */
    constructor(connector) {
        super();
        /**
         * Data received related to the channel
         */
        this.data = [];
        /**
         * If received a trap instead of a positive response
         */
        this.trapped = false;
        /**
         * If is streaming content
         */
        this.streaming = false;
        this.id = Math.random().toString(36).substring(3);
        this.connector = connector;
        this.once('unknown', this.onUnknown.bind(this));
    }
    /**
     * Get the id of the channel
     *
     * @returns {string}
     */
    get Id() {
        return this.id;
    }
    /**
     * Get the connector used in the channel
     *
     * @returns {Connector}
     */
    get Connector() {
        return this.connector;
    }
    /**
     * Organize the data to be written over the socket with the id
     * generated. Adds a reader to the id provided, so we wait for
     * the data.
     *
     * @param {Array} params
     * @returns {Promise}
     */
    write(params, isStream = false, returnPromise = true) {
        this.streaming = isStream;
        params.push('.tag=' + this.id);
        if (returnPromise) {
            this.on('data', (packet) => this.data.push(packet));
            return new Promise((resolve, reject) => {
                this.once('done', (data) => resolve(data));
                this.once('trap', (data) => reject(new Error(data.message)));
                this.readAndWrite(params);
            });
        }
        this.readAndWrite(params);
        return;
    }
    /**
     * Closes the channel, algo asking for
     * the connector to remove the reader.
     * If streaming, not forcing will only stop
     * the reader, not the listeners of the events
     *
     * @param {boolean} force - force closing by removing all listeners
     */
    close(force = false) {
        this.emit('close');
        if (!this.streaming || force) {
            this.removeAllListeners();
        }
        this.connector.stopRead(this.id);
    }
    /**
     * Register the reader for the tag and write the params over
     * the socket
     *
     * @param {Array} params
     */
    readAndWrite(params) {
        this.connector.read(this.id, (packet) => this.processPacket(packet));
        this.connector.write(params);
    }
    /**
     * Process the data packet received to
     * figure out the answer to give to the
     * channel listener, either if it's just
     * the data we were expecting or if
     * a trap was given.
     *
     * @param {Array} packet
     */
    processPacket(packet) {
        const reply = packet.shift();
        info('Processing reply %s with data %o', reply, packet);
        const parsed = this.parsePacket(packet);
        if (reply === '!trap') {
            this.trapped = true;
            this.emit('trap', parsed);
            return;
        }
        if (packet.length > 0 && !this.streaming)
            this.emit('data', parsed);
        switch (reply) {
            case '!re':
                if (this.streaming)
                    this.emit('stream', parsed);
                break;
            case '!done':
                if (!this.trapped)
                    this.emit('done', this.data);
                this.close();
                break;
            case '!empty':
                if (!this.trapped)
                    this.emit('done', this.data);
                this.close();
                break;
            default:
                this.emit('unknown', reply);
                this.close();
                break;
        }
    }
    /**
     * Parse the packet line, separating the key from the data.
     * Ex: transform '=interface=ether2' into object {interface:'ether2'}
     *
     * @param {Array} packet
     * @return {Object}
     */
    parsePacket(packet) {
        const obj = {};
        for (const line of packet) {
            const linePair = line.split('=');
            linePair.shift(); // remove empty index
            obj[linePair.shift()] = linePair.join('=');
        }
        info('Parsed line, got %o as result', obj);
        return obj;
    }
    /**
     * Waits for the unknown event.
     * It shouldn't happen, but if it does, throws the error and
     * stops the channel
     *
     * @param {string} reply
     * @returns {function}
     */
    onUnknown(reply) {
        throw new RosException_1.RosException('UNKNOWNREPLY', { reply: reply });
    }
}
exports.Channel = Channel;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiQ2hhbm5lbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uL3NyYy9DaGFubmVsLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7O0FBQUEsbUNBQXNDO0FBRXRDLGlEQUE4QztBQUM5QywrQkFBK0I7QUFHL0IsTUFBTSxJQUFJLEdBQUcsS0FBSyxDQUFDLDJCQUEyQixDQUFDLENBQUM7QUFDaEQsTUFBTSxLQUFLLEdBQUcsS0FBSyxDQUFDLDRCQUE0QixDQUFDLENBQUM7QUFFbEQ7Ozs7O0dBS0c7QUFDSCxNQUFhLE9BQVEsU0FBUSxxQkFBWTtJQTBCckM7Ozs7T0FJRztJQUNILFlBQVksU0FBb0I7UUFDNUIsS0FBSyxFQUFFLENBQUM7UUFyQlo7O1dBRUc7UUFDSyxTQUFJLEdBQVUsRUFBRSxDQUFDO1FBRXpCOztXQUVHO1FBQ0ssWUFBTyxHQUFZLEtBQUssQ0FBQztRQUVqQzs7V0FFRztRQUNLLGNBQVMsR0FBWSxLQUFLLENBQUM7UUFTL0IsSUFBSSxDQUFDLEVBQUUsR0FBRyxJQUFJLENBQUMsTUFBTSxFQUFFLENBQUMsUUFBUSxDQUFDLEVBQUUsQ0FBQyxDQUFDLFNBQVMsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUNsRCxJQUFJLENBQUMsU0FBUyxHQUFHLFNBQVMsQ0FBQztRQUMzQixJQUFJLENBQUMsSUFBSSxDQUFDLFNBQVMsRUFBRSxJQUFJLENBQUMsU0FBUyxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDO0lBQ3BELENBQUM7SUFFRDs7OztPQUlHO0lBQ0gsSUFBSSxFQUFFO1FBQ0YsT0FBTyxJQUFJLENBQUMsRUFBRSxDQUFDO0lBQ25CLENBQUM7SUFFRDs7OztPQUlHO0lBQ0gsSUFBSSxTQUFTO1FBQ1QsT0FBTyxJQUFJLENBQUMsU0FBUyxDQUFDO0lBQzFCLENBQUM7SUFFRDs7Ozs7OztPQU9HO0lBQ0ksS0FBSyxDQUNSLE1BQWdCLEVBQ2hCLFFBQVEsR0FBRyxLQUFLLEVBQ2hCLGFBQWEsR0FBRyxJQUFJO1FBRXBC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