"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const events_1 = require("events");
const net_1 = require("net");
const tls = require("tls");
const Receiver_1 = require("./Receiver");
const Transmitter_1 = require("./Transmitter");
const RosException_1 = require("../RosException");
const debug = require("debug");
const info = debug('routeros-api:connector:connector:info');
const error = debug('routeros-api:connector:connector:error');
/**
 * Connector class responsible for communicating with
 * the routeros via api, sending and receiving buffers.
 *
 * The main focus of this class is to be able to
 * construct and destruct dinamically by the RouterOSAPI class
 * when needed, so the authentication parameters don't
 * need to be changed every time we need to reconnect.
 */
class Connector extends events_1.EventEmitter {
    /**
     * Constructor which receive the options of the connection
     *
     * @param {Object} options
     */
    constructor(options) {
        super();
        /**
         * Connected status
         */
        this.connected = false;
        /**
         * Connecting status
         */
        this.connecting = false;
        /**
         * Closing status
         */
        this.closing = false;
        this.host = options.host;
        if (options.timeout)
            this.timeout = options.timeout;
        if (options.port)
            this.port = options.port;
        if (typeof options.tls === 'boolean' && options.tls)
            options.tls = {};
        if (typeof options.tls === 'object') {
            if (!options.port)
                this.port = 8729;
            this.tls = options.tls;
        }
    }
    /**
     * Connect to the routerboard
     *
     * @returns {Connector}
     */
    connect() {
        if (!this.connected) {
            if (!this.connecting) {
                this.connecting = true;
                if (this.tls) {
                    this.socket = tls.connect(this.port, this.host, this.tls, this.onConnect.bind(this));
                    this.transmitter = new Transmitter_1.Transmitter(this.socket);
                    this.receiver = new Receiver_1.Receiver(this.socket);
                    this.socket.on('data', this.onData.bind(this));
                    this.socket.on('tlsClientError', this.onError.bind(this));
                    this.socket.once('end', this.onEnd.bind(this));
                    this.socket.once('timeout', this.onTimeout.bind(this));
                    this.socket.once('fatal', this.onEnd.bind(this));
                    this.socket.on('error', this.onError.bind(this));
                    this.socket.setTimeout(this.timeout * 1000);
                    this.socket.setKeepAlive(true);
                }
                else {
                    this.socket = new net_1.Socket();
                    this.transmitter = new Transmitter_1.Transmitter(this.socket);
                    this.receiver = new Receiver_1.Receiver(this.socket);
                    this.socket.once('connect', this.onConnect.bind(this));
                    this.socket.once('end', this.onEnd.bind(this));
                    this.socket.once('timeout', this.onTimeout.bind(this));
                    this.socket.once('fatal', this.onEnd.bind(this));
                    this.socket.on('error', this.onError.bind(this));
                    this.socket.on('data', this.onData.bind(this));
                    this.socket.setTimeout(this.timeout * 1000);
                    this.socket.setKeepAlive(true);
                    this.socket.connect(this.port, this.host);
                }
            }
        }
        return this;
    }
    /**
     * Writes data through the open socket
     *
     * @param {Array} data
     * @returns {Connector}
     */
    write(data) {
        for (const line of data) {
            this.transmitter.write(line);
        }
        this.transmitter.write(null);
        return this;
    }
    /**
     * Register a tag to receive data
     *
     * @param {string} tag
     * @param {function} callback
     */
    read(tag, callback) {
        this.receiver.read(tag, callback);
    }
    /**
     * Unregister a tag, so it no longer waits for data
     * @param {string} tag
     */
    stopRead(tag) {
        this.receiver.stop(tag);
    }
    /**
     * Start closing the connection
     */
    close() {
        if (!this.closing) {
            this.closing = true;
            this.socket.end();
        }
    }
    /**
     * Destroy the socket, no more data
     * can be exchanged from now on and
     * this class itself must be recreated
     */
    destroy() {
        this.socket.destroy();
        this.removeAllListeners();
    }
    /**
     * Socket connection event listener.
     * After the connection is stablished,
     * ask the transmitter to run any
     * command stored over the pool
     *
     * @returns {function}
     */
    onConnect() {
        this.connecting = false;
        this.connected = true;
        info('Connected on %s', this.host);
        this.transmitter.runPool();
        this.emit('connected', this);
    }
    /**
     * Socket end event listener.
     * Terminates the connection after
     * the socket is released
     *
     * @returns {function}
     */
    onEnd() {
        this.emit('close', this);
        this.destroy();
    }
    /**
     * Socket error event listener.
     * Emmits the error while trying to connect and
     * destroys the socket.
     *
     * @returns {function}
     */
    onError(err) {
        err = new RosException_1.RosException(err.errno, err);
        error('Problem while trying to connect to %s. Error: %s', this.host, err.message);
        this.emit('error', err, this);
        this.destroy();
    }
    /**
     * Socket timeout event listener
     * Emmits timeout error and destroys the socket
     *
     * @returns {function}
     */
    onTimeout() {
        this.emit('timeout', new RosException_1.RosException('SOCKTMOUT', { seconds: this.timeout }), this);
        this.destroy();
    }
    /**
     * Socket data event listener
     * Receives the data and sends it to processing
     *
     * @returns {function}
     */
    onData(data) {
        info('Got data from the socket, will process it');
        this.receiver.processRawData(data);
    }
}
exports.Connector = Connector;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiQ29ubmVjdG9yLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vc3JjL2Nvbm5lY3Rvci9Db25uZWN0b3IudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7QUFBQSxtQ0FBc0M7QUFDdEMsNkJBQTZCO0FBQzdCLDJCQUEyQjtBQUMzQix5Q0FBc0M7QUFDdEMsK0NBQTRDO0FBQzVDLGtEQUErQztBQUMvQywrQkFBK0I7QUFFL0IsTUFBTSxJQUFJLEdBQUcsS0FBSyxDQUFDLHVDQUF1QyxDQUFDLENBQUM7QUFDNUQsTUFBTSxLQUFLLEdBQUcsS0FBSyxDQUFDLHdDQUF3QyxDQUFDLENBQUM7QUFFOUQ7Ozs7Ozs7O0dBUUc7QUFDSCxNQUFhLFNBQVUsU0FBUSxxQkFBWTtJQW1EdkM7Ozs7T0FJRztJQUNILFlBQVksT0FBWTtRQUNwQixLQUFLLEVBQUUsQ0FBQztRQTFCWjs7V0FFRztRQUNLLGNBQVMsR0FBWSxLQUFLLENBQUM7UUFFbkM7O1dBRUc7UUFDSyxlQUFVLEdBQVksS0FBSyxDQUFDO1FBRXBDOztXQUVHO1FBQ0ssWUFBTyxHQUFZLEtBQUssQ0FBQztRQWU3QixJQUFJLENBQUMsSUFBSSxHQUFHLE9BQU8sQ0FBQyxJQUFJLENBQUM7UUFDekIsSUFBSSxPQUFPLENBQUMsT0FBTztZQUFFLElBQUksQ0FBQyxPQUFPLEdBQUcsT0FBTyxDQUFDLE9BQU8sQ0FBQztRQUNwRCxJQUFJLE9BQU8sQ0FBQyxJQUFJO1lBQUUsSUFBSSxDQUFDLElBQUksR0FBRyxPQUFPLENBQUMsSUFBSSxDQUFDO1FBQzNDLElBQUksT0FBTyxPQUFPLENBQUMsR0FBRyxLQUFLLFNBQVMsSUFBSSxPQUFPLENBQUMsR0FBRztZQUFFLE9BQU8sQ0FBQyxHQUFHLEdBQUcsRUFBRSxDQUFDO1FBQ3RFLElBQUksT0FBTyxPQUFPLENBQUMsR0FBRyxLQUFLLFFBQVEsRUFBRTtZQUNqQyxJQUFJLENBQUMsT0FBTyxDQUFDLElBQUk7Z0JBQUUsSUFBSSxDQUFDLElBQUksR0FB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