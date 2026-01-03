"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Connector_1 = require("./connector/Connector");
const Channel_1 = require("./Channel");
const RosException_1 = require("./RosException");
const RStream_1 = require("./RStream");
const crypto = require("crypto");
const debug = require("debug");
const timers_1 = require("timers");
const events_1 = require("events");
const info = debug('routeros-api:api:info');
const error = debug('routeros-api:api:error');
/**
 * Creates a connection object with the credentials provided
 */
class RouterOSAPI extends events_1.EventEmitter {
    /**
     * Constructor, also sets the language of the thrown errors
     *
     * @param {Object} options
     */
    constructor(options) {
        super();
        /**
         * Connected flag
         */
        this.connected = false;
        /**
         * Connecting flag
         */
        this.connecting = false;
        /**
         * Closing flag
         */
        this.closing = false;
        /**
         * Counter for channels open
         */
        this.channelsOpen = 0;
        /**
         * Flag if the connection was held by the keepalive parameter
         * or keepaliveBy function
         */
        this.holdingConnectionWithKeepalive = false;
        this.registeredStreams = [];
        this.setOptions(options);
    }
    /**
     * Set connection options, affects before connecting
     *
     * @param options connection options
     */
    setOptions(options) {
        this.host = options.host;
        this.user = options.user;
        this.password = options.password;
        this.port = options.port || 8728;
        this.timeout = options.timeout || 10;
        this.tls = options.tls;
        this.keepalive = options.keepalive || false;
    }
    /**
     * Tries a connection to the routerboard with the provided credentials
     *
     * @returns {Promise}
     */
    connect() {
        if (this.connecting)
            return Promise.reject('ALRDYCONNECTING');
        if (this.connected)
            return Promise.resolve(this);
        info('Connecting on %s', this.host);
        this.connecting = true;
        this.connected = false;
        this.connector = new Connector_1.Connector({
            host: this.host,
            port: this.port,
            timeout: this.timeout,
            tls: this.tls,
        });
        return new Promise((resolve, reject) => {
            const endListener = (e) => {
                this.stopAllStreams();
                this.connected = false;
                this.connecting = false;
                if (e)
                    reject(e);
            };
            this.connector.once('error', endListener);
            this.connector.once('timeout', endListener);
            this.connector.once('close', () => {
                this.emit('close');
                endListener();
            });
            this.connector.once('connected', () => {
                this.login()
                    .then(() => {
                    this.connecting = false;
                    this.connected = true;
                    this.connector.removeListener('error', endListener);
                    this.connector.removeListener('timeout', endListener);
                    const connectedErrorListener = (e) => {
                        this.connected = false;
                        this.connecting = false;
                        this.emit('error', e);
                    };
                    this.connector.once('error', connectedErrorListener);
                    this.connector.once('timeout', connectedErrorListener);
                    if (this.keepalive)
                        this.keepaliveBy('#');
                    info('Logged in on %s', this.host);
                    resolve(this);
                })
                    .catch((e) => {
                    this.connecting = false;
                    this.connected = false;
                    reject(e);
                });
            });
            this.connector.connect();
        });
    }
    /**
     * Writes a command over the socket to the routerboard
     * on a new channel
     *
     * @param {string|Array} params
     * @param {Array<string|string[]>} moreParams
     * @returns {Promise}
     */
    write(params, ...moreParams) {
        params = this.concatParams(params, moreParams);
        let chann = this.openChannel();
        this.holdConnection();
        chann.once('close', () => {
            chann = null; // putting garbage collector to work :]
            this.decreaseChannelsOpen();
            this.releaseConnectionHold();
        });
        return chann.write(params);
    }
    /**
     * Writes a command over the socket to the routerboard
     * on a new channel and return an event of what happens
     * with the responses. Listen for 'data', 'done', 'trap' and 'close'
     * events.
     *
     * @param {string|Array} params
     * @param {Array<string|string[]>} moreParams
     * @returns {RStream}
     */
    writeStream(params, ...moreParams) {
        params = this.concatParams(params, moreParams);
        const stream = new RStream_1.RStream(this.openChannel(), params);
        stream.on('started', () => {
            this.holdConnection();
        });
        stream.on('stopped', () => {
            this.unregisterStream(stream);
            this.decreaseChannelsOpen();
            this.releaseConnectionHold();
        });
        stream.start();
        this.registerStream(stream);
        return stream;
    }
    /**
     * Returns a stream object for handling continuous data
     * flow.
     *
     * @param {string|Array} params
     * @param {function} callback
     * @returns {RStream}
     */
    stream(params = [], ...moreParams) {
        let callback = moreParams.pop();
        if (typeof callback !== 'function') {
            if (callback)
                moreParams.push(callback);
            callback = null;
        }
        params = this.concatParams(params, moreParams);
        const stream = new RStream_1.RStream(this.openChannel(), params, callback);
        stream.on('started', () => {
            this.holdConnection();
        });
        stream.on('stopped', () => {
            this.unregisterStream(stream);
            this.decreaseChannelsOpen();
            this.releaseConnectionHold();
            stream.removeAllListeners();
        });
        stream.start();
        stream.prepareDebounceEmptyData();
        this.registerStream(stream);
        return stream;
    }
    /**
     * Keep the connection alive by running a set of
     * commands provided instead of the random command
     *
     * @param {string|Array} params
     * @param {function} callback
     */
    keepaliveBy(params = [], ...moreParams) {
        this.holdingConnectionWithKeepalive = true;
        if (this.keptaliveby)
            timers_1.clearTimeout(this.keptaliveby);
        let callback = moreParams.pop();
        if (typeof callback !== 'function') {
            if (callback)
                moreParams.push(callback);
            callback = null;
        }
        params = this.concatParams(params, moreParams);
        const exec = () => {
            if (!this.closing) {
                if (this.keptaliveby)
                    timers_1.clearTimeout(this.keptaliveby);
                this.keptaliveby = setTimeout(() => {
                    this.write(params.slice())
                        .then((data) => {
                        if (typeof callback === 'function')
                            callback(null, data);
                        exec();
                    })
                        .catch((err) => {
                        if (typeof callback === 'function')
                            callback(err, null);
                        exec();
                    });
                }, (this.timeout * 1000) / 2);
            }
        };
        exec();
    }
    /**
     * Closes the connection.
     * It can be openned again without recreating
     * an object from this class.
     *
     * @returns {Promise}
     */
    close() {
        if (this.closing) {
            return Promise.reject(new RosException_1.RosException('ALRDYCLOSNG'));
        }
        if (!this.connected) {
            return Promise.resolve(this);
        }
        if (this.connectionHoldInterval) {
            timers_1.clearTimeout(this.connectionHoldInterval);
        }
        timers_1.clearTimeout(this.keptaliveby);
        this.stopAllStreams();
        return new Promise((resolve) => {
            this.closing = true;
            this.connector.once('close', () => {
                this.connector.destroy();
                this.connector = null;
                this.closing = false;
                this.connected = false;
                resolve(this);
            });
            this.connector.close();
        });
    }
    /**
     * Opens a new channel either for just writing or streaming
     *
     * @returns {Channel}
     */
    openChannel() {
        this.increaseChannelsOpen();
        return new Channel_1.Channel(this.connector);
    }
    increaseChannelsOpen() {
        this.channelsOpen++;
    }
    decreaseChannelsOpen() {
        this.channelsOpen--;
    }
    registerStream(stream) {
        this.registeredStreams.push(stream);
    }
    unregisterStream(stream) {
        this.registeredStreams = this.registeredStreams.filter((registeredStreams) => registeredStreams !== stream);
    }
    stopAllStreams() {
        for (const registeredStream of this.registeredStreams) {
            registeredStream.stop();
        }
    }
    /**
     * Holds the connection if keepalive wasn't set
     * so when a channel opens, ensure that we
     * receive a response before a timeout
     */
    holdConnection() {
        // If it's not the first connection to open
        // don't try to hold it again
        if (this.channelsOpen !== 1)
            return;
        if (this.connected && !this.holdingConnectionWithKeepalive) {
            if (this.connectionHoldInterval)
                timers_1.clearTimeout(this.connectionHoldInterval);
            const holdConnInterval = () => {
                this.connectionHoldInterval = setTimeout(() => {
                    let chann = new Channel_1.Channel(this.connector);
                    chann.on('close', () => {
                        chann = null;
                    });
                    chann
                        .write(['#'])
                        .then(() => {
                        holdConnInterval();
                    })
                        .catch(() => {
                        holdConnInterval();
                    });
                }, (this.timeout * 1000) / 2);
            };
            holdConnInterval();
        }
    }
    /**
     * Release the connection that was held
     * when waiting for responses from channels open
     */
    releaseConnectionHold() {
        // If there are channels still open
        // don't release the hold
        if (this.channelsOpen > 0)
            return;
        if (this.connectionHoldInterval)
            timers_1.clearTimeout(this.connectionHoldInterval);
    }
    /**
     * Login on the routerboard to provide
     * api functionalities, using the credentials
     * provided.
     *
     * @returns {Promise}
     */
    login() {
        this.connecting = true;
        info('Sending 6.43+ login to %s', this.host);
        return this.write('/login', [
            `=name=${this.user}`,
            `=password=${this.password}`,
        ])
            .then((data) => {
            if (data.length === 0) {
                info('6.43+ Credentials accepted on %s, we are connected', this.host);
                return Promise.resolve(this);
            }
            else if (data.length === 1) {
                info('Received challenge on %s, will send credentials. Data: %o', this.host, data);
                const challenge = Buffer.alloc(this.password.length + 17);
                const challengeOffset = this.password.length + 1;
                // Here we have 32 chars with hex encoded 16 bytes of challenge data
                const ret = data[0].ret;
                challenge.write(String.fromCharCode(0) + this.password);
                // To write 32 hec chars to buffer as bytes we need to write 16 bytes
                challenge.write(ret, challengeOffset, ret.length / 2, 'hex');
                const resp = '00' +
                    crypto
                        .createHash('MD5')
                        .update(challenge)
                        .digest('hex');
                return this.write('/login', [
                    '=name=' + this.user,
                    '=response=' + resp,
                ])
                    .then(() => {
                    info('Credentials accepted on %s, we are connected', this.host);
                    return Promise.resolve(this);
                })
                    .catch((err) => {
                    if (err.message === 'cannot log in' ||
                        err.message ===
                            'invalid user name or password (6)') {
                        err = new RosException_1.RosException('CANTLOGIN');
                    }
                    this.connector.destroy();
                    error("Couldn't loggin onto %s, Error: %O", this.host, err);
                    return Promise.reject(err);
                });
            }
            error('Unknown return from /login command on %s, data returned: %O', this.host, data);
            Promise.reject(new RosException_1.RosException('CANTLOGIN'));
        })
            .catch((err) => {
            if (err.message === 'cannot log in' ||
                err.message === 'invalid user name or password (6)') {
                err = new RosException_1.RosException('CANTLOGIN');
            }
            this.connector.destroy();
            error("Couldn't loggin onto %s, Error: %O", this.host, err);
            return Promise.reject(err);
        });
    }
    concatParams(firstParameter, parameters) {
        if (typeof firstParameter === 'string')
            firstParameter = [firstParameter];
        for (let parameter of parameters) {
            if (typeof parameter === 'string')
                parameter = [parameter];
            if (parameter.length > 0)
                firstParameter = firstParameter.concat(parameter);
        }
        return firstParameter;
    }
}
exports.RouterOSAPI = RouterOSAPI;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiUm91dGVyT1NBUEkuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi9zcmMvUm91dGVyT1NBUEkudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7QUFDQSxxREFBa0Q7QUFDbEQsdUNBQW9DO0FBQ3BDLGlEQUE4QztBQUU5Qyx1Q0FBb0M7QUFDcEMsaUNBQWlDO0FBQ2pDLCtCQUErQjtBQUMvQixtQ0FBc0M7QUFDdEMsbUNBQXNDO0FBR3RDLE1BQU0sSUFBSSxHQUFHLEtBQUssQ0FBQyx1QkFBdUIsQ0FBQyxDQUFDO0FBQzVDLE1BQU0sS0FBSyxHQUFHLEtBQUssQ0FBQyx3QkFBd0IsQ0FBQyxDQUFDO0FBRTlDOztHQUVHO0FBQ0gsTUFBYSxXQUFZLFNBQVEscUJBQVk7SUFnRnpDOzs7O09BSUc7SUFDSCxZQUFZLE9BQW9CO1FBQzVCLEtBQUssRUFBRSxDQUFDO1FBdkRaOztXQUVHO1FBQ0ksY0FBUyxHQUFZLEtBQUssQ0FBQztRQUVsQzs7V0FFRztRQUNJLGVBQVUsR0FBWSxLQUFLLENBQUM7UUFFbkM7O1dBRUc7UUFDSSxZQUFPLEdBQVksS0FBSyxDQUFDO1FBaUJoQzs7V0FFRztRQUNLLGlCQUFZLEdBQVcsQ0FBQyxDQUFDO1FBRWpDOzs7V0FHRztRQUNLLG1DQUE4QixHQUFZLEtBQUssQ0FBQztRQVFoRCxzQkFBaUIsR0FBYyxFQUFFLENBQUM7UUFTdEMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxPQUFPLENBQUMsQ0FBQztJQUM3QixDQUFDO0lBRUQ7Ozs7T0FJRztJQUNJLFVBQVUsQ0FBQyxPQUFvQjtRQUNsQyxJQUFJLENBQUMsSUFBSSxHQUFHLE9BQU8sQ0FBQyxJQUFJLENBQUM7UUFDekIsSUFBSSxDQUFDLElBQUksR0FBRyxPQUFPLENBQUMsSUFBSSxDQUFDO1FBQ3pCLElBQUksQ0FBQyxRQUFRLEdBQUcsT0FBTyxDQUFDLFFBQVEsQ0FBQztRQUNqQyxJQUFJLENBQUMsSUFBSSxHQUFHLE9BQU8sQ0FBQyxJQUFJLElBQUksSUFBSSxDQUFD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