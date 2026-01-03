"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const events_1 = require("events");
const Channel_1 = require("./Channel");
const RosException_1 = require("./RosException");
const timers_1 = require("timers");
const utils_1 = require("./utils");
/**
 * Stream class is responsible for handling
 * continuous data from some parts of the
 * routeros, like /ip/address/listen or
 * /tool/torch which keeps sending data endlessly.
 * It is also possible to pause/resume/stop generated
 * streams.
 */
class RStream extends events_1.EventEmitter {
    /**
     * Constructor, it also starts the streaming after construction
     *
     * @param {Channel} channel
     * @param {Array} params
     * @param {function} callback
     */
    constructor(channel, params, callback) {
        super();
        /** Flag for turning on empty data debouncing */
        this.shouldDebounceEmptyData = false;
        /**
         * If is streaming flag
         */
        this.streaming = true;
        /**
         * If is pausing flag
         */
        this.pausing = false;
        /**
         * If is paused flag
         */
        this.paused = false;
        /**
         * If is stopping flag
         */
        this.stopping = false;
        /**
         * If is stopped flag
         */
        this.stopped = false;
        /**
         * If got a trap error
         */
        this.trapped = false;
        /**
         * Save the current section of the packet, if has any
         */
        this.currentSection = null;
        this.forcelyStop = false;
        /**
         * Store the current section in a single
         * array before sending when another section comes
         */
        this.currentSectionPacket = [];
        this.channel = channel;
        this.params = params;
        this.callback = callback;
    }
    /**
     * Function to receive the callback which
     * will receive data, if not provided over the
     * constructor or changed later after the streaming
     * have started.
     *
     * @param {function} callback
     */
    data(callback) {
        this.callback = callback;
    }
    /**
     * Resume the paused stream, using the same channel
     *
     * @returns {Promise}
     */
    resume() {
        if (this.stopped || this.stopping)
            return Promise.reject(new RosException_1.RosException('STREAMCLOSD'));
        if (!this.streaming) {
            this.pausing = false;
            this.start();
            this.streaming = true;
        }
        return Promise.resolve();
    }
    /**
     * Pause the stream, but don't destroy the channel
     *
     * @returns {Promise}
     */
    pause() {
        if (this.stopped || this.stopping)
            return Promise.reject(new RosException_1.RosException('STREAMCLOSD'));
        if (this.pausing || this.paused)
            return Promise.resolve();
        if (this.streaming) {
            this.pausing = true;
            return this.stop(true)
                .then(() => {
                this.pausing = false;
                this.paused = true;
                return Promise.resolve();
            })
                .catch((err) => {
                return Promise.reject(err);
            });
        }
        return Promise.resolve();
    }
    /**
     * Stop the stream entirely, can't re-stream after
     * this if called directly.
     *
     * @returns {Promise}
     */
    stop(pausing = false) {
        if (this.stopped || this.stopping)
            return Promise.resolve();
        if (!pausing)
            this.forcelyStop = true;
        if (this.paused) {
            this.streaming = false;
            this.stopping = false;
            this.stopped = true;
            if (this.channel)
                this.channel.close(true);
            return Promise.resolve();
        }
        if (!this.pausing)
            this.stopping = true;
        let chann = new Channel_1.Channel(this.channel.Connector);
        chann.on('close', () => {
            chann = null;
        });
        if (this.debounceSendingEmptyData)
            this.debounceSendingEmptyData.cancel();
        return chann
            .write(['/cancel', '=tag=' + this.channel.Id])
            .then(() => {
            this.streaming = false;
            if (!this.pausing) {
                this.stopping = false;
                this.stopped = true;
            }
            this.emit('stopped');
            return Promise.resolve();
        })
            .catch((err) => {
            return Promise.reject(err);
        });
    }
    /**
     * Alias for stop()
     */
    close() {
        return this.stop();
    }
    /**
     * Write over the connection and start the stream
     */
    start() {
        if (!this.stopped && !this.stopping) {
            this.channel.on('close', () => {
                if (this.forcelyStop || (!this.pausing && !this.paused)) {
                    if (!this.trapped)
                        this.emit('done');
                    this.emit('close');
                }
                this.stopped = false;
            });
            this.channel.on('stream', (packet) => {
                if (this.debounceSendingEmptyData)
                    this.debounceSendingEmptyData.run();
                this.onStream(packet);
            });
            this.channel.once('trap', this.onTrap.bind(this));
            this.channel.once('done', this.onDone.bind(this));
            this.channel.write(this.params.slice(), true, false);
            this.emit('started');
            if (this.shouldDebounceEmptyData)
                this.prepareDebounceEmptyData();
        }
    }
    prepareDebounceEmptyData() {
        this.shouldDebounceEmptyData = true;
        const intervalParam = this.params.find((param) => {
            return /=interval=/.test(param);
        });
        let interval = 2000;
        if (intervalParam) {
            const val = intervalParam.split('=')[2];
            interval = parseInt(val, null) * 1000;
        }
        this.debounceSendingEmptyData = utils_1.debounce(() => {
            if (!this.stopped ||
                !this.stopping ||
                !this.paused ||
                !this.pausing) {
                this.onStream([]);
                this.debounceSendingEmptyData.run();
            }
        }, interval + 300);
    }
    /**
     * When receiving the stream packet, give it to
     * the callback
     *
     * @returns {function}
     */
    onStream(packet) {
        this.emit('data', packet);
        if (this.callback) {
            if (packet['.section']) {
                timers_1.clearTimeout(this.sectionPacketSendingTimeout);
                const sendData = () => {
                    this.callback(null, this.currentSectionPacket.slice(), this);
                    this.currentSectionPacket = [];
                };
                this.sectionPacketSendingTimeout = timers_1.setTimeout(sendData.bind(this), 300);
                if (this.currentSectionPacket.length > 0 &&
                    packet['.section'] !== this.currentSection) {
                    timers_1.clearTimeout(this.sectionPacketSendingTimeout);
                    sendData();
                }
                this.currentSection = packet['.section'];
                this.currentSectionPacket.push(packet);
            }
            else {
                this.callback(null, packet, this);
            }
        }
    }
    /**
     * When receiving a trap over the connection,
     * when pausing, will receive a 'interrupted' message,
     * this will not be considered as an error but a flag
     * for the pause and resume function
     *
     * @returns {function}
     */
    onTrap(data) {
        if (data.message === 'interrupted') {
            this.streaming = false;
        }
        else {
            this.stopped = true;
            this.trapped = true;
            if (this.callback) {
                this.callback(new Error(data.message), null, this);
            }
            else {
                this.emit('error', data);
            }
            this.emit('trap', data);
        }
    }
    /**
     * When the channel stops sending data.
     * It will close the channel if the
     * intention was stopping it.
     *
     * @returns {function}
     */
    onDone() {
        if (this.stopped && this.channel) {
            this.channel.close(true);
        }
    }
}
exports.RStream = RStream;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiUlN0cmVhbS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uL3NyYy9SU3RyZWFtLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7O0FBQUEsbUNBQXNDO0FBQ3RDLHVDQUFvQztBQUNwQyxpREFBOEM7QUFDOUMsbUNBQWtEO0FBQ2xELG1DQUFtQztBQUVuQzs7Ozs7OztHQU9HO0FBQ0gsTUFBYSxPQUFRLFNBQVEscUJBQVk7SUE2RXJDOzs7Ozs7T0FNRztJQUNILFlBQ0ksT0FBZ0IsRUFDaEIsTUFBZ0IsRUFDaEIsUUFBK0Q7UUFFL0QsS0FBSyxFQUFFLENBQUM7UUEvRFosZ0RBQWdEO1FBQ3hDLDRCQUF1QixHQUFZLEtBQUssQ0FBQztRQUVqRDs7V0FFRztRQUNLLGNBQVMsR0FBWSxJQUFJLENBQUM7UUFFbEM7O1dBRUc7UUFDSyxZQUFPLEdBQVksS0FBSyxDQUFDO1FBRWpDOztXQUVHO1FBQ0ssV0FBTSxHQUFZLEtBQUssQ0FBQztRQUVoQzs7V0FFRztRQUNLLGFBQVEsR0FBWSxLQUFLLENBQUM7UUFFbEM7O1dBRUc7UUFDSyxZQUFPLEdBQVksS0FBSyxDQUFDO1FBRWpDOztXQUVHO1FBQ0ssWUFBTyxHQUFZLEtBQUssQ0FBQztRQUVqQzs7V0FFRztRQUNLLG1CQUFjLEdBQVcsSUFBSSxDQUFDO1FBRTlCLGdCQUFXLEdBQVksS0FBSyxDQUFDO1FBRXJDOzs7V0FHRztRQUNLLHlCQUFvQixHQUFVLEVBQUUsQ0FBQztRQW9CckMsSUFBSSxDQUFDLE9BQU8sR0FBRyxPQUFPLENBQUM7UUFDdkIsSUFBSSxDQUFDLE1BQU0sR0FBRyxNQUFNLENBQUM7UUFDckIsSUFBSSxDQUFDLFFBQVEsR0FBRyxRQUFRLENBQUM7SUFDN0IsQ0FBQztJQUVEOzs7Ozs7O09BT0c7SUFDSSxJQUFJLENBQ1AsUUFBOEQ7UUFFOUQsSUFBSSxDQUFDLFFBQVEsR0FBRyxRQUFRLENBQUM7SUFDN0IsQ0FBQztJQUVEOzs7O09BSUc7SUFDSSxNQUFNO1FBQ1QsSUFBSSxJQUFJ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