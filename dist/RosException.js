"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const messages_1 = require("./messages");
/**
 * RouterOS Exception Handler
 */
class RosException extends Error {
    constructor(errno, extras) {
        super();
        // Maintains proper stack trace for where our error was thrown
        Error.captureStackTrace(this, this.constructor);
        this.name = this.constructor.name;
        // Custom debugging information
        this.errno = errno;
        let message = messages_1.default[errno];
        if (message) {
            for (const key in extras) {
                if (extras.hasOwnProperty(key)) {
                    message = message.replace(`{{${key}}}`, extras[key]);
                }
            }
            this.message = message;
        }
    }
}
exports.RosException = RosException;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiUm9zRXhjZXB0aW9uLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vc3JjL1Jvc0V4Y2VwdGlvbi50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOztBQUFBLHlDQUFrQztBQUVsQzs7R0FFRztBQUNILE1BQWEsWUFBYSxTQUFRLEtBQUs7SUFHbkMsWUFBWSxLQUFhLEVBQUUsTUFBWTtRQUNuQyxLQUFLLEVBQUUsQ0FBQztRQUVSLDhEQUE4RDtRQUM5RCxLQUFLLENBQUMsaUJBQWlCLENBQUMsSUFBSSxFQUFFLElBQUksQ0FBQyxXQUFXLENBQUMsQ0FBQztRQUVoRCxJQUFJLENBQUMsSUFBSSxHQUFHLElBQUksQ0FBQyxXQUFXLENBQUMsSUFBSSxDQUFDO1FBRWxDLCtCQUErQjtRQUMvQixJQUFJLENBQUMsS0FBSyxHQUFHLEtBQUssQ0FBQztRQUVuQixJQUFJLE9BQU8sR0FBRyxrQkFBUSxDQUFDLEtBQUssQ0FBQyxDQUFDO1FBRTlCLElBQUksT0FBTyxFQUFFO1lBQ1QsS0FBSyxNQUFNLEdBQUcsSUFBSSxNQUFNLEVBQUU7Z0JBQ3RCLElBQUksTUFBTSxDQUFDLGNBQWMsQ0FBQyxHQUFHLENBQUMsRUFBRTtvQkFDNUIsT0FBTyxHQUFHLE9BQU8sQ0FBQyxPQUFPLENBQUMsS0FBSyxHQUFHLElBQUksRUFBRSxNQUFNLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQztpQkFDeEQ7YUFDSjtZQUNELElBQUksQ0FBQyxPQUFPLEdBQUcsT0FBTyxDQUFDO1NBQzFCO0lBQ0wsQ0FBQztDQUNKO0FBekJELG9DQXlCQyJ9