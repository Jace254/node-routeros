/**
 * RouterOS Exception Handler
 */
export declare class RosException extends Error {
    errno: string;
    constructor(errno: string, extras?: any);
}
