"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.RxConflictResolutionStrategy = void 0;
/**
 * Types for the shared worker storage
 */
/**
 * Default settings for the shared worker storage
 */
/**
 * Message types for communication between main thread and worker
 */
/**
 * Base message interface
 */
/**
 * Response message from the worker
 */
/**
 * Conflict resolution strategy
 */
var RxConflictResolutionStrategy = exports.RxConflictResolutionStrategy = /*#__PURE__*/function (RxConflictResolutionStrategy) {
  RxConflictResolutionStrategy["LAST_WRITE_WINS"] = "last-write-wins";
  return RxConflictResolutionStrategy;
}({});
/**
 * Conflict resolution task
 */
//# sourceMappingURL=shared-worker-types.js.map