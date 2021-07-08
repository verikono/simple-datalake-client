"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.logger = void 0;
const simple_node_logger_1 = require("simple-node-logger");
exports.logger = simple_node_logger_1.createSimpleLogger();
const level = process.env.LOGLEVEL || 'DEBUG';
exports.logger.setLevel(level);
//# sourceMappingURL=logger.js.map