import {createSimpleLogger} from 'simple-node-logger';
export const logger = createSimpleLogger();
const level = process.env.LOGLEVEL || 'DEBUG';
logger.setLevel(level);