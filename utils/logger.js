/**
 * Simple logger - can be replaced with winston/pino later
 */
const logger = {
  error: (msg, err) => {
    console.error(msg, err?.message ?? err);
  },
  warn: (msg) => {
    if (process.env.NODE_ENV !== "test") console.warn(msg);
  },
  info: (msg) => {
    if (process.env.NODE_ENV !== "production") console.log(msg);
  },
};

module.exports = logger;
