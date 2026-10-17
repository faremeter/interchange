// A positive control for the browser-bundle test: a CommonJS require of a
// Node builtin, which the builtin recorder must see.

// eslint-disable-next-line @typescript-eslint/no-require-imports -- the fixture exists to bundle a CommonJS require
const os: unknown = require("node:os");

export default os;
