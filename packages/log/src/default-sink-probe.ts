// Probe for the module-load contract: importing this package installs a
// default console sink. It runs as a child process because that contract
// cannot be observed in-process -- `installDefaultConsoleSink` returns early
// when a config already exists, and under a shared module registry some
// earlier file has almost always triggered the install already, so an
// in-process check reads back whatever that file left behind.
//
// Reports to stdout rather than through a logger, so the report cannot be
// routed by the very configuration under test.
import { getConfig } from "./index";

const config = getConfig();
process.stdout.write(
  JSON.stringify({
    installed: config !== null,
    sinks: config === null ? [] : Object.keys(config.sinks).sort(),
  }),
);
