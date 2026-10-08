// Probe for the module-load contract: importing this package installs a
// default console sink. Runs as a child process because the contract is
// unobservable in-process -- the installer returns early when a config
// already exists, so under a shared module registry some earlier import
// has almost always triggered the install. Reports to stdout so the
// report is not routed by the configuration under test.
import { getConfig } from "./index";

const config = getConfig();
process.stdout.write(
  JSON.stringify({
    installed: config !== null,
    sinks: config === null ? [] : Object.keys(config.sinks).sort(),
  }),
);
