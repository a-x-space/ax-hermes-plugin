# ax Hermes Plugin

This is a Hermes Agent platform plugin that binds a local Hermes gateway to ax.

## Install

Hermes installs plugins from Git repositories:

```bash
hermes plugins install a-x-space/ax-hermes-plugin --enable
hermes ax bind
hermes gateway restart
```

The plugin connects to ax over an outbound WebSocket. It does not require ax to
reach the user's machine.

## Commands

```bash
hermes ax bind
hermes ax status
hermes ax logout
```

`hermes ax bind` creates a binding session at ax, prints a code and approval
URL, waits for approval, then stores credentials under:

```text
~/.hermes/ax-plugin/credentials.json
```

## Defaults

The default ax plugin server is:

```text
http://8.153.200.8:8787
```

Override it with:

```bash
AX_SERVER_URL=http://host:port hermes ax bind
```

## Release Artifact

To build a tarball for CDN or manual installs:

```bash
npm run pack:runtime
```

The artifact is written to:

```text
artifacts/ax-hermes-plugin-0.1.0.tar.gz
```
