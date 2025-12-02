# jepsen.nats

Jepsen tests for the NATS JetStream distributed streaming system.

## Usage

To run the full test suite (assuming you have a [Jepsen environment](https://github.com/jepsen-io/jepsen?tab=readme-ov-file#setting-up-a-jepsen-environment)):

```
lein run test-all
```

There are a host of options available for controlling fault injection, request rates, concurrency, and so on:

```
lein run test --help
```

## License

Copyright © 2025 Jepsen, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
