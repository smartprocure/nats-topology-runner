# 0.5.0

* Propagate graceful shutdown from `nats-jobs`.
* `getTopologyId` fn. Pass `topologyId` to `loadSnapshot`, `persistSnapshot`, and `shouldResume`.
* Encoded data should be an object. See `MsgData` type.
* `ExtendedContext` type.

# 0.4.0

* Latest `nats-jobs`.

# 0.3.0

* Added `options` to `runTopologyWithNats`.
* Always persist and await last snapshot.
* `debounceMs` option to debounce persist calls.

# 0.2.0

* Simplify code based on `topology-runner` changes.

# 0.1.0

* Change API for `loadSnapshot` and `persistSnapshot` to make loading and persisting
more flexible.

# 0.0.2

* Add stream data to `meta` when calling `runTopology`.

# 0.0.1

* Initial release
