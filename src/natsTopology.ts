import { runTopology, resumeTopology, Snapshot } from 'topology-runner'
import { JsMsg } from 'nats'
import _ from 'lodash/fp'
import { RunTopology } from './types'
import _debug from 'debug'

const debug = _debug('nats-topology-runner')

export const getStreamDataFromMsg = (msg: JsMsg) => {
  const { stream, streamSequence } = msg.info
  return { stream, streamSequence }
}

/**
 * Returns a fn that takes a JsMsg and runs the topology
 * with the data off the message. Automatically resumes a topology
 * if the redeliveryCount is > 1. Regardless of whether the topology
 * succeeds or fails, the last snapshot will be persisted and awaited.
 *
 * Pass value for `debounceMs` to minimize how many times `persistSnapshot`
 * is called.
 */
export const runTopologyWithNats: RunTopology =
  (spec, dag, fns, options) => async (msg) => {
    const { unpack, loadSnapshot, persistSnapshot } = fns
    const { debounceMs } = options || {}
    const data = unpack(msg.data)
    const numAttempts = msg.info.redeliveryCount
    const isRedelivery = numAttempts > 1
    debug('Redelivery %s', isRedelivery)
    const { emitter, promise, getSnapshot } = isRedelivery
      ? // Resume topology based on unique stream data
        resumeTopology(spec, await loadSnapshot(msg))
      : // Run topoloty with data from msg
        runTopology(spec, dag, { ...options, data })
    const streamData = getStreamDataFromMsg(msg)
    const persist = (snapshot: Snapshot) => {
      const streamSnapshot = { ...snapshot, ...streamData, numAttempts }
      debug('Stream Snapshot %O', streamSnapshot)
      // Persist snapshot
      return persistSnapshot(streamSnapshot, msg)
    }
    // Emit data with an optional debounce delay
    emitter.on('data', debounceMs ? _.debounce(debounceMs, persist) : persist)
    try {
      // Wait for the topology to finish
      await promise
    } finally {
      // Make sure we persist the final snapshot
      await persist(getSnapshot())
    }
  }
