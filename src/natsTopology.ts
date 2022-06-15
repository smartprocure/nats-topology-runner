import { runTopology, resumeTopology, Snapshot } from 'topology-runner'
import { JsMsg } from 'nats'
import _ from 'lodash'
import { RunTopology } from './types'
import _debug from 'debug'

const debug = _debug('nats-topology-runner')

/**
 * Get the name of the stream and the sequence number of the message
 */
export const getStreamDataFromMsg = (msg: JsMsg) => {
  const { stream, streamSequence } = msg.info
  return { stream, streamSequence }
}

/**
 * Resume the topology when the message is delivered the second, third, etc.
 * time.
 */
const defShouldResume = (msg: JsMsg) => msg.info.redeliveryCount > 1

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
  (spec, dag, fns, options) => async (msg, context) => {
    const {
      unpack,
      loadSnapshot,
      persistSnapshot,
      shouldResume = defShouldResume,
    } = fns
    // Augment meta with context and msg
    const extendedContext = { ...context, msg, ...options?.context }
    const { debounceMs } = options || {}
    const data = unpack(msg.data)
    const numAttempts = msg.info.redeliveryCount
    debug('Num attempts %d', numAttempts)
    const resuming = await shouldResume(msg)
    debug('Resuming %s', resuming)
    const { emitter, promise, getSnapshot } = resuming
      ? // Resume topology based on unique stream data
        resumeTopology(spec, await loadSnapshot(msg), {
          context: extendedContext,
        })
      : // Run topology with data from msg
        runTopology(spec, dag, { ...options, data, context: extendedContext })
    const streamData = getStreamDataFromMsg(msg)
    const persist = (snapshot: Snapshot) => {
      const streamSnapshot = { ...snapshot, ...streamData, numAttempts }
      debug('Stream Snapshot %O', streamSnapshot)
      // Persist snapshot
      return persistSnapshot(streamSnapshot, msg)
    }
    const debounced = _.debounce(persist, debounceMs)
    // Emit data with an optional debounce delay
    emitter.on('data', debounceMs ? debounced : persist)
    try {
      // Wait for the topology to finish
      await promise
    } finally {
      // Cancel any delayed invocations
      debounced.cancel()
      // Make sure we persist the final snapshot
      await persist(getSnapshot())
    }
  }
