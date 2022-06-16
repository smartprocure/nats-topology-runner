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
 * Pass a value for `debounceMs` to prevent rapid calls to `persistSnapshot`.
 * To customize the resumption behavior, pass a fn for `shouldResume`.
 */
export const runTopologyWithNats: RunTopology =
  (spec, dag, fns, options) => async (msg, context) => {
    const {
      unpack,
      loadSnapshot,
      persistSnapshot,
      shouldResume = defShouldResume,
    } = fns
    // Merge context from nats-jobs, msg, and any optional user context
    const extendedContext = { ...context, msg, ...options?.context }
    const { debounceMs } = options || {}
    // Unpack data
    const data = unpack(msg.data)
    const numAttempts = msg.info.redeliveryCount
    debug('Num attempts %d', numAttempts)
    // Should the topology be resumed
    const resuming = await shouldResume(msg)
    debug('Resuming %s', resuming)
    const { emitter, promise, getSnapshot } = resuming
      ? // Resume topology based on uniqueness defined in loadSnapshot
        resumeTopology(spec, await loadSnapshot(msg), {
          context: extendedContext,
        })
      : // Run topology with data from msg
        runTopology(spec, dag, { ...options, data, context: extendedContext })
    // Get the stream data
    const streamData = getStreamDataFromMsg(msg)
    // Persist
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
      // Make sure we persist and await the final snapshot
      await persist(getSnapshot())
    }
  }
