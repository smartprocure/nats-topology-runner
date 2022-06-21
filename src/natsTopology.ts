import { runTopology, resumeTopology, Snapshot } from 'topology-runner'
import { JsMsg } from 'nats'
import _ from 'lodash'
import { RunTopology, MsgData } from './types'
import _debug from 'debug'

const debug = _debug('nats-topology-runner')

const defGetTopologyId = (msg: JsMsg) => {
  const { stream, streamSequence } = msg.info
  return `${stream}-${streamSequence}`
}

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
      shouldResume = async (topologyId: string) => {
        const snapshot = await loadSnapshot(topologyId)
        // Snapshot was found and status is not completed
        return snapshot && snapshot?.status !== 'completed'
      },
      getTopologyId = defGetTopologyId,
    } = fns
    const { debounceMs } = options || {}
    // Unpack data
    const { topologyId = getTopologyId(msg), ...topologyOptions }: MsgData =
      unpack(msg.data)
    // Get the stream data
    const { redeliveryCount: numAttempts, stream, streamSequence } = msg.info
    // Merge message context, msg, and topologyId
    const extendedContext = { ...context, stream, topologyId, msg }
    debug('Num attempts %d', numAttempts)
    // Should the topology be resumed
    const resuming = await shouldResume(topologyId)
    debug('Resuming %s', resuming)
    const { emitter, promise, getSnapshot, stop } = resuming
      ? // Resume topology based on uniqueness defined in loadSnapshot
        resumeTopology(spec, await loadSnapshot(topologyId), {
          context: extendedContext,
        })
      : // Run topology with data from msg
        runTopology(spec, dag, {
          ...topologyOptions,
          context: extendedContext,
        })
    // Propagate abort to topology
    if (context) {
      context.signal.addEventListener('abort', stop)
    }
    // Persist
    const persist = (snapshot: Snapshot) => {
      const streamSnapshot = {
        ...snapshot,
        stream,
        streamSequence,
        numAttempts,
      }
      debug('Stream Snapshot %O', streamSnapshot)
      // Persist snapshot
      return persistSnapshot(topologyId, streamSnapshot)
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
