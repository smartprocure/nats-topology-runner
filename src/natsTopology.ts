import { runTopology, resumeTopology, Snapshot } from 'topology-runner'
import { JsMsg } from 'nats'
import { RunTopology } from './types'
import _debug from 'debug'

const debug = _debug('nats-topology-runner')

export const getStreamDataFromMsg = (msg: JsMsg) => {
  const { stream, streamSequence } = msg.info
  return { stream, streamSequence }
}

export const runTopologyWithNats: RunTopology =
  (spec, dag, fns, options) => async (msg) => {
    const { unpack, loadSnapshot, persistSnapshot } = fns
    const data = unpack(msg.data)
    const numAttempts = msg.info.redeliveryCount
    const isRedelivery = numAttempts > 1
    debug('Redelivery %s', isRedelivery)
    const { emitter, promise } = isRedelivery
      ? // Resume topology based on unique stream data
        resumeTopology(spec, await loadSnapshot(msg))
      : // Run topoloty with data from msg
        runTopology(spec, dag, { ...options, data })
    const streamData = getStreamDataFromMsg(msg)
    const persist = (snapshot: Snapshot) => {
      const streamSnapshot = { ...snapshot, ...streamData, numAttempts }
      debug('Stream Snapshot %O', streamSnapshot)
      // Let NATS know we're working
      msg.working()
      // Persist snapshot
      persistSnapshot(streamSnapshot, msg)
    }
    emitter.on('data', persist)
    await promise
  }

