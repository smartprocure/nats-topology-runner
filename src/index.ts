import {
  runTopology,
  resumeTopology,
  Snapshot,
  Spec,
  DAG,
} from 'topology-runner'
import { JsMsg } from 'nats'
import _debug from 'debug'

const debug = _debug('nats-topology-runner')

export interface StreamData {
  stream: string
  streamSequence: number
}

export interface StreamSnapshot extends Snapshot, StreamData {
  numAttempts: number
}

export type Fns = {
  unpack(x: Uint8Array): any
  loadSnapshot(msg: JsMsg): Promise<Snapshot> | Snapshot
  persistSnapshot(snapshot: StreamSnapshot, msg: JsMsg): void
}

export const getStreamDataFromMsg = (msg: JsMsg) => {
  const { stream, streamSequence } = msg.info
  return { stream, streamSequence }
}

export const runTopologyWithNats =
  (spec: Spec, dag: DAG, fns: Fns) => async (msg: JsMsg) => {
    const { unpack, loadSnapshot, persistSnapshot } = fns
    const data = unpack(msg.data)
    const numAttempts = msg.info.redeliveryCount
    const isRedelivery = numAttempts > 1
    debug('Redelivery %s', isRedelivery)
    const { emitter, promise, getSnapshot } = isRedelivery
      ? // Resume topology based on unique stream data
        resumeTopology(spec, await loadSnapshot(msg))
      : // Run topoloty with data from msg
        runTopology(spec, dag, { data })
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
    try {
      await promise
    } finally {
      await persist(getSnapshot())
    }
  }
