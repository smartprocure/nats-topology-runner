import { Snapshot, Spec, DAG, Options } from 'topology-runner'
import { JsMsg } from 'nats'
import { Context } from 'nats-jobs'

export interface StreamData {
  stream: string
  streamSequence: number
}

export interface StreamSnapshot extends Snapshot, StreamData {
  numAttempts: number
}

export type Fns = {
  unpack(x: Uint8Array): any
  loadSnapshot(topologyId: string): Promise<Snapshot> | Snapshot | undefined
  persistSnapshot(topologyId: string, snapshot: StreamSnapshot): void
  shouldResume?(topologyId: string): Promise<boolean> | boolean
  getTopologyId?(msg: JsMsg): string
}

export interface RunOptions {
  debounceMs?: number
}

export type RunTopology = (
  spec: Spec,
  dag: DAG,
  fns: Fns,
  options?: RunOptions
) => (msg: JsMsg, context?: Context) => Promise<void>

export interface MsgData extends Options {
  topologyId: string
}

export interface ExtendedContext extends Context {
  stream: string
  topologyId: string
  msg: JsMsg
}
