import { Snapshot, Spec, DAG, Options } from 'topology-runner'
import { JsMsg } from 'nats'
import { PerformOpts } from 'nats-jobs'

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
  shouldResume?(msg: JsMsg): Promise<boolean> | boolean
}

export interface RunOptions {
  debounceMs?: number
}

export type RunTopology = (
  spec: Spec,
  dag: DAG,
  fns: Fns,
  options?: Options & RunOptions
) => (msg: JsMsg, context?: PerformOpts) => Promise<void>
