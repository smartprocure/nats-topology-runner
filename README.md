# NATS Topology Runner

Run a job using [nats-jobs](https://github.com/smartprocure/nats-jobs)
and [topology-runner](https://github.com/smartprocure/topology-runner).
Exports the function `runTopologyWithNats` that runs a topology and
resumes the topology based on `loadSnapshot`. Uniqueness can be determined
by using `getStreamDataFromMsg` to extract `stream` and `streamSequence`
from the message.

## runTopologyWithNats

Returns a fn that takes a JsMsg and runs the topology
with the data off the message. Automatically resumes a topology
if a snapshot exists with the topologyId and it was not run to completion.
Regardless of whether the topology succeeds or fails, the last snapshot will
be persisted and awaited.

The example below uses an in-memory database called `loki`. In a real-world
scenario you would want to use something like MongoDB or Redis.

If you topology executes fast enough you may want to use the `debounceMs`
option to prevent the possibility of out-of-order writes to the datastore.

```typescript
import {
  runTopologyWithNats,
  getStreamDataFromMsg,
  StreamSnapshot,
  Fns,
} from 'nats-topology-runner'
import { JsMsg, JSONCodec } from 'nats'
import { expBackoff, JobDef, jobProcessor } from 'nats-jobs'
import { DAG, RunFn, Spec } from 'topology-runner'
import loki from 'lokijs'
import _ from 'lodash/fp'

const db = new loki('test.db')
const topology = db.addCollection<StreamSnapshot & { topologyId: string }>(
  'topology'
)
const scraper = db.addCollection('scraper')
const jc = JSONCodec()

const dag: DAG = {
  api: { deps: [] },
  details: { deps: ['api'] },
  attachments: { deps: ['api'] },
  writeToDB: { deps: ['details', 'attachments'] },
}

const spec: Spec = {
  nodes: {
    api: {
      run: async () => [1, 2, 3],
    },
    details: {
      run: async ({ data }) => {
        const ids: number[] = data[0]
        return ids.reduce(
          (acc, n) => _.set(n, { description: `description ${n}` }, acc),
          {}
        )
      },
    },
    attachments: {
      run: async ({ data }) => {
        const ids: number[] = data[0]
        return ids.reduce(
          (acc, n) => _.set(n, { file: `file${n}.jpg` }, acc),
          {}
        )
      },
    },
    writeToDB: {
      run: async ({ data }) => {
        scraper.insert(_.mergeAll(data))
      },
    },
  },
}
const loadSnapshot = (topologyId: string) =>
  topology.findOne({ topologyId }) || undefined

const persistSnapshot = (topologyId: string, snapshot: StreamSnapshot) => {
  const existing = topology.findOne({ topologyId })
  if (existing) {
    // Loki adds a meta field which must be stripped from the snapshot
    topology.update({ ...existing, ...stripLoki(snapshot) })
  } else {
    topology.insertOne({ ...snapshot, topologyId })
  }
}

const fns: Fns = {
  unpack: jc.decode,
  persistSnapshot,
  loadSnapshot,
}
const perform = runTopologyWithNats(spec, dag, fns)

const def: JobDef = {
  stream: 'scraper',
  backoff: expBackoff(1000),
  perform,
}
const processor = await jobProcessor()
processor.start(def)
```
