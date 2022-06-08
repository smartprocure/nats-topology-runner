# NATS Topology Runner

Run a job using [nats-jobs](https://github.com/smartprocure/nats-jobs) and [topology-runner](https://github.com/smartprocure/topology-runner). Exports a single function `runTopologyWithNats` that runs a topology and resumes the topology on redelivery of a message with the same `stream` and `streamSequence` pair.

## runTopologyWithNats

```typescript
import { runTopologyWithNats, StreamSnapshot, Fns } from 'nats-topology-runner'
import { JsMsg, StringCodec } from 'nats'
import { expBackoff, JobDef, jobProcessor } from 'nats-jobs'
import { DAG, RunFn, Spec } from 'topology-runner'
import loki from 'lokijs'
import _ from 'lodash/fp'

const db = new loki('test.db')
const topology = db.addCollection<StreamSnapshot>('topology')
const scraper = db.addCollection('scraper')
const sc = StringCodec()

const getStreamData = _.pick(['stream', 'streamSequence'])

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
const loadSnapshot = async (streamData: StreamData) => {
  const streamSnapshot = topology.findOne(streamData)
  // Snapshot not found
  if (!streamSnapshot) {
    throw new Error(`No snapshot found for ${streamData}`)
  }
  return streamSnapshot
}

const persistSnapshot = async (snapshot: StreamSnapshot) => {
  const streamData = getStreamData(snapshot)
  const streamSnapshot = topology.findOne(streamData)
  if (streamSnapshot) {
    topology.update({ ...streamSnapshot, ...snapshot })
  } else {
    topology.insertOne(snapshot)
  }
}

const fns: Fns = {
  unpack: sc.decode,
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
