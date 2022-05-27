import { describe, expect, test, afterEach } from '@jest/globals'
import { JsMsg, StringCodec } from 'nats'
import { runTopologyWithNats, Fns, StreamData, StreamSnapshot } from './index'
import loki from 'lokijs'
import { DAG, RunFn, Spec } from 'topology-runner'
import _ from 'lodash/fp'

const db = new loki('test')
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

const stripLoki = _.omit(['$loki', 'meta'])

describe('runTopologyWithNats', () => {
  afterEach(() => {
    topology.clear()
    scraper.clear()
  })
  test('should run topology to completion', async () => {
    const fns: Fns = {
      unpack: sc.decode,
      persistSnapshot,
      loadSnapshot,
    }
    const perform = runTopologyWithNats(spec, dag, fns)
    const streamData = { stream: 'ORDERS', streamSequence: 1 }
    const msg = {
      info: { ...streamData, redeliveryCount: 1 },
      data: sc.encode('Hello'),
      subject: 'ORDERS',
      /* eslint-disable-next-line */
      working() {},
    }
    await perform(msg as JsMsg)
    const topologyRecord = stripLoki(topology.findOne(streamData))
    const scraperRecord = stripLoki(scraper.findOne({}))

    expect(topologyRecord).toMatchObject({
      status: 'completed',
      dag: {
        api: { deps: [] },
        details: { deps: ['api'] },
        attachments: { deps: ['api'] },
        writeToDB: { deps: ['details', 'attachments'] },
      },
      data: {
        api: {
          input: 'Hello',
          status: 'completed',
          output: [1, 2, 3],
        },
        details: {
          input: [[1, 2, 3]],
          status: 'completed',
          output: {
            '1': { description: 'description 1' },
            '2': { description: 'description 2' },
            '3': { description: 'description 3' },
          },
        },
        attachments: {
          input: [[1, 2, 3]],
          status: 'completed',
          output: {
            '1': { file: 'file1.jpg' },
            '2': { file: 'file2.jpg' },
            '3': { file: 'file3.jpg' },
          },
        },
        writeToDB: {
          input: [
            {
              '1': { description: 'description 1' },
              '2': { description: 'description 2' },
              '3': { description: 'description 3' },
            },
            {
              '1': { file: 'file1.jpg' },
              '2': { file: 'file2.jpg' },
              '3': { file: 'file3.jpg' },
            },
          ],
          status: 'completed',
          output: undefined,
        },
      },
      stream: 'ORDERS',
      streamSequence: 1,
      numAttempts: 1,
    })
    expect(scraperRecord).toEqual({
      '1': { description: 'description 1', file: 'file1.jpg' },
      '2': { description: 'description 2', file: 'file2.jpg' },
      '3': { description: 'description 3', file: 'file3.jpg' },
    })
  })

  test('should resume topology after initial error', async () => {
    let attempt = 1
    const attachmentsRun: RunFn = async ({ data, state, updateStateFn }) => {
      // Flatten
      data = data.flat()
      // Start from next element if resume scenario
      const ids: number[] = state ? data.slice(state.index + 1) : data
      const output: Record<number, { file: string }> = state ? state.output : {}
      for (let i = 0; i < ids.length; i++) {
        const id = ids[i]
        output[id] = { file: `file${id}.jpg` }
        // Simulate error while processing second element.
        // Error occurs the first time the fn is called.
        if (i === 1 && attempt++ === 1) {
          throw new Error(`Failed processing id: ${id}`)
        }
        // Successfully processed so record state
        updateStateFn({ index: i, output })
      }
      return output
    }
    const modifiedSpec = _.set('nodes.attachments.run', attachmentsRun, spec)
    const fns: Fns = {
      unpack: sc.decode,
      persistSnapshot,
      loadSnapshot,
    }
    const perform = runTopologyWithNats(modifiedSpec, dag, fns)
    const streamData = { stream: 'ORDERS', streamSequence: 2 }
    const msg = {
      info: { ...streamData, redeliveryCount: 1 },
      data: sc.encode('Hello'),
      subject: 'ORDERS',
      /* eslint-disable-next-line */
      working() {},
    }
    await expect(perform(msg as JsMsg)).rejects.toThrow()
    let topologyRecord = stripLoki(topology.findOne(streamData))
    let scraperRecord = stripLoki(scraper.findOne({}))
    expect(topologyRecord).toMatchObject({
      status: 'errored',
      dag: {
        api: { deps: [] },
        details: { deps: ['api'] },
        attachments: { deps: ['api'] },
        writeToDB: { deps: ['details', 'attachments'] },
      },
      data: {
        api: {
          input: 'Hello',
          status: 'completed',
          output: [1, 2, 3],
        },
        details: {
          input: [[1, 2, 3]],
          status: 'completed',
          output: {
            '1': { description: 'description 1' },
            '2': { description: 'description 2' },
            '3': { description: 'description 3' },
          },
        },
        attachments: {
          input: [[1, 2, 3]],
          status: 'errored',
          state: {
            index: 0,
            output: { '1': { file: 'file1.jpg' }, '2': { file: 'file2.jpg' } },
          },
        },
      },
      stream: 'ORDERS',
      streamSequence: 2,
      numAttempts: 1,
      error: 'Failed processing id: 2',
    })
    expect(scraperRecord).toEqual({})

    // Simulate retry
    msg.info.redeliveryCount = 2
    await perform(msg as JsMsg)
    topologyRecord = stripLoki(topology.findOne(streamData))
    scraperRecord = stripLoki(scraper.findOne({}))
    expect(topologyRecord).toMatchObject({
      status: 'completed',
      dag: {
        api: { deps: [] },
        details: { deps: ['api'] },
        attachments: { deps: ['api'] },
        writeToDB: { deps: ['details', 'attachments'] },
      },
      data: {
        api: {
          input: 'Hello',
          status: 'completed',
          output: [1, 2, 3],
        },
        details: {
          input: [[1, 2, 3]],
          status: 'completed',
          output: {
            '1': { description: 'description 1' },
            '2': { description: 'description 2' },
            '3': { description: 'description 3' },
          },
        },
        attachments: {
          input: [[1, 2, 3]],
          status: 'completed',
          output: {
            '1': { file: 'file1.jpg' },
            '2': { file: 'file2.jpg' },
            '3': { file: 'file3.jpg' },
          },
        },
        writeToDB: {
          input: [
            {
              '1': { description: 'description 1' },
              '2': { description: 'description 2' },
              '3': { description: 'description 3' },
            },
            {
              '1': { file: 'file1.jpg' },
              '2': { file: 'file2.jpg' },
              '3': { file: 'file3.jpg' },
            },
          ],
          status: 'completed',
          output: undefined,
        },
      },
      stream: 'ORDERS',
      streamSequence: 2,
      numAttempts: 2,
    })
    expect(scraperRecord).toEqual({
      '1': { description: 'description 1', file: 'file1.jpg' },
      '2': { description: 'description 2', file: 'file2.jpg' },
      '3': { description: 'description 3', file: 'file3.jpg' },
    })
  })
})
