import { describe, expect, test, afterEach } from '@jest/globals'
import { JsMsg, JSONCodec } from 'nats'
import { runTopologyWithNats, Fns, StreamSnapshot } from './index'
import loki from 'lokijs'
import { RunFn, Spec } from 'topology-runner'
import _ from 'lodash/fp'

const db = new loki('test')
const topology = db.addCollection<StreamSnapshot & { topologyId: string }>(
  'topology'
)
const scraper = db.addCollection('scraper')
const jc = JSONCodec()

const spec: Spec = {
  api: {
    deps: [],
    run: async () => [1, 2, 3],
  },
  details: {
    deps: ['api'],
    run: async ({ data }) => {
      const ids: number[] = data[0]
      return ids.reduce(
        (acc, n) => _.set(n, { description: `description ${n}` }, acc),
        {}
      )
    },
  },
  attachments: {
    deps: ['api'],
    run: async ({ data }) => {
      const ids: number[] = data[0]
      return ids.reduce((acc, n) => _.set(n, { file: `file${n}.jpg` }, acc), {})
    },
  },
  writeToDB: {
    deps: ['details', 'attachments'],
    run: async ({ data }) => {
      scraper.insert(_.mergeAll(data))
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

const stripLoki = _.omit(['$loki', 'meta'])

describe('runTopologyWithNats', () => {
  afterEach(() => {
    topology.clear()
    scraper.clear()
  })
  test('should run topology to completion', async () => {
    const fns: Fns = {
      unpack: jc.decode,
      persistSnapshot,
      loadSnapshot,
    }
    const perform = runTopologyWithNats(spec, fns)
    const streamData = { stream: 'ORDERS', streamSequence: 1 }
    const msg = {
      info: { ...streamData, redeliveryCount: 1 },
      data: jc.encode({ data: 'Hello' }),
      subject: 'ORDERS',
      /* eslint-disable-next-line */
      working() {},
    }
    await perform(msg as JsMsg)
    const topologyRecord = stripLoki(topology.findOne(streamData))
    const scraperRecord = stripLoki(scraper.findOne({}))

    expect(topologyRecord).toMatchObject({
      status: 'completed',
      data: {
        api: {
          deps: [],
          input: ['Hello'],
          status: 'completed',
          output: [1, 2, 3],
        },
        details: {
          deps: ['api'],
          input: [[1, 2, 3]],
          status: 'completed',
          output: {
            '1': { description: 'description 1' },
            '2': { description: 'description 2' },
            '3': { description: 'description 3' },
          },
        },
        attachments: {
          deps: ['api'],
          input: [[1, 2, 3]],
          status: 'completed',
          output: {
            '1': { file: 'file1.jpg' },
            '2': { file: 'file2.jpg' },
            '3': { file: 'file3.jpg' },
          },
        },
        writeToDB: {
          deps: ['details', 'attachments'],
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
    const attachmentsRun: RunFn = async ({ data, state, updateState }) => {
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
        updateState({ index: i, output })
      }
      return output
    }
    const modifiedSpec = _.set('attachments.run', attachmentsRun, spec)
    const fns: Fns = {
      unpack: jc.decode,
      persistSnapshot,
      loadSnapshot,
    }
    const perform = runTopologyWithNats(modifiedSpec, fns)
    const streamData = { stream: 'ORDERS', streamSequence: 2 }
    const msg = {
      info: { ...streamData, redeliveryCount: 1 },
      data: jc.encode({ data: 'Hello' }),
      subject: 'ORDERS',
      /* eslint-disable-next-line */
      working() {},
    }
    await expect(perform(msg as JsMsg)).rejects.toThrow()
    let topologyRecord = stripLoki(topology.findOne(streamData))
    let scraperRecord = stripLoki(scraper.findOne({}))
    expect(topologyRecord).toMatchObject({
      status: 'errored',
      data: {
        api: {
          deps: [],
          input: ['Hello'],
          status: 'completed',
          output: [1, 2, 3],
        },
        details: {
          deps: ['api'],
          input: [[1, 2, 3]],
          status: 'completed',
          output: {
            '1': { description: 'description 1' },
            '2': { description: 'description 2' },
            '3': { description: 'description 3' },
          },
        },
        attachments: {
          deps: ['api'],
          input: [[1, 2, 3]],
          status: 'errored',
          error: {
            stack: expect.stringContaining('Error: Failed processing id: 2'),
          },
          state: {
            index: 0,
            output: { '1': { file: 'file1.jpg' }, '2': { file: 'file2.jpg' } },
          },
        },
      },
      stream: 'ORDERS',
      streamSequence: 2,
      numAttempts: 1,
    })
    expect(scraperRecord).toEqual({})

    // Simulate retry
    msg.info.redeliveryCount = 2
    await perform(msg as JsMsg)
    topologyRecord = stripLoki(topology.findOne(streamData))
    scraperRecord = stripLoki(scraper.findOne({}))
    expect(topologyRecord).toMatchObject({
      status: 'completed',
      data: {
        api: {
          deps: [],
          input: ['Hello'],
          status: 'completed',
          output: [1, 2, 3],
        },
        details: {
          deps: ['api'],
          input: [[1, 2, 3]],
          status: 'completed',
          output: {
            '1': { description: 'description 1' },
            '2': { description: 'description 2' },
            '3': { description: 'description 3' },
          },
        },
        attachments: {
          deps: ['api'],
          input: [[1, 2, 3]],
          status: 'completed',
          output: {
            '1': { file: 'file1.jpg' },
            '2': { file: 'file2.jpg' },
            '3': { file: 'file3.jpg' },
          },
        },
        writeToDB: {
          deps: ['details', 'attachments'],
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
