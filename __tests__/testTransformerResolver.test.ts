import * as assert from 'assert'

import * as transform from '../transformer'
import * as c from '../constants'

describe('Transformer Tests', function () {
  beforeEach(function () {
    delete process.env[c.STREAM_DATATYPE_ENV]
    delete process.env[c.TRANSFORMER_FUNCTION_ENV]
  })
  describe('- Verify the default transformer', function () {
    it(': Is using the default transformer', async function () {
      const t = await transform.setupTransformer()
      assert.equal(
        t.name,
        c.transformerRegistry.jsonToStringTransformer,
        ' Transformer Incorrectly Set '
      )
    })
  })
  describe('- Verify configuring the stream datatype CSV', function () {
    it(': Is using the configured transformer', async function () {
      process.env[c.STREAM_DATATYPE_ENV] = 'CSV'
      const t = await transform.setupTransformer()
      assert.equal(
        t.name,
        c.transformerRegistry.addNewlineTransformer,
        ' Transformer Incorrectly Set '
      )
    })
  })
  describe('- Verify configuring the stream datatype CSV with newlines', function () {
    it(': Is using the configured transformer', async function () {
      process.env[c.STREAM_DATATYPE_ENV] = 'CSV-WITH-NEWLINES'
      const t = await transform.setupTransformer()
      assert.equal(
        t.name,
        c.transformerRegistry.doNothingTransformer,
        ' Transformer Incorrectly Set '
      )
    })
  })
  describe('- Verify configuring the stream datatype BINARY', function () {
    it(': Is using the configured transformer', async function () {
      process.env[c.STREAM_DATATYPE_ENV] = 'BINARY'
      const t = await transform.setupTransformer()
      assert.equal(
        t.name,
        c.transformerRegistry.doNothingTransformer,
        ' Transformer Incorrectly Set '
      )
    })
  })
})
