const os = require('os')
const path = require('path')

const platform = os.platform()
const file = platform === 'darwin'
  ? 'enchu-extend.node'
  : 'enchu-extend-linux.node'

module.exports = require(path.join(__dirname, file))
