var path = require('path');

module.exports = {
  mode: 'production',
  context: path.join(__dirname, '/dist'),
  entry: './client.js',
  output: {
    path: path.join(__dirname, '/browser'),
    filename: 'client.js',
    library: 'GraphQLWS'
  }
};
