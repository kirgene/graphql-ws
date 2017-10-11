import { GraphQLScalarType } from 'graphql';
import { Binary } from '../Binary';

const schema = `
  scalar Binary
`;

const resolvers = {
  Binary: new GraphQLScalarType({
    name: 'Binary',
    description: 'File transfer type',
    serialize(value) {
      return (value instanceof Binary) ? value : null;
    },
    parseValue(value) {
      return (value instanceof Binary) ? value : null;
    },
    parseLiteral(ast) {
      // Binaries can be passed only via variables
      return null;
    },
  }),
};

const GraphQLBinary = {
    get schema() {
      return schema;
    },
    get resolvers() {
      return resolvers;
    },
};

export { GraphQLBinary };
