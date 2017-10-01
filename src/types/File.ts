import { GraphQLScalarType } from 'graphql';
import { Subject } from 'rxjs';

const schema = `
  scalar File
`;

const resolvers = {
  File: new GraphQLScalarType({
    name: 'File',
    description: 'Observable that handles file transfer',
    serialize(value) {
      console.log('serialize');
      let result;
      // Implement your own behavior here by setting the 'result' variable
      return result;
    },
    parseValue(value) {
      console.log('parseValue');
      return (value && value.file instanceof Subject) ? value : null;
    },
    parseLiteral(ast) {
      // Observables can be passed only via variables
      return null;
    },
  }),
};

const File = {
    get schema() {
      return schema;
    },
    get resolvers() {
      return resolvers;
    },
};

export { File };
