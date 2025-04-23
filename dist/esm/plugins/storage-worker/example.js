/**
 * This file shows an example of how to use the Worker storage
 */
import { createRxDatabase } from '../../';
import { getRxStorageWorker } from '.';
import { addRxPlugin } from '../../plugin';
import { RxDBUpdatePlugin } from '../update';
addRxPlugin(RxDBUpdatePlugin);

// Define a schema for a collection
var heroSchema = {
  title: 'hero schema',
  version: 0,
  description: 'describes a hero',
  primaryKey: 'id',
  type: 'object',
  properties: {
    id: {
      type: 'string',
      maxLength: 100
    },
    name: {
      type: 'string'
    },
    color: {
      type: 'string'
    },
    healthpoints: {
      type: 'number',
      minimum: 0,
      maximum: 100
    },
    secret: {
      type: 'object',
      properties: {
        name: {
          type: 'string'
        }
      }
    }
  },
  required: ['id', 'name', 'healthpoints']
};

/**
 * Create a database with Worker storage
 */
async function createDatabase() {
  console.log("mill create");
  // Create the database
  var db = await createRxDatabase({
    name: 'heroesdb',
    storage: getRxStorageWorker({
      // Path to the worker file
      // This must be a file that is reachable from the webserver
      workerInput: './work.js',
      // Options for the Worker constructor
      workerOptions: {
        type: 'module'
      }
    })
  });
  console.log("create success");

  // Create a collection
  var heroesCollection = await db.addCollections({
    heroes: {
      schema: heroSchema
    }
  });

  // Insert a document
  await heroesCollection.heroes.insert({
    id: 'hero1',
    name: 'Captain America',
    color: 'blue',
    healthpoints: 100,
    secret: {
      name: 'Steve Rogers'
    }
  });
  heroesCollection.heroes.find().$.subscribe(r => {
    console.log("get changes");
    console.log(JSON.stringify(r, null, 2));
  });

  // Query documents
  var heroes = await heroesCollection.heroes.find().exec();
  console.log('Heroes:', heroes.map(hero => hero.toJSON()));

  // Update a document
  await heroesCollection.heroes.findOne('hero1').update({
    $set: {
      healthpoints: 90
    }
  });
  await db.close();
}
console.log("create databse");
// Run the example
await createDatabase().catch(console.error);
//# sourceMappingURL=example.js.map