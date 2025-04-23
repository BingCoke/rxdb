/**
 * This file shows an example of how to use the SharedWorker storage
 */
import { createRxDatabase } from '../../';
import { getRxStorageSharedWorker } from './index';

// Add the SharedWorker storage plugin

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
 * Create a database with SharedWorker storage
 */
async function createDatabase() {
  // Create the database
  var db = await createRxDatabase({
    name: 'heroesdb',
    storage: getRxStorageSharedWorker({
      // Path to the shared worker file
      // This must be a file that is reachable from the webserver
      workerInput: './work.js',
      // Set to true to improve performance if you only create the database once
      multiInstance: false
    })
  });

  // Create a collection
  var heroesCollection = await db.addCollections({
    heroes: {
      schema: heroSchema
    }
  });
  var subscription = heroesCollection.heroes.find().$.subscribe(r => {
    console.log("get heros change");
    console.log(r);
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

  // Update a document
  await heroesCollection.heroes.findOne('hero1').update({
    $set: {
      healthpoints: 90
    }
  });

  // Clean up
  subscription.unsubscribe();
  await db.close();
}

// Run the example
createDatabase().catch(console.error);
//# sourceMappingURL=example.js.map