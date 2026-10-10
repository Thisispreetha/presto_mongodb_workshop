// ----------------------------------------------------------------------------
// MongoDB Schema & Index Initialization for GDELT Global Geopolitical Lakehouse
// Database: market_data
// ----------------------------------------------------------------------------

db = db.getSiblingDB("market_data");

print("Creating collections and compound indexes in market_data for GDELT events...");

// 1. events collection (Operational Hot Tier)
db.createCollection("events");
db.events.createIndex({ action_country: 1, event_time: -1 }, { name: "idx_country_event_time" });
db.events.createIndex({ _modifiedTS: 1 }, { name: "idx_modifiedTS" });
db.events.createIndex({ global_event_id: 1 }, { unique: true, name: "idx_global_event_id_unique" });
db.events.createIndex({ event_date: 1 }, { name: "idx_event_date" });
db.events.createIndex({ location: "2dsphere" }, { sparse: true, name: "idx_location_2dsphere" });

// 2. Presto _schema pinning
// Presto MongoDB connector reads the _schema collection to enforce exact SQL types
db.createCollection("_schema");

db.getCollection("_schema").replaceOne(
  { table: "events" },
  {
    table: "events",
    fields: [
      { name: "_id", type: "varchar", hidden: false },
      { name: "global_event_id", type: "bigint", hidden: false },
      { name: "event_date", type: "date", hidden: false },
      { name: "event_time", type: "timestamp", hidden: false },
      { name: "actor1_name", type: "varchar", hidden: false },
      { name: "actor1_country", type: "varchar", hidden: false },
      { name: "actor2_name", type: "varchar", hidden: false },
      { name: "actor2_country", type: "varchar", hidden: false },
      { name: "event_code", type: "varchar", hidden: false },
      { name: "quad_class", type: "integer", hidden: false },
      { name: "goldstein_scale", type: "double", hidden: false },
      { name: "num_mentions", type: "integer", hidden: false },
      { name: "num_sources", type: "integer", hidden: false },
      { name: "num_articles", type: "integer", hidden: false },
      { name: "avg_tone", type: "double", hidden: false },
      { name: "action_country", type: "varchar", hidden: false },
      { name: "latitude", type: "double", hidden: false },
      { name: "longitude", type: "double", hidden: false },
      { name: "source_url", type: "varchar", hidden: false },
      { name: "source", type: "varchar", hidden: false },
      { name: "received_at", type: "timestamp", hidden: false },
      { name: "_insertedTS", type: "timestamp", hidden: false },
      { name: "_modifiedTS", type: "timestamp", hidden: false }
    ]
  },
  { upsert: true }
);

print("GDELT events collection, indexes, and Presto _schema pinned successfully.");
