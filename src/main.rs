use chrono::{DateTime, Utc};
use mongodb::{
    bson,
    bson::doc,
    options::{CreateCollectionOptions, TimeseriesGranularity, TimeseriesOptions},
    sync::Client,
};
use serde::{Deserialize, Serialize};
use std::env;
use std::error::Error;

#[derive(Deserialize, Serialize)]
struct BuoyDatum {
    #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    time: DateTime<Utc>, // UTC
    longitude: f32,               // Degrees east
    latitude: f32,                // Degrees north
    station_id: String,           // String
    significant_wave_height: f32, // Meters
    mean_wave_period: f32,        // Seconds
    mean_wave_direction: f32,     // Degrees
    wave_power: f32,              // Kilowatts / Meter
    peak_period: f32,             // Seconds
    energy_period: f32,           // Seconds
}

// BuoyCollection is a time-series collection with buoy data.
struct BuoyCollection {
    dbg: bool,
    coll: mongodb::sync::Collection<BuoyDatum>,
}

impl BuoyCollection {
    fn new(dbg: bool) -> Result<BuoyCollection, Box<dyn Error>> {
        // Connect to server.
        let client = Client::with_uri_str(
            env::var("MONGODB_URI").unwrap_or("mongodb://localhost:27017/".to_string()),
        )?;
        client
            .database("admin")
            .run_command(doc! {"ping": 1}, None)?;
        if dbg {
            println!("connected successfully");
        }

        // Create a BuoyCollection.
        let (coll_name, db) = (
            env::var("COLLECTION").unwrap_or("coll".to_string()),
            client.database(env::var("DATABASE").unwrap_or("db".to_string()).as_str()),
        );
        let coll = db.collection::<BuoyDatum>(coll_name.as_str());
        coll.drop(None)?;
        if dbg {
            println!("dropped collection");
        }
        db.create_collection(
            coll_name.as_str(),
            CreateCollectionOptions::builder()
                .timeseries(
                    TimeseriesOptions::builder()
                        .time_field("time".to_string())
                        .meta_field(Some("station_id".to_string()))
                        .granularity(Some(TimeseriesGranularity::Minutes))
                        .build(),
                )
                .build(),
        )?;
        if dbg {
            println!("created buoy collection {}", coll_name);
        }

        Ok(BuoyCollection {
            dbg,
            coll: db.collection::<BuoyDatum>(coll_name.as_str()),
        })
    }

    // load_csv will populate the collection with buoy data from the file referenced by the file
    // path.
    fn load_csv(&self, fp: &str) -> Result<(), Box<dyn Error>> {
        // Build the CSV reader and iterate over each record.
        if self.dbg {
            println!("loading from {} into buoy collection...", fp);
        }
        let mut rdr = csv::Reader::from_path(fp)?;
        for result in rdr.deserialize() {
            // Notice that we need to provide a type hint for automatic
            // deserialization.
            let buoy_datum: BuoyDatum = result?;
            self.coll.insert_one(buoy_datum, None)?;
        }
        if self.dbg {
            println!("finished loading");
        }
        Ok(())
    }

    // delete_buoy will delete all data associated with the supplied buoy from the collection.
    fn delete_buoy(&self, buoy: &str) -> Result<(), Box<dyn Error>> {
        if self.dbg {
            println!("deleting all {} data from buoy collection...", buoy);
        }
        self.coll.delete_many(doc! { "station_id" : buoy }, None)?;
        if self.dbg {
            println!("finished deleting");
        }
        Ok(())
    }

    // list_buoys will print all buoys with data in the collection to stdout.
    fn list_buoys(&self) -> Result<(), Box<dyn Error>> {
        let buoys = self.coll.distinct("station_id", doc! {}, None)?;
        println!("{:?}", buoys);
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    // Create new buoy collection.
    let buoy_coll = BuoyCollection::new(true)?;

    // Populate the collection with data from 2017-short.csv.
    buoy_coll.load_csv("data/2017-short.csv")?;

    // List the buoys available for query.
    buoy_coll.list_buoys()?;
    Ok(())
}
