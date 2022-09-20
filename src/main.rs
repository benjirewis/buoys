use mongodb::{
    bson::doc,
    options::{CreateCollectionOptions, TimeseriesGranularity, TimeseriesOptions},
    sync::Client,
};
use serde::{Deserialize, Serialize};
use std::env;
use std::error::Error;

// let date_str = "2020-04-12T22:10:57+02:00";
// convert the string into DateTime<FixedOffset>
// let datetime = DateTime::parse_from_rfc3339(date_str).unwrap();

#[derive(Debug, Deserialize, Serialize)]
struct BuoyDatum {
    time: String,                 // UTC
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
    coll: mongodb::sync::Collection<BuoyDatum>,
}

impl BuoyCollection {
    fn new(coll: mongodb::sync::Collection<BuoyDatum>) -> BuoyCollection {
        BuoyCollection { coll }
    }

    // load_csv will populate the collection with buoy data from the file referenced by the file
    // path.
    fn load_csv(&self, fp: &str) -> Result<(), Box<dyn Error>> {
        // Build the CSV reader and iterate over each record.
        let mut rdr = csv::Reader::from_path(fp)?;
        for result in rdr.deserialize() {
            // Notice that we need to provide a type hint for automatic
            // deserialization.
            let buoy_datum: BuoyDatum = result?;
            self.coll.insert_one(buoy_datum, None)?;
        }
        Ok(())
    }

    // delete_buoy will delete all data associated with the supplied buoy from the collection.
    fn delete_buoy(&self, buoy: &str) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    // list_buoys will print all buoys with data in the collection.
    fn list_buoys(&self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    // Connect to server.
    let client = Client::with_uri_str("mongodb://localhost:27017/")?;
    client
        .database("admin")
        .run_command(doc! {"ping": 1}, None)?;
    println!("connected successfully");

    // Create a BuoyCollection.
    let (coll_name, db) = (
        env::var("COLLECTION").unwrap_or("coll".to_string()),
        client.database(env::var("DATABASE").unwrap_or("db".to_string()).as_str()),
    );
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
    let buoy_coll = BuoyCollection::new(db.collection::<BuoyDatum>(coll_name.as_str()));

    // Populate the 'buoys' database with data from 2017-short.csv.
    buoy_coll.load_csv("data/2017-short.csv")?;
    Ok(())
}
