use mongodb::{bson::doc, sync::Client};
use serde::{Deserialize, Serialize};
use std::error::Error;

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

// let date_str = "2020-04-12T22:10:57+02:00";
// convert the string into DateTime<FixedOffset>
// let datetime = DateTime::parse_from_rfc3339(date_str).unwrap();

// load_csv will populate the supplied Database with buoy data from the file referenced by the file
// path.
fn load_csv(fp: &str, db: &mongodb::sync::Database) -> Result<(), Box<dyn Error>> {
    // Build the CSV reader and iterate over each record.
    let mut rdr = csv::Reader::from_path(fp)?;
    for result in rdr.deserialize() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.
        let buoy_datum: BuoyDatum = result?;
        let coll = db.collection::<BuoyDatum>(buoy_datum.station_id.as_str());
        coll.insert_one(buoy_datum, None)?;
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    // Connect to server.
    let client = Client::with_uri_str("mongodb://localhost:27017/")?;
    client
        .database("admin")
        .run_command(doc! {"ping": 1}, None)?;
    println!("connected successfully");

    // Populate the 'buoys' database with data from 2017-short.csv.
    let db = client.database("buoys");
    load_csv("data/2017-short.csv", &db)?;
    Ok(())
}
