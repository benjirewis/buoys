use mongodb::{
    bson::doc,
    bson::DateTime,
    options::{CreateCollectionOptions, TimeseriesGranularity, TimeseriesOptions},
    sync::Client,
};
use serde::{de, Deserialize, Serialize};
use std::{env, error::Error, fmt};

// BuoyDatum represents a single, reported buoy measurement.
#[derive(Deserialize, Serialize)]
struct BuoyDatum {
    #[serde(deserialize_with = "deserialize_datetime")]
    time: bson::DateTime,
    longitude: f32,
    latitude: f32,
    station_id: String,
    significant_wave_height: f32,
    mean_wave_period: f32,
    mean_wave_direction: f32,
    wave_power: f32,
    peak_period: f32,
    energy_period: f32,
}

// TODO: move date deserializing logic to another file.
struct DateTimeFromRFC3339Visitor;

fn deserialize_datetime<'de, D>(d: D) -> Result<DateTime, D::Error>
where
    D: de::Deserializer<'de>,
{
    d.deserialize_str(DateTimeFromRFC3339Visitor)
}

impl<'de> de::Visitor<'de> for DateTimeFromRFC3339Visitor {
    type Value = DateTime;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a BSON datetime string")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match DateTime::parse_rfc3339_str(value) {
            Ok(dt) => Ok(dt),
            Err(e) => Err(E::custom(format!("Parse error {} for {}", e, value))),
        }
    }
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
    let buoy_coll = BuoyCollection::new(env::var("DEBUG").is_ok())?;

    // Populate the collection with data from 2017-short.csv.
    buoy_coll.load_csv("data/2017-short.csv")?;

    // List the buoys available for query.
    buoy_coll.list_buoys()?;

    // Delete the 'Belmullet_Inner' buoy data.
    buoy_coll.delete_buoy("Belmullet_Inner")?;

    // List the buoys available for query again.
    buoy_coll.list_buoys()?;
    Ok(())
}
