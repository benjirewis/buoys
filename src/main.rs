use mongodb::{
    bson::doc,
    options::{CreateCollectionOptions, FindOptions, TimeseriesGranularity, TimeseriesOptions},
    sync::Client,
};
use rgb::RGB8;
use serde::{Deserialize, Serialize};
use std::{env, error::Error};
use textplots::{Chart, ColorPlot, Shape};

// BuoyDatum represents a single, reported buoy measurement.
#[derive(Deserialize, Serialize)]
struct BuoyDatum {
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

// BuoyCollection is a time-series collection with buoy data.
struct BuoyCollection {
    // dbg turns on extra logging through stdout.
    dbg: bool,

    // coll is the underlying time-series collection.
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
        for result in rdr.records() {
            let record = result?;

            // Pray that the CSV is well-formed...
            let time_str = record.get(0).unwrap_or_default();
            let longitude_str = record.get(1).unwrap_or_default();
            let latitude_str = record.get(2).unwrap_or_default();
            let station_id_str = record.get(3).unwrap_or_default();
            let swh_str = record.get(4).unwrap_or_default();
            let mwp_str = record.get(5).unwrap_or_default();
            let mwd_str = record.get(6).unwrap_or_default();
            let wave_power_str = record.get(7).unwrap_or_default();
            let peak_period_str = record.get(8).unwrap_or_default();
            let energy_period_str = record.get(9).unwrap_or_default();

            self.coll.insert_one(
                BuoyDatum {
                    time: bson::DateTime::parse_rfc3339_str(time_str)?,
                    longitude: longitude_str.parse::<f32>().unwrap(),
                    latitude: latitude_str.parse::<f32>().unwrap(),
                    station_id: station_id_str.to_string(),
                    significant_wave_height: swh_str.parse::<f32>().unwrap(),
                    mean_wave_period: mwp_str.parse::<f32>().unwrap(),
                    mean_wave_direction: mwd_str.parse::<f32>().unwrap(),
                    wave_power: wave_power_str.parse::<f32>().unwrap(),
                    peak_period: peak_period_str.parse::<f32>().unwrap(),
                    energy_period: energy_period_str.parse::<f32>().unwrap(),
                },
                None,
            )?;
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

    // draw_buoy will visualize the statistics of a buoy with textplots-rs.
    fn draw_buoy(&self, buoy: &str) -> Result<(), Box<dyn Error>> {
        let cur = self.coll.find(
            doc! { "station_id": buoy },
            FindOptions::builder().sort(doc! { "time": 1 }).build(),
        )?;

        let mut swh = Vec::new();
        let mut i = -100.0;
        for buoy_datum in cur {
            let buoy_datum = buoy_datum?;
            swh.push((i, buoy_datum.significant_wave_height));
            i += 1.0;
        }

        let mut chart = Chart::new(120, 60, -100.0, 100.0);
        chart
            .linecolorplot(&Shape::Lines(&swh), RGB8 { r: 255, g: 0, b: 0 })
            .display();

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

    // Draw the 'Belmullet_Inner' buoy.
    buoy_coll.draw_buoy("Belmullet_Inner")?;
    Ok(())
}
