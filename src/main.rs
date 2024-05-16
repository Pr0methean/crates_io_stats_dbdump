use chrono::{NaiveDate};
use std::collections::{BTreeMap as Map};
use std::fs::File;
use std::io::copy;
use semver::Version;
use tempdir::TempDir;

const CRATE: &str = "zip";

fn main() -> db_dump::Result<()> {
    simple_log::quick().expect("Failed to configure logging");
    let db_dir = TempDir::new("crates_io_stats_dbdump")?;
    let db_path = db_dir.path().join("db-dump.tar.gz");
    let mut db = File::create(&db_path).expect("failed to create file");
    let mut resp = reqwest::blocking::get("https://static.crates.io/db-dump.tar.gz").expect("request failed");
    copy(&mut resp, &mut db).expect("failed to copy content");
    let mut crate_id = None;
    let mut versions = Vec::new();
    let mut version_downloads = Vec::new();
    db_dump::Loader::new()
        .crates(|row| {
            if row.name == CRATE {
                crate_id = Some(row.id);
            }
        })
        .versions(|row| versions.push(row))
        .version_downloads(|row| version_downloads.push(row))
        .load(db_path)?;
    drop(db_dir);

    // Crate id of the crate we care about.
    let crate_id = crate_id.expect("no such crate");

    // Set of all version ids corresponding to that crate.
    let mut version_ids = Map::new();
    for version in versions {
        if version.crate_id == crate_id {
            version_ids.insert(version.id, version.num);
        }
    }

    let mut max_date = NaiveDate::MIN;
    let mut min_date = NaiveDate::MAX;

    // Add up downloads across all version of the crate by day.
    let mut downloads = Map::<Version, Map<NaiveDate, u64>>::new();
    for stat in version_downloads {
        if let Some(version_num) = version_ids.get(&stat.version_id) {
            let naive_date = stat.date.naive_utc();
            *downloads.entry(version_num.to_owned()).or_default()
                .entry(naive_date).or_default() += stat.downloads;
            max_date = max_date.max(naive_date);
            min_date = min_date.min(naive_date);
        }
    }
    let version_nums: Vec<_> = version_ids.values().collect();
    print!("\"Date\",");
    println!("{}", version_nums.iter().map(|ver| format!("\"{}\"", ver)).collect::<Vec<_>>().join(","));
    for date in min_date.iter_days() {
        println!("\"{}\",{}", date,
               version_nums.iter().map(|ver| downloads.get(ver).unwrap().get(&date).unwrap_or(&0).to_string()).collect::<Vec<_>>().join(","));
        if date >= max_date {
            return Ok(())
        }
    }
    unreachable!()
}