use adif::RecordStream;
use futures::StreamExt;
use tokio::fs::File;
use tokio::io::BufReader;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "examples/sample.adif";
    let file = File::open(path).await?;
    let reader = BufReader::new(file);
    let mut stream = RecordStream::new(reader, true);

    while let Some(result) = stream.next().await {
        let record = result?;

        if record.is_header() {
            println!("Header:");
            if let Some(version) = record.get("adif_ver") {
                println!("  ADIF Version: {}", version.as_str());
            }
            if let Some(program) = record.get("programid") {
                println!("  Program: {}", program.as_str());
            }
        } else {
            println!("Contact:");
            if let Some(call) = record.get("call") {
                println!("  Call: {}", call.as_str());
            }
            if let Some(date) = record.get("qso_date") {
                println!("  Date: {}", date.as_str());
            }
            if let Some(time) = record.get("time_on") {
                println!("  Time: {}", time.as_str());
            }
            if let Some(band) = record.get("band") {
                println!("  Band: {}", band.as_str());
            }
            if let Some(mode) = record.get("mode") {
                println!("  Mode: {}", mode.as_str());
            }
        }
        println!();
    }

    Ok(())
}
