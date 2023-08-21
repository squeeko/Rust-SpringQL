use springql::{SpringConfig, SpringPipeline};

fn main() {
    let pipeline = SpringPipeline::new(&SpringConfig::default()).unwrap();

    /*
    Create a source stream. A stream is similar to a table in a relational database.
    A source stream is a s stream to fetch stream data from a foreign source.

    ROWTIME is a keyword to declare the column represents the event time of a row.
    */

    pipeline
        .command(
            "CREATE SOURCE STREAM source_temperature_celsius (
                ts TIMESTAMP NOT NULL ROWTIME,
                temperature FLOAT NOT NULL
                );
                ",
        )
        .unwrap();

    /*
    Create a sink stream.
    A sink stream is a final stream in a pipeline. A foreign sink fetches stream rows from it.
    */

    pipeline
        .command(
            "
            CREATE SINK STREAM sink_temperature_fahrenheit (
                ts TIMESTAMP NOT NULL ROWTIME,
                temperature FLOAT NOT NULL
                );
                ",
        )
        .unwrap();

    /*
    Create a pump to convert Celsius to Fahrenheit.
    A pump fetches stream rows from a stream, make some conversions to the rows and emits them to another stream
    */

    pipeline
        .command(
            "
            CREATE PUMP c_to_f AS
                INSERT INTO sink_temperature_fahrenheit (ts, temperature)
                SELECT STREAM
                    source_temperature_celsius.ts,
                    32.0 + source_temperature_celsius.temperature * 1.8
                FROM source_temperature_celsius;
                ",
        )
        .unwrap();

    /*
    Create a sink writer as an in-memory queue.
    A sink writer fetches stream rows from a sink stream and writes then to various foreign sinks.
    Here we use a in-memory queue as the foreign sinks to easily get rows from our program written here.
    */

    pipeline
        .command(
            "
            CREATE SINK WRITER queue_temperature_fahrenheit FOR sink_temperature_fahrenheit
            TYPE IN_MEMORY_QUEUE OPTIONS (
                NAME 'q'
            );
            ",
        )
        .unwrap();

    /*
    Create a source reader as a TCP server (listening to tcp/9876).
    A source reader fetches stream rows from a foreign source and emits them to a source stream.

    Dataflow starts soon after this command creates the source reader instance.
    */

    pipeline
        .command(
            "
            CREATE SOURCE READER tcp_trade FOR source_temperature_celsius
            TYPE NET_SERVER OPTIONS(
                PROTOCOL 'TCP',
                PORT '9876'
            );
            ",
        )
        .unwrap();

    /*
    Get a row,
    */

    while let Ok(row) = pipeline.pop("q") {
        // fetch columns,
        let ts: String = row.get_not_null_by_index(0).unwrap();
        let temperature_fahrenheit: f32 = row.get_not_null_by_index(1).unwrap();
        // print them
        eprint!("{}\t{}\n", ts, temperature_fahrenheit);
    }
}
