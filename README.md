# Ancla

Ancla is a Rust-based project providing a command-line interface (CLI) tool (`boltcli`) and a library (`ancla`) for interacting with and reading data from Go's Bolt (bbolt) database files.

## Features

* **`boltcli` CLI Tool**: A powerful command-line utility to inspect and manage Bolt database files.
  * **Database Information**: Get general information about a Bolt database file.
  * **Bucket Listing**: List all top-level buckets within a database.
  * **Key-Value Operations**: Retrieve key-value pairs from specified buckets.
  * **Page Inspection**: List and inspect pages within the database.
* **`ancla` Library**: A Rust library for programmatic access to Bolt database files, enabling you to build your own applications that read Bolt data.
* **`boltypes` Library**: Defines the core data structures and types used in Bolt database files.

## Installation

To install the `boltcli` command-line tool, you need to have Rust and Cargo installed. If you don't have them, you can install them from [rustup.rs](https://rustup.rs/).

Once Rust is installed, you can install `boltcli` via Cargo:

```bash
cargo install --path cli
```

This will compile the `boltcli` executable and place it in your Cargo bin directory (usually `~/.cargo/bin`), making it available in your PATH.

## Usage

### `boltcli` Command-Line Tool

The `boltcli` tool provides various subcommands to interact with Bolt database files, including `info`, `buckets`, `kvs`, and `pages`.

To see all available commands and their usage, run:

```bash
boltcli --help
```

For detailed help on a specific subcommand, use `boltcli <command> --help` (e.g., `boltcli info --help`).

### `ancla` Library

For programmatic usage, add `ancla` as a dependency to your `Cargo.toml`:

```toml
[dependencies]
ancla = "0.1.0" # Or the latest version
```

Then you can use it in your Rust code:

```rust
use ancla::db::Db;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = Path::new("path/to/your/data.db");
    let db = Db::open(db_path)?;

    // Example: Get a value from a bucket
    if let Some(value) = db.get_value("my_bucket", "my_key")? {
        println!("Value: {:?}", String::from_utf8_lossy(&value));
    }

    Ok(())
}
```

## Project Structure

* **`cli/`**: Contains the source code for the `boltcli` command-line tool.
* **`crates/ancla/`**: The core library for reading and interacting with Bolt database files.
* **`crates/boltypes/`**: Defines the fundamental data structures and types used within Bolt database files.

## Development

This project uses `just` for task automation. Common development tasks like building, testing, formatting, and linting are defined in the `justfile`. You can list all available commands by running `just` in the project root.

## Contributing

Contributions are welcome! Please feel free to open issues or submit pull requests.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
