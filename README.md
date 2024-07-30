## [1brc - 1 Billion Row Challenge](https://1brc.dev/)

Calculate the min, max, and average of 1 billion measurements

Here is my solution using Rust

## How to run

```bash
cargo run --release <file_name>
```

## Results

Running in a 1.4G file with 100 million rows

```bash
 cargo run --release 1.4G.txt 
    Finished `release` profile [optimized] target(s) in 0.00s
     Running `target/release/a_1brc_rs 1.4G.txt`
Reading file
Processing file
Tokenizing results
Sorting results
Printing results
XXXXXXXX: -xx.x/xx/xx.x
Elapsed: 3 seconds
```

Finally, the results for 14 gb file with 1 billion rows

```bash
 cargo run --release 14G.txt 
    Finished `release` profile [optimized] target(s) in 0.01s
     Running `target/release/a_1brc_rs 14G.txt`
Reading file
Processing file
Tokenizing results
Sorting results
Printing results
XXXXXXXX: -xx.x/xx/xx.x
Elapsed: 41 seconds
```

## Experience

It was fun to implement this solution in Rust. I had to learn a lot about the language and its ecosystem. I had to learn about the `std::fs` module to read the file, `std::collections::HashMap` to store the results and many other rust internal features and unstabe features.

