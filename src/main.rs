use std::{
    fs::File,
    os::unix::fs::{FileExt, MetadataExt},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    collections::HashMap as HMap
};
use fxhash;

// The size of each chunk to read from the file.
const CHUNK_SIZE: u64 = 16 * 1024 * 1024;
// The excess size of each chunk to read from the file.
const CHUNK_EXCESS: u64 = 64;

// A type alias for a `Result` with a boxed error.
type Result<T, E = Box<dyn std::error::Error>> = std::result::Result<T, E>;

// Represents a collection of records.
#[derive(Debug, Clone, Copy)]
struct Records {
    count: u64,
    min: f32,
    max: f32,
    sum: f32,
}

/// Represents a collection of records.
impl Records {
    /// Updates the records with a new item.
    ///
    /// # Arguments
    ///
    /// * `item` - The new item to be added to the records.
    ///
    /// # Example
    ///
    /// ```
    /// let mut records = Records::from_item(5.0);
    /// records.update(10.0);
    /// assert_eq!(records.count, 2);
    /// assert_eq!(records.min, 5.0);
    /// assert_eq!(records.max, 10.0);
    /// assert_eq!(records.sum, 15.0);
    /// ```
    fn update(&mut self, item: f32) {
        self.count += 1;
        self.min = self.min.min(item);
        self.max = self.max.max(item);
        self.sum += item;
    }

    /// Creates a new `Records` instance from a single item.
    ///
    /// # Arguments
    ///
    /// * `item` - The item to create the `Records` instance from.
    ///
    /// # Returns
    ///
    /// A new `Records` instance with the provided item.
    ///
    /// # Example
    ///
    /// ```
    /// let records = Records::from_item(5.0);
    /// assert_eq!(records.count, 1);
    /// assert_eq!(records.min, 5.0);
    /// assert_eq!(records.max, 5.0);
    /// assert_eq!(records.sum, 5.0);
    /// ```
    fn from_item(item: f32) -> Self {
        Self {
            count: 1,
            min: item,
            max: item,
            sum: item,
        }
    }

    /// Calculates the mean value of the records.
    ///
    /// # Returns
    ///
    /// The mean value of the records.
    ///
    /// # Example
    ///
    /// ```
    /// let records = Records::from_item(5.0);
    /// records.update(10.0);
    /// assert_eq!(records.mean(), 7.5);
    /// ```
    fn mean(&self) -> f32 {
        let mean = self.sum / (self.count as f32);
        (mean * 10.0).round() / 10.0
    }

    /// Merges two `Records` instances into a single instance.
    ///
    /// # Arguments
    ///
    /// * `other` - The other `Records` instance to merge with.
    ///
    /// # Returns
    ///
    /// A new `Records` instance that is the result of merging the two instances.
    ///
    /// # Example
    ///
    /// ```
    /// let records1 = Records::from_item(5.0);
    /// let records2 = Records::from_item(10.0);
    /// let merged_records = records1.merge(records2);
    /// assert_eq!(merged_records.count, 2);
    /// assert_eq!(merged_records.min, 5.0);
    /// assert_eq!(merged_records.max, 10.0);
    /// assert_eq!(merged_records.sum, 15.0);
    /// ```
    fn merge(self, other: Self) -> Self {
        Self {
            count: self.count + other.count,
            min: self.min.min(other.min),
            max: self.max.max(other.max),
            sum: self.sum + other.sum,
        }
    }
}

type Hasher = fxhash::FxBuildHasher;

type GenericMap<K, V> = std::collections::HashMap<K, V, Hasher>;

type Map = GenericMap<String, Records>;

// Creates a new `HashMap` with the `Hasher` type.
macro_rules! new_map {
    ($t:ty) => {{
        let hasher = Hasher::default();
        <$t>::with_hasher(hasher)
    }};
}

fn calculate_aligned_buffer<'a>(
    file: &File,
    offset: u64,
    file_size: u64,
    buffer: &'a mut [u8],
) -> Result<&'a [u8]> {
    if offset > file_size {
        return Ok(&[]);
    }

    // The size of the buffer to read from the file.
    let buffer_size = buffer.len().min((file_size - offset) as usize);

    // The position to read from in the file.
    let (head, read_from) = if offset == 0 {
        (0, 0)
    } else {
        (CHUNK_EXCESS as usize, offset - CHUNK_EXCESS)
    };

    // Read the buffer from the file.
    file.read_exact_at(&mut buffer[..buffer_size], read_from)?;

    // The index of the last newline character before the `head` position in the buffer.
    let head = if head > 0 {
        buffer[..head].iter().rposition(|&c| c == b'\n').map_or(0, |pos| pos + 1)
    } else {
        0
    };
    
    // The index of the last newline character in the buffer.
    let tail = buffer[..buffer_size].iter().rposition(|&c| c == b'\n').unwrap_or(0);

    Ok(&buffer[head..=tail])
}

fn parse_line(line: &[u8]) -> Result<(&[u8], f32), String> {
    let split_point = line
        .iter()
        .enumerate()
        .find_map(|(idx, &b)| (b == b';').then_some(idx))
        .ok_or_else(|| {
            let line = std::str::from_utf8(line).unwrap_or("<invalid utf8>");
            format!("no ';' in {line}")
        })?;

    // Represents the index of the split point in the line.
    // 
    // The `split_point` variable is used to find the index of the first occurrence of the ';' character in the `line` slice.
    // If no ';' character is found, an error message is returned.
    let split_point = split_point;

    let temp = std::str::from_utf8(&line[split_point + 1..])
        .map_err(|err| format!("non-utf8 temp: {err}"))?;
    let temp: f32 = temp
        .parse()
        .map_err(|err| format!("parsing {temp}: {err}"))?;

    Ok((&line[..split_point], temp))
}

fn analyze_chunk(
    file: &File,
    offset: u64,
    outer_map: &mut Arc<Mutex<HMap<String, Records, Hasher>>>,
    buffer: &mut [u8],
    file_size: u64,
) -> Result<(), String> {
    let aligned_buffer = calculate_aligned_buffer(file, offset, file_size, buffer).unwrap();
    let mut local_map: HMap<Vec<u8>, Records> = HMap::new();

    // Update local_map in a single operation to minimize lock contention
    for line in aligned_buffer
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
    {
        let (city, temp) = parse_line(line)?;
        local_map.entry(city.to_vec())
            .and_modify(|records| records.update(temp))
            .or_insert_with(|| Records::from_item(temp));
    }

    // Update outer_map in a single operation to minimize lock contention
    let mut outer = outer_map.lock().map_err(|_| "non-poisoned mutex")?;
    for (city, records) in local_map {
        let city = String::from_utf8(city).map_err(|err| format!("non-utf8 city: {err}"))?;
        outer.entry(city)
            .and_modify(|outer_records| *outer_records = outer_records.merge(records))
            .or_insert(records);
    }

    Ok(())
}

fn distribute_to_threads(file: &File) -> Result<Map, Box<dyn std::error::Error>> {
    // The size of the file in bytes.
    let file_size = file.metadata()?.size();
    let offset = Arc::new(AtomicU64::new(0));
    let map = Arc::new(Mutex::new(new_map!(Map)));
    let num_threads = thread::available_parallelism()?.get().min((file_size / CHUNK_SIZE) as usize + 1);

    // The channel used to communicate between the main thread and the worker threads.
    let (tx, rx) = std::sync::mpsc::channel();

    // Spawn worker threads to process the file in parallel.
    thread::scope(|scope| {
        for _ in 0..num_threads {
            let offset = Arc::clone(&offset);
            let mut map = Arc::clone(&map);
            let tx = tx.clone();
            scope.spawn(move || {
                let mut buffer = vec![0; (CHUNK_SIZE + CHUNK_EXCESS) as usize];
                loop {
                    let current_offset = offset.fetch_add(CHUNK_SIZE, Ordering::SeqCst);
                    if current_offset >= file_size {
                        break;
                    }

                    if let Err(e) = analyze_chunk(file, current_offset, &mut map, &mut buffer, file_size) {
                        let _ = tx.send(Err(e));
                        return;
                    }
                }
                let _ = tx.send(Ok(()));
            });
        }
    });

    // Collect any errors that occurred during processing.
    let mut errors = Vec::new();
    for _ in 0..num_threads {
        if let Err(e) = rx.recv()? {
            errors.push(e);
        }
    }

    // Return the map if no errors occurred.
    if !errors.is_empty() {
        return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Errors occurred in processing")));
    }

    // Unwrap the Arc and Mutex to return the map.
    Ok(Arc::try_unwrap(map)
        .expect("Arc should be uniquely owned")
        .into_inner()?)
}

fn main() -> Result<()> {
    let now_2 = std::time::Instant::now();

    let args: Vec<String> = std::env::args().collect();
    let filename = &args[1];
    
    println!("Reading file");
    let file = std::fs::File::open(filename)?;

    println!("Processing file");
    let map = distribute_to_threads(&file)?;


    println!("Tokenizing results");
    let mut keys = map.keys().collect::<Vec<_>>();
    
    println!("Sorting results");
    keys.sort_unstable();

    println!("Printing results");
    for key in keys {
        let record = map[key];
        let min = record.min;
        let mean = record.mean();
        let max = record.max;

        println!("{key}: {min}/{mean}/{max}");
    }

    let elapsed = now_2.elapsed().as_secs();
    println!("Elapsed: {elapsed} seconds");

    Ok(())
}