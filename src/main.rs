#[cfg(test)]
mod tests;

use std::fs::File;
use std::io::Write;
use std::ops::Range;
use anyhow::Error;
use tokio::task::JoinSet;

const START: u128 = 0xFFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF_0000;
const END: u128 = 0xFFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF;
// const START: u128 = 0xFF_FFFF_FFFF_FFFF_0000;
// const END: u128 = 0xFFFF_FFFF_FFFF_FFFF_FFFF;
// const START: u64 = 2;
// const END: u64 = 7920;

const PAGE_SIZE: u128 = 8;

const WORKERS: usize = 28;

#[tokio::main]
async fn main() {
    let batch_generator = BatchGenerator::new(START, END, PAGE_SIZE);
    let mut page = 0;

    let mut task_set = JoinSet::new();
    let mut file = start_writer("/tmp/primes.txt").unwrap();

    for _ in 0..WORKERS {
        match next_batch(&batch_generator, &mut page, &mut task_set) {
            Ok(_) => continue,
            Err(_) => break,
        }
    }

    let mut track = 0;
    let mut ended = false;

    loop {
        let join_next = task_set.join_next().await;
        match join_next {
            None => {
                break;
            },
            Some(result) => {
                match result {
                    Ok(primes) => {
                        let amount = primes.len();
                        track += amount;

                        write_results(&mut file, &primes).unwrap();

                        if !ended {
                            match next_batch(&batch_generator, &mut page, &mut task_set) {
                                Ok(_) => continue,
                                Err(_) => {
                                    ended = true;
                                    continue;
                                },
                            }
                        }
                    }
                    Err(err) => {
                        println!("Error: {err}")
                    }
                }
            }
        }

    }

    println!("Calculated {track} primes.")
}

fn start_writer(file_name: &str) -> Result<File, Error>{
    match File::create(file_name) {
        Ok(ok) => Ok(ok),
        Err(err) => Err(Error::from(err)),
    }
}

fn write_results(file: &mut File, values: &Vec<u128>) -> Result<(), Error>{
    for value in values {
        let line = format!("{value}\r");
        match file.write_all(line.as_bytes()) {
            Ok(_) => continue,
            Err(err) => return Err(Error::from(err)),
        }
    }

    Ok(())
}

fn next_batch(batch_generator: &BatchGenerator, page: &mut u128, task_set: &mut JoinSet<Vec<u128>>) -> Result<(), ()> {
    let batch_result = batch_generator.get_page(*page);
    *page += 1;

    match batch_result {
        None => {
            Err(())
        }
        Some(range) => {
            println!("Starting #{page}");
            task_set.spawn(get_primes_async(range));
            Ok(())
        },
    }
}

async fn get_primes_async(range: Range<u128>) -> Vec<u128> {
    get_primes(range)
}

fn get_primes(range: Range<u128>) -> Vec<u128> {
    let mut result = Vec::with_capacity(range.clone().count());

    for number_to_check in range {
        if is_prime(number_to_check) {
            result.push(number_to_check);
        }
    }

    result
}

fn is_prime(num: u128) -> bool {
    if num % 2 == 0 && num != 2 {
        return false;
    }

    let end = num.isqrt().max(2);
    let mut quotient= 3;
    while quotient <= end {
        let remainder = num % quotient;
        if remainder == 0 {
            return false;
        } else {
            quotient += 2;
            continue;
        }
    }

    true
}

struct BatchGenerator {
    start: u128,
    end: u128,
    page_size: u128,
}

impl BatchGenerator {
    pub fn new(start: u128, end: u128, page_size: u128) -> Self {
        Self {
            start,
            end,
            page_size,
        }
    }

    pub fn get_page(&self, page: u128) -> Option<Range<u128>> {
        let start = (page * self.page_size) + self.start;
        let end = (start + self.page_size).min(self.end);

        match start >= end {
            true => None,
            false => Some(start..end),
        }
    }
}
