
extern crate rand;
use rand::Rng;

use std::env;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::fs::{File, OpenOptions};
use std::f32;
use std::thread;
use std::sync::{Arc, Barrier, Mutex};
/*
use rand::Rng;
use std::env;
use std::fs::{File};
use std::io::{Read, Cursor, Seek, SeekFrom};
*/
extern crate bytes;
use bytes::Buf;

fn main() {
    let args : Vec<String> = env::args().collect();
    if args.len() != 4 {
        println!("Usage: {} <threads> input output", args[0]);
    }

    let threads =  args[1].parse::<usize>().unwrap();
    let inp_path = &args[2];
    let out_path = &args[3];
    
    //open file
    let mut inpf = File::open(inp_path).unwrap();
    //get the count value from the file
    let size = read_size(&mut inpf);

    // Sample
    // Calculate pivots
    let pivots = find_pivots(&mut inpf, threads, size);
    
    /*
    // Create output file
    {
        let mut outf = File::create(out_path).unwrap();
        let tmp = size.to_bits().to_ne_bytes();
        outf.write_all(&tmp).unwrap();
        outf.set_len(size).unwrap();
    }

    let mut workers = vec![];

    // Spawn worker threads
    let sizes = Arc::new(Mutex::new(vec![0u64; threads]));
    let barrier = Arc::new(Barrier::new(threads));

    for ii in 0..threads {
        let inp = inp_path.clone();
        let out = out_path.clone();
        let piv = pivots.clone();
        let szs = sizes.clone();
        let bar = barrier.clone();

        let tt = thread::spawn(move || {
            worker(ii, inp, out, piv, szs, bar);
        });
        workers.push(tt);
    }

    // Join worker threads
    for tt in workers {
        tt.join().unwrap();
    }
    */
}

fn read_size(file: &mut File) -> u64 {
    // TODO: Read size field from data file
    let mut count = [0u8; 8];
    file.read_exact(&mut count);
    let xx = Cursor::new(count).get_u64_le();
    xx
}

fn read_item(file: &mut File, ii: u64) -> f32 {
    let mut tmp = [0u8;4];
    file.seek(SeekFrom::Start(8 + ii*4)).unwrap();
    file.read_exact(&mut tmp);
    let xx = Cursor::new(tmp).get_f32_le();
    xx
}

fn sample(file: &mut File, count: usize, size: u64) -> Vec<f32> {
    let mut rng = rand::thread_rng();
    let mut ys = vec![];
    let mut index = 0;
    //sample "count" random items from the file
    while index < count{
        let rand_val = rng.gen_range(0, size);
        ys.push(read_item(file, rand_val));
        index += 1;
    }

    ys
}

fn find_pivots(mut file: &mut File, threads: usize, size: u64) -> Vec<f32> {
    let mut pivots = vec![0f32; threads+1];
    const INFINITY:f32 = 1.0f32/0.0f32;
    //Sample 3*(threads-1) items from the file
    let count = 3*(threads - 1);
    let mut samples = sample(&mut file, count, size);
   // println!("the sampled values = {:?}\n", samples);
    
    // Sort the sampled list
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    //println!("the sorted values = {:?}\n", samples);
    // push the pivots into the array
    let mut index = 1;
    let mut i = 0;
    
    while i < count
    {
        pivots[index] = samples[i];
        i += 3;
        index += 1;
    }
    pivots[threads] = INFINITY;
        
    pivots
}
/*
fn worker(tid: usize, inp_path: String, out_path: String, pivots: Vec<f32>,
          sizes: Arc<Mutex<Vec<u64>>>, bb: Arc<Barrier>) {

    // TODO: Open input as local fh

    // TODO: Scan to collect local data
    let data = vec![0f32, 1f32];

    // TODO: Write local size to shared sizes
    {
        // curly braces to scope our lock guard
    }

    // TODO: Sort local data

    // Here's our printout
    println!("{}: start {}, count {}", tid, &data[0], data.len());

    // TODO: Write data to local buffer
    let mut cur = Cursor::new(vec![]);

    for xx in &data {
        let tmp = xx.to_bits().to_ne_bytes();
        cur.write_all(&tmp).unwrap();
    }

    // TODO: Get position for output file
    let prev_count = {
        // curly braces to scope our lock guard
        5
    };

    /*
    let mut outf = OpenOptions::new()
        .read(true)
        .write(true)
        .open(out_path).unwrap();
    */
    // TODO: Seek and write local buffer.

    // TODO: Figure out where the barrier goes.
}
*/