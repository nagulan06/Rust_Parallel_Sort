
extern crate rand;
use rand::Rng;

use std::env;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::fs::{File, OpenOptions};
use std::f32;
use std::thread;
use std::sync::{Arc, Barrier, Mutex};
use std::convert::TryInto;

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
    
    // Create output file
    let mut outf = File::create(out_path).unwrap();
    let tmp = size.to_ne_bytes();
    outf.write_all(&tmp).unwrap();
    outf.set_len(size).unwrap();
    
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
    
}
// Read size field from data file and return it
fn read_size(file: &mut File) -> u64 {
    let mut count = [0u8; 8];
    file.read_exact(&mut count).unwrap();
    let xx = Cursor::new(count).get_u64_le();
    xx
}

//read the iith item from the file and return it
fn read_item(file: &mut File, ii: u64) -> f32 { 
    let mut tmp = [0u8;4];
    file.seek(SeekFrom::Start(8 + ii*4)).unwrap();
    file.read_exact(&mut tmp).unwrap();
    let xx = Cursor::new(tmp).get_f32_le();
    xx
}

//sample "count" random items from the file and return the vector that contains the sample
fn sample(file: &mut File, count: usize, size: u64) -> Vec<f32> {
    let mut rng = rand::thread_rng();
    let mut ys = vec![];
    let mut index = 0;
    while index < count{
        let rand_val = rng.gen_range(0, size);
        ys.push(read_item(file, rand_val));
        index += 1;
    }

    ys
}

//Sort the sampled list, find medians, fill out the pivots array and return it
fn find_pivots(mut file: &mut File, threads: usize, size: u64) -> Vec<f32> {
    let mut pivots = vec![0f32; threads+1];
    const INFINITY:f32 = 1.0f32/0.0f32;
    //Sample 3*(threads-1) items from the file
    let count = 3*(threads - 1);
    let mut samples = sample(&mut file, count, size);
    
    // Sort the sampled list
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
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

//Each thread calls this function, which finds data that belong to its range ,sorts them parallely and puts them onto the output file at the correct location.
fn worker(tid: usize, inp_path: String, out_path: String, pivots: Vec<f32>,
          sizes: Arc<Mutex<Vec<u64>>>, bb: Arc<Barrier>) {

    //Open input as local fh
    let mut inpf = File::open(inp_path).unwrap();
    let size = read_size(&mut inpf);

    //Scan to collect local data
    let mut data: Vec<f32> = Vec::new();
    for i in 0..size
    {
        let item = read_item(&mut inpf, i);
        if (item >= pivots[tid]) && (item < pivots[tid+1])
        {
            data.push(item);
        }
    }

    //Write local size to shared sizes
    //since sizes is a shared array, it requires a lock to access it
    // curly braces to scope our lock guard
    {
        let mut sizes = sizes.lock().unwrap();
        sizes[tid] = data.len().try_into().unwrap();
    }

    // Sort local data
    data.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // calculate the start and end index for the current thread
    //But this requires all the thread to have completed calculating its size. Hence, barrier wait is used.

    bb.wait();
    let mut start = 0;
    //Get position for output file
    let prev_count = {
        //cope of the lock guard is within this "let" block.
        let sizes = sizes.lock().unwrap();
            for i in 0..tid
            {
                start = start + sizes[i];
            }
        start
    };


    // Here's our printout
    // The if condition is to make sure that we don't access the data index when it doesn't exist and hence leading to program crashing
    if data.len() != 0 
    {
        println!("{}: start {}, count {}", tid, &data[0], data.len());
    }
    else
    {
        println!("{}: count {}", tid, data.len());
    }

    //open file with read and write permissions
    let mut outf = OpenOptions::new()
        .read(true)
        .write(true)
        .open(out_path).unwrap();
    
    //Seek and write local buffer.
    //Seek to the right position where the current thread has to put data into the output file
    outf.seek(SeekFrom::Start(8 + prev_count*4)).unwrap();
    
    //Put all the data into the file.
    for i in 0..data.len()
    {
        let tmp = data[i].to_bits().to_ne_bytes();
        outf.write_all(&tmp).unwrap();
    }
    
}
