
use crate::file;

use std::io::Write;
use std::collections::HashSet;

use chrono::{Utc, DateTime, Duration};
use log::warn;
use rayon::prelude::*;
use serde::{Serialize, Deserialize};

use horrorshow::html;
use horrorshow::helper::doctype;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Stats {
    pub timestamp: String,
    pub name: String,
    pub status: u16,
    pub latency: u128
}

impl Stats {
    pub fn new(name: &str, status: u16, latency: u128) -> Stats {
        Stats {
            timestamp: Utc::now().to_rfc3339(),
            name: String::from(name),
            status,
            latency
        }
    }
}

pub fn generate(report_file: String) -> Result<(), Box<dyn std::error::Error>> {
    let (names, stats) = get_stats(&report_file)?;

    let mut total_hits = 0;
    let mut total_errors = 0.0;

    let starttime = DateTime::parse_from_rfc3339(&stats[0].timestamp).unwrap() - Duration::milliseconds(stats[0].latency as i64);
    let endtime = DateTime::parse_from_rfc3339(&stats[stats.len()-1].timestamp)?;
    let et = endtime.signed_duration_since(starttime).num_seconds();

    let mut table: Vec<Vec<String>> = Vec::new();

    for name in names {
                                    
        //HEAVY CLONING HAPPENING HERE - TRY TO FIX
        let filter: Vec<Stats> = stats.clone().into_iter().filter(|s| s.name == name).collect();
        let num = filter.len();

        let mut times: Vec<u128> = filter.par_iter().map(|s| s.latency).collect();
        times.sort();

        let min = times[0];
        let max = times[num-1];

        let pc_90 = get_percentile(&times, 90);
        let pc_95 = get_percentile(&times, 95);
        let pc_99 = get_percentile(&times, 99);

        let sum: u128 = filter.par_iter().map(|s| s.latency).sum();
        let sum = sum as usize;
        let avg: usize = sum/num;
        let num_f32 = num as f32;
        let et_f32 = et as f32;
        let tput: f32 = num_f32 / et_f32;
        let errors = filter.par_iter().filter(|s| s.status >= 400).count() as f32;
        let error_rate: f32 = errors * 100.0 / num_f32;
    
        let row: Vec<String> = vec![name.to_string(), num.to_string(), tput.to_string(), 
                                    min.to_string(), avg.to_string(), max.to_string(),
                                    pc_90.to_string(), pc_95.to_string(), pc_99.to_string(),
                                    errors.to_string(), error_rate.to_string()];

        total_hits += num;
        total_errors += errors;
        table.push(row);
    }

    let et_s = et as f32;
    let total_hits = total_hits as f32;
    let ttput =  total_hits/et_s ;
    let err_rate = total_errors * 100.0 / total_hits;

    let actual = format!("{}", html! {
    : doctype::HTML;
        html {
            head {
                // Use a variable
                title : "Bombardier Report";
                meta(charset="utf-8");
                meta(name="viewport", content="width=device-width, initial-scale=1");
                link(rel="stylesheet", href="https://maxcdn.bootstrapcdn.com/bootstrap/4.4.1/css/bootstrap.min.css");
                script(src="https://ajax.googleapis.com/ajax/libs/jquery/3.4.1/jquery.min.js");
                script(src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.16.0/umd/popper.min.js");
                script(src="https://maxcdn.bootstrapcdn.com/bootstrap/4.4.1/js/bootstrap.min.js");
            }
            body {
                
                div(class="jumbotron text-center") {
                    h1 : "Bombardier";
                    p {
                        : "Test Report";
                    }
                }

                div (class="container"){
                    div(class="row") {
                        h3 : "Requests";
                    }
                    div(class="row") {
                        table(class="table") {
                            thead{
                                th: "Request";
                                th(class="text-center") : "Total Hits";
                                th(class="text-center") : "Hits/s";
                                th(class="text-center") : "Min";
                                th(class="text-center") : "Avg";
                                th(class="text-center") : "Max";
                                th(class="text-center") : "90%";
                                th(class="text-center") : "95%";
                                th(class="text-center") : "99%";
                                th(class="text-center") : "Errors";
                                th(class="text-center") : "Error Rate";
                            }
                            tbody {
                                
                                @ for row in &table {
                                    tr {
                                        td : &row[0]; // name
                                        td(class="text-center") : &row[1]; // num
                                        td(class="text-center") : &row[2]; // tput
                                        td(class="text-center") : &row[3]; // min
                                        td(class="text-center") : &row[4]; // avg
                                        td(class="text-center") : &row[5]; // max
                                        td(class="text-center") : &row[6]; // pc_90
                                        td(class="text-center") : &row[7]; // pc_95
                                        td(class="text-center") : &row[8]; // pc_99
                                        @ if &row[9] == "0" { 
                                            td(class="text-center") {
                                                span(class= "badge badge-success") : &row[9]; // errors
                                            }      
                                            td(class="text-center") {
                                                span(class= "badge badge-success") : &row[10]; // error_rate
                                            }
                                        } else {
                                            td(class="text-center") {
                                                span(class= "badge badge-danger") : &row[9]; // errors
                                            }       
                                            td(class="text-center") {
                                                span(class= "badge badge-danger") : &row[10]; // error_rate
                                            }
                                        }                                                                         
                                    }
                                }
                            }
                        }
                    }
                    div(class="row") {
                        h3 : "Summary";
                    }
                    div(class="row") {
                        table(class="table") {
                            thead{
                                th : "Total Execution Time (in secs)";
                                th(class="text-center") : "Total Hits";
                                th(class="text-center") : "Hits/s";
                                th(class="text-center") : "Total Errors";
                                th(class="text-center") : "Total Error Rate";
                            }
                            tbody {
                                tr {
                                    td: et_s.to_string();
                                    td(class="text-center"): total_hits.to_string();
                                    td(class="text-center"): ttput.to_string();
                                    @ if total_errors == 0.0 { 
                                        td(class="text-center") {
                                            span(class= "badge badge-success") : total_errors.to_string();
                                        }       
                                        td(class="text-center") {
                                            span(class= "badge badge-success") : err_rate.to_string();
                                        }
                                    } else {
                                        td(class="text-center") {
                                            span(class= "badge badge-danger") : total_errors.to_string();
                                        }       
                                        td(class="text-center") {
                                            span(class= "badge badge-danger") : err_rate.to_string();
                                        }
                                    }             
                                }
                            }
                        }
                    }
                }
            }
        }
    });
        
    let mut report_file = file::create_file("report.html")?;
    match report_file.write(actual.as_bytes()) {
        Err(err) => warn!("Unable to write stat {} to file due to error {}", actual, err),
        Ok(_) => (),
    }
    Ok(())
}

fn get_stats(report_file: &str) -> Result<(HashSet<String>, Vec<Stats>), csv::Error> {
    let mut stats: Vec<Stats> = Vec::new();
    let mut names: HashSet<String> = HashSet::new();

    let mut reader = csv::ReaderBuilder::new().has_headers(true).trim(csv::Trim::All).from_path(report_file)?;
    let records_iter = reader.deserialize();
    
    for stat in records_iter {
        let s: Stats = stat?;
        if !names.contains(&s.name) {
            names.insert(s.name.clone());
        }
        stats.push(s);
    }   
        
    Ok((names, stats))
}

fn get_percentile(sorted_vector: &Vec<u128>, p: usize) -> u128 {
    let len = sorted_vector.len();
    match p*len/100 {
        0 => sorted_vector[0],
        _ => sorted_vector[(p*len/100)-1]
    }
}