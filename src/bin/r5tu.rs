#![cfg(feature = "oxigraph")]
#![allow(deprecated)]
use std::{env, fs::File, io::BufReader, path::PathBuf, time::Instant};
use rdf5d::{StreamingWriter, writer::{WriterOptions}, Term, Quint, R5tuFile};
use oxigraph::io::RdfFormat;
use oxigraph::store::Store;
use oxigraph::model::GraphNameRef;

fn usage() {
    eprintln!("r5tu (build-graph|build-dataset|stat) [options]\n\nCommands:\n  build-graph   --input <file> [--input <file> ...] --output <file> [--format <turtle|ntriples|rdfxml>] [--id <str>] [--graphname <str>] [--zstd] [--no-crc]\n  build-dataset --input <file> [--input <file> ...] --output <file> [--format <trig|nquads>] [--id <str>] [--default-graphname <str>] [--zstd] [--no-crc]\n  stat          --file <r5tu file> [--verbose] [--graphname <g>] [--list]\n");
 }

fn parse_flag(args: &[String], flag: &str) -> bool { args.iter().any(|a| a == flag) }
fn parse_opt(args: &[String], key: &str) -> Option<String> {
    args.windows(2).find(|w| w[0] == key).map(|w| w[1].clone())
}

fn parse_multi_inputs(args: &[String]) -> Vec<PathBuf> {
    let mut inputs = Vec::new();
    let mut i = 0usize;
    while i + 1 < args.len() {
        if args[i] == "--input" { inputs.push(PathBuf::from(&args[i+1])); i += 2; } else { i += 1; }
    }
    inputs
}

fn infer_graph_rdf_format(ext: &str) -> Option<RdfFormat> {
    match ext.to_ascii_lowercase().as_str() {
        "nt" | "ntriples" => Some(RdfFormat::NTriples),
        "ttl" | "turtle" => Some(RdfFormat::Turtle),
        "rdf" | "xml" | "rdfxml" => Some(RdfFormat::RdfXml),
        _ => None,
    }
}
fn infer_dataset_rdf_format(ext: &str) -> Option<RdfFormat> {
    match ext.to_ascii_lowercase().as_str() {
        "nq" | "nquads" => Some(RdfFormat::NQuads),
        "trig" => Some(RdfFormat::TriG),
        _ => None,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 { usage(); std::process::exit(2); }
    let cmd = &args[1];
    match cmd.as_str() {
        "build-graph" => {
            let inputs = parse_multi_inputs(&args);
            if inputs.is_empty() { return Err("--input required (can be repeated)".into()); }
            let output = parse_opt(&args, "--output").map(PathBuf::from).ok_or("--output required")?;
            let fmt = parse_opt(&args, "--format");
            let id_global = parse_opt(&args, "--id");
            let gname_opt = parse_opt(&args, "--graphname");
            let opts = WriterOptions { zstd: parse_flag(&args, "--zstd"), with_crc: !parse_flag(&args, "--no-crc") };
            let mut w = StreamingWriter::new(&output, opts);
            let start = Instant::now();
            for input in inputs {
                let f = File::open(&input)?; let mut rdr = BufReader::new(f);
                let rfmt: RdfFormat = match fmt.as_deref() {
                    Some("turtle") => RdfFormat::Turtle,
                    Some("ntriples") => RdfFormat::NTriples,
                    Some("rdfxml") => RdfFormat::RdfXml,
                    Some(other) => infer_graph_rdf_format(other).ok_or("unknown graph format")?,
                    None => infer_graph_rdf_format(input.extension().and_then(|e| e.to_str()).unwrap_or("")).unwrap_or(RdfFormat::Turtle),
                };
                // Load into store via BulkLoader (explicit fast path)
                let store = Store::new()?;
                let loader = store.bulk_loader();
                loader.load_graph(&mut rdr, rfmt, GraphNameRef::DefaultGraph, None)?;
                let gname_auto = rdf5d::writer::detect_graphname_from_store(&store).unwrap_or_else(|| gname_opt.clone().unwrap_or_else(|| "default".to_string()));
                let id = id_global.clone().unwrap_or_else(|| input.to_string_lossy().to_string());
                // Stream loaded triples from default graph into our writer
                let mut n = 0usize;
                for q in store.quads_for_pattern(None, None, None, Some(GraphNameRef::DefaultGraph)) {
                    let q = q?; n += 1;
                    let s = match q.subject { oxigraph::model::Subject::NamedNode(nm) => Term::Iri(nm.as_str().to_string()), oxigraph::model::Subject::BlankNode(b) => Term::BNode(format!("_:{}", b.as_str())), _ => continue };
                    let p = Term::Iri(q.predicate.as_str().to_string());
                    let o = match q.object { oxigraph::model::Term::NamedNode(nm) => Term::Iri(nm.as_str().to_string()), oxigraph::model::Term::BlankNode(b) => Term::BNode(format!("_:{}", b.as_str())), oxigraph::model::Term::Literal(l) => { let lex=l.value().to_string(); if let Some(lang)=l.language(){ Term::Literal{lex,dt:None,lang:Some(lang.to_string())} } else { Term::Literal{lex,dt:Some(l.datatype().as_str().to_string()),lang:None} } }, _ => continue };
                    w.add(Quint{ id: id.clone(), s, p, o, gname: gname_auto.clone() })?;
                }
                println!("Added graph id='{}' graphname='{}' ({} triples) from '{}'", id, gname_auto, n, input.display());
            }
            w.finalize()?;
            eprintln!("built in {:?}", start.elapsed());
        }
        "build-dataset" => {
            let inputs = parse_multi_inputs(&args);
            if inputs.is_empty() { return Err("--input required (can be repeated)".into()); }
            let output = parse_opt(&args, "--output").map(PathBuf::from).ok_or("--output required")?;
            let fmt = parse_opt(&args, "--format");
            let id_global = parse_opt(&args, "--id");
            let default_g = parse_opt(&args, "--default-graphname").unwrap_or_else(|| "default".to_string());
            let opts = WriterOptions { zstd: parse_flag(&args, "--zstd"), with_crc: !parse_flag(&args, "--no-crc") };
            let mut w = StreamingWriter::new(&output, opts);
            let start = Instant::now();
            for input in inputs {
                let f = File::open(&input)?; let mut rdr = BufReader::new(f);
                let rfmt: RdfFormat = match fmt.as_deref() {
                    Some("trig") => RdfFormat::TriG,
                    Some("nquads") => RdfFormat::NQuads,
                    Some(other) => infer_dataset_rdf_format(other).ok_or("unknown dataset format")?,
                    None => infer_dataset_rdf_format(input.extension().and_then(|e| e.to_str()).unwrap_or("")).unwrap_or(RdfFormat::NQuads),
                };
                let store = Store::new()?;
                store.load_dataset(&mut rdr, rfmt, None)?;
                let id = id_global.clone().unwrap_or_else(|| input.to_string_lossy().to_string());
                let mut n = 0usize;
                for q in store.quads_for_pattern(None, None, None, None) {
                    let q = q?; n += 1;
                    let s = match q.subject { oxigraph::model::Subject::NamedNode(nm) => Term::Iri(nm.as_str().to_string()), oxigraph::model::Subject::BlankNode(b) => Term::BNode(format!("_:{}", b.as_str())), _ => continue };
                    let p = Term::Iri(q.predicate.as_str().to_string());
                    let o = match q.object { oxigraph::model::Term::NamedNode(nm) => Term::Iri(nm.as_str().to_string()), oxigraph::model::Term::BlankNode(b) => Term::BNode(format!("_:{}", b.as_str())), oxigraph::model::Term::Literal(l) => { let lex=l.value().to_string(); if let Some(lang)=l.language(){ Term::Literal{lex,dt:None,lang:Some(lang.to_string())} } else { Term::Literal{lex,dt:Some(l.datatype().as_str().to_string()),lang:None} } }, _ => continue };
                    let gname = match q.graph_name { oxigraph::model::GraphName::DefaultGraph => default_g.clone(), oxigraph::model::GraphName::NamedNode(nm) => nm.as_str().to_string(), oxigraph::model::GraphName::BlankNode(b) => format!("_:{}", b.as_str()), };
                    w.add(Quint{ id: id.clone(), s, p, o, gname })?;
                }
                println!("Added dataset id='{}' quads={} from '{}'", id, n, input.display());
            }
            w.finalize()?;
            eprintln!("built in {:?}", start.elapsed());
        }
        "stat" => {
            let file = parse_opt(&args, "--file").map(PathBuf::from).ok_or("--file required")?;
            let f = match R5tuFile::open(&file) {
                Ok(f) => f,
                Err(e) => { eprintln!("stat: failed to open '{}': {}\nHint: Use 'build-graph' or 'build-dataset' to produce an .r5tu file first.", file.display(), e); std::process::exit(2); }
            };
            let verbose = parse_flag(&args, "--verbose");
            let list = parse_flag(&args, "--list");
            let filter_g = parse_opt(&args, "--graphname");
            let toc = f.toc();
            eprintln!("sections: {}", toc.len());
            if verbose {
                let h = f.header();
                eprintln!("header.magic='{}' version={} flags=0x{:04x} created_unix={} toc_off={} toc_len={}",
                    std::str::from_utf8(&h.magic).unwrap_or("????"), h.version_u16, h.flags_u16, h.created_unix64, h.toc_off_u64, h.toc_len_u32);
                for (i, e) in toc.iter().enumerate() {
                    eprintln!("  [{}] kind={:?} off={} len={} crc={}", i, e.kind, e.section.off, e.section.len, e.crc32_u32);
                }
            }
            let start = Instant::now();
            let mut n_triples = 0u64;
            let graphs = if let Some(gname) = filter_g.as_ref() { f.enumerate_by_graphname(gname)? } else { f.enumerate_all()? };
            let n_graphs = graphs.len() as u64;
            for gr in &graphs { n_triples += gr.n_triples; }
            eprintln!("graphs: {} triples: {} in {:?}", n_graphs, n_triples, start.elapsed());
            if list {
                for gr in graphs {
                    println!("gid={} id='{}' graphname='{}' n_triples={}", gr.gid, gr.id, gr.graphname, gr.n_triples);
                }
            }
        }
        _ => { usage(); std::process::exit(2); }
    }
    Ok(())
}
