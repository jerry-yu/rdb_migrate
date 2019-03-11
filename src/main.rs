extern crate clap;
extern crate kvdb_rocksdb as cita_db;
extern crate log;

use cita_db::{Database, DatabaseConfig};
use clap::App;

const MEM_BUGET : usize = 4096;
const COLUMN_NUM : u32 = 7;


fn migrate(src_db : &Database, dst_db: &Database, col :Option<u32>) {
    let mut flag = true;
    let mut entry_num = 0;
    if let Some(mut iter) = src_db.iter(col) {
        while flag {
            let mut batch = dst_db.transaction();
            while let Some( (k,v)  ) =  iter.next(){
                batch.put(col, &k, &v);
                entry_num += 1;
                //println!("get entry_num {} column {:?}",entry_num,col);
                if entry_num & 0x7f == 0 {
                    break;
                }
            }
            if let Some( (k,v) ) =  iter.next() {
                batch.put(col, &k, &v);
            } else {
                flag = false;
            }
            dst_db.write(batch).unwrap();
        }
    }
}

fn main() {
    let matches = App::new("cita-recover")
        //.version(get_build_info_str(true))
        .author("yubo")
        .about("CITA Block Chain Node powered by Rust")
        .args_from_usage("-d, --dst=[PATH] 'Set dst data dir'")
        .args_from_usage("-s, --src=[PATH] 'Set src data dir'")
        .get_matches();


    let src_path = matches.value_of("src").expect("specify the src db dir. -h for help");
    let dst_path = matches.value_of("dst").unwrap_or("./dst_data");

    println!("src_path {}",src_path);
    let mut database_config = DatabaseConfig::with_columns(Some(COLUMN_NUM));
    database_config.memory_budget = Some(MEM_BUGET);

    let dst_db = Database::open(&database_config, &*dst_path).expect("Dst DB dir not right");
    let src_db = Database::open(&database_config, &*src_path).expect("Src DB dir not right");

    for i in  0..COLUMN_NUM{
        migrate(&src_db,&dst_db,Some(i));
    }
}
