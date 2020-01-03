use chrono::naive::{
    NaiveDate,
    NaiveDateTime
};
use encoding_rs::{Encoder, Encoding, Decoder};
use futures::{
    future::{
        BoxFuture,
        Future,
        lazy
    },
    stream::Stream,
    task::{
        Context,
        Poll
    }
};
use std::{
    fmt::{
        Display
    },
    fs::File,
    io::Read,
    ops::Index,
    pin::Pin
};

#[cfg(test)]
mod tests;

pub mod foxpro;

fn get_encoding(cp: &str) -> &'static Encoding {
    match Encoding::for_label(cp.as_bytes()) {
        Some(e) => e,
        None => {
            panic!("Fail to find encoding for codepage {}", cp)
        }
    }
}

fn get_decoder(cp: &str) -> Decoder {
    let encoding = get_encoding(cp);
    encoding.new_decoder()
}

fn get_encoder(cp: &str) -> Encoder {
    let encoding = get_encoding(cp);
    encoding.new_encoder()
}

#[derive(Debug)]
pub enum DBFType {
    FoxBase,
    DBaseIIIPlus,
    DBaseIV,
    DBaseV,
    VisualFoxPro,
    VisualFoxProAutoInc,
    VisualFoxProVarBLOB,
    DBaseIVSQLTableFiles,
    DBaseIVSQLSystem,
    DBaseIIIPlusMemos,
    DBaseIVMemos,
    DBaseIVSQLTable,
    FoxProMemos,
    Undefined
}

impl DBFType {
    pub fn parse_type(flag: u8) -> DBFType {
        match flag {
            0x02 => {
                DBFType::FoxBase
            },
            0x03 => {
                DBFType::DBaseIIIPlus
            },
            0x04 => {
                DBFType::DBaseIV
            }
            0x05 => {
                DBFType::DBaseV
            },
            0x30 => {
                DBFType::VisualFoxPro
            },
            0x31 => {
                DBFType::VisualFoxProAutoInc
            },
            0x32 => {
                DBFType::VisualFoxProVarBLOB
            },
            0x43 => {
                DBFType::DBaseIVSQLTableFiles
            },
            0x63 => {
                DBFType::DBaseIVSQLSystem
            },
            0x83 => {
                DBFType::DBaseIIIPlusMemos
            },
            0x8b => {
                DBFType::DBaseIVMemos
            },
            0x8e => {
                DBFType::DBaseIVSQLTable
            },
            0xf5 => {
                DBFType::FoxProMemos
            },
            _ => {
                DBFType::Undefined
            }
        }
    }
}

/// Field metadata.
pub trait FieldMeta {
    /// A field is nullable
    fn nullable(&self) -> bool;
    /// A field is auto-increment numeric
    fn autoincrement(&self) -> bool;
    /// Field name
    fn name(&self) -> &str;
    /// Offset of byte in each record
    fn rec_offset(&self) -> usize;
    /// Size in bytes of this field
    fn size(&self) -> usize;
    /// Precision of float/numeric field
    fn precision(&self) -> usize;
    /// Next auto incremented id
    fn next_id(&mut self) -> u32;
}

/// Operation conversion from/to bytes into field
pub trait FieldOps : FieldMeta + Display {
    /// Parse bytes based on current meta data and update the state
    fn from_record_bytes(&mut self) -> BoxFuture<()>;
    /// Return bytes represent by this field.
    /// The result is a byte slice with length equals to size stored in meta data.
    fn to_bytes(&self) -> BoxFuture<&[u8]>;
}

/// Record standard operation.
/// It can be indexed to access each field.
/// It can be iterated to read each field in this record in streaming fashion.
pub trait RecordOps<T>: 
    Stream<Item=Vec<T>> + 
    Index<usize, Output=Vec<T>>
where T: FieldOps + FieldMeta 
{
    
}

/// Standard table operation.
/// It can be indexed to access each record.
/// It can be iterated to read each record in streaming fashion.
pub trait TableOps<F> : RecordOps<F> where F: FieldOps + FieldMeta {
    fn join<V>(&self, other: impl TableOps<F>, condition: impl Fn(&[F], &[F]) -> Option<Vec<F>>) -> V where V: TableOps<F>;
    fn select<V>(&self, condition: impl Fn(&[F]) -> Option<Vec<F>>) -> V where V: TableOps<F>;
}

pub struct Header {
    pub db_type: DBFType,
    pub last_update: NaiveDate,
    pub records_count: usize,
    pub first_record_position: usize,
    record_len: usize,
    table_flag: u8,
    codepage: &'static str
}

pub struct Table {
    pub meta: Header,
    raw_bytes: Vec<u8>
}

/// Read a first byte of dbf file and return an Enum that represent the
/// DBF's type. If it doesn't recognize the first byte, it'll be 
/// [DBFType::Undefined](enum.DBFType.html#variant.Undefined)
/// 
/// # Return
/// [DBFType](enum.DBFType.html)
pub fn read_dbf_type<P: std::convert::AsRef<std::path::Path> + Display>(path: P) -> std::io::Result<DBFType> {
    let mut file = match File::open(&path) {
        Ok(f) => f,
        Err(_) => panic!("Fail to read file from {}", path)
    };
    let flag = &mut [0];
    match file.read_exact(flag) {
        Err(err) => {
            return std::io::Result::Err(err);
        },
        _ => {}
    }

    Ok(DBFType::parse_type(flag[0]))
}

// /// Load table into memory
// /// 
// /// This function load entire DBF file as `Vec<u8>` into memory.
// pub async fn load_table<P: std::convert::AsRef<std::path::Path> + Display>(path: P) -> Table {
//     let file = match File::open(&path) {
//         Ok(f) => f,
//         Err(_) => panic!("Fail to read file from {}", path)
//     };
//     let bytes : Vec<u8> = file.bytes().map(|b| b.expect("Fail to read some byte in given file")).collect();
//     let result: IResult<&[u8], &[u8]> = take(1usize)(bytes.as_slice());
//     let chunk = match result {
//         Ok(c) => c,
//         Err(_) => panic!("Fail to read some bytes of {}", path)
//     };
//     // let remain_bytes : &[u8] = chunk.0;
//     let file_type = DBFType::parse_type(chunk.1[0]);
//     let header = Header {db_type: file_type};
//     Table {meta: header, raw_bytes: bytes}
// }