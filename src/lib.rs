use chrono::naive::{
    NaiveDate,
    NaiveDateTime
};
use encoding_rs::{CoderResult, Encoder, Encoding, Decoder};
use futures::{
    future::{
        BoxFuture,
        Future,
        Lazy,
        lazy
    },
    lock::Mutex,
    stream::Stream,
    task::{
        Context,
        Poll
    }
};
use std::{
    convert::TryInto,
    fmt::{
        Display
    },
    fs::{
        File
    },
    io::{
        Read
    },
    iter::{
        FromIterator,
        FusedIterator
    },
    ops::{
        Deref,
        DerefMut,
        Index,
        IndexMut
    },
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

/// All field that may need a conversion layer between actual
/// value and underlying value
pub trait ConversionField<T> {
    fn get(&self) -> T;
    fn set(&mut self, value: &T);
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
    fn datatype_flag(&self) -> u8;
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
    /// For autoincrement field, after id is used, how much increment
    /// should it be.
    /// The default increment value is 1.
    fn id_step(&self) -> u32 {
        if self.autoincrement() {
            1
        } else {
            0
        }
    }
}

/// Operation conversion from/to bytes into field
pub trait FieldOps : FieldMeta + Display + Send {
    /// Parse bytes based on current meta data and update the state
    fn from_record_bytes(&mut self) -> BoxFuture<()>;
    /// Return bytes represent by this field.
    /// The result is a byte slice with length equals to size stored in meta data.
    fn to_bytes(&self) -> BoxFuture<&[u8]>;
    /// Return true if the field is ready to be read
    fn ready(&self) -> bool;
}

/// Type wrapper to wrap trait object inside Vec.
/// This ease user on type annotation only.
/// It implement Deref and DerefMut into Vec so
/// user can treat it like Vec.
/// 
/// User can either index directy into each field and
/// call `from_record_bytes` on the field that user want to use
/// or simply call `load_all` which will load every field in this record.
/// Both method is `async` which mean user need to `await` for
/// each field to be ready to read.
pub struct Record (Vec<Box<dyn FieldOps>>);

impl Record {
    /// Load up all fields in this record.
    /// It return Vec of Future where each Future represent
    /// each field byte parsing.
    /// The order of Future in Vec is similar to the order
    /// of field in the record.
    pub fn load_all(&mut self) -> Vec<BoxFuture<()>> {
        self.iter_mut().map(|field| field.from_record_bytes()).collect()
    }
    /// Load up all fields in this record.
    /// It return Vec of Future where each Future represent
    /// each field byte parsing.
    /// When any field is loaded, the future return an index of completed
    /// one. This can help improve efficiency when order is unimportant.
    pub fn load_all_unordered(&mut self) -> Vec<BoxFuture<usize>> {
        self.iter_mut().enumerate().map(|(i, field)| 
            Box::pin(async move {
                field.from_record_bytes();
                i
            }) as BoxFuture<usize> // need explicit type
        ).collect()
    }
}

impl Deref for Record {
    type Target=Vec<Box<dyn FieldOps>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Record {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Record operation.
/// It is mandatory to implement this trait for all record.
/// There's two mandatory functions.
/// 
/// First is `from_bytes` where it convert slice of bytes into
/// a struct that represent the record.
/// Second is `to_bytes` where it serialize current content of this
/// struct into `Vec<u8>` so that it can be persisted.
pub trait RecordOps {
    /// Parse a slice of bytes and construct a record from it.
    fn from_bytes(record: &[u8]) -> Self;
    /// Convert this struct as `Vec<u8>`
    fn to_bytes(&self) -> Vec<u8>;
}

pub trait DynamicRecordOps: RecordOps {
    /// Get a field of this record at index `i` as string
    #[allow(unused)]
    fn get_string(&self, i: usize) -> &String {
        unimplemented!("Operation not support")
    }
    /// Set a field of this record at index `i` as given string
    #[allow(unused)]
    fn set_string(&mut self, i: usize, value: &String) {
        unimplemented!("Operation not support")
    }

    /// Get a field of this record at index `i` as &str
    #[allow(unused)]
    fn get_str(&self, i: usize) -> &str {
        unimplemented!("Operation not support")
    }

    /// Set a field of this record at index `i` as &str
    #[allow(unused)]
    fn set_str(&mut self, i: usize, value: &str) {
        unimplemented!("Operation not support")
    }

    /// Get a field of this record at index `i` as u8
    #[allow(unused)]
    fn get_u8(&self, i: usize) -> u8 {
        unimplemented!("Operation not support")
    }

    /// Set a field of this record at index `i` as u8
    #[allow(unused)]
    fn set_u8(&mut self, i: usize, value: u8) {
        unimplemented!("Operation not support")
    }

    /// Get a field of this record at index `i` as u16
    #[allow(unused)]
    fn get_u16(&self, i: usize) -> u16 {
        unimplemented!("Operation not support")
    }

    /// Set a field of this record at index `i` as u16
    #[allow(unused)]
    fn set_u16(&mut self, i: usize, value: u16) {
        unimplemented!("Operation not support")
    }

    /// Get a field of this record at index `i` as u32
    #[allow(unused)]
    fn get_u32(&self, i: usize) -> u32 {
        unimplemented!("Operation not support")
    }

    /// Set a field of this record at index `i` as u32
    #[allow(unused)]
    fn set_u32(&mut self, i: usize, value: u32) {
        unimplemented!("Operation not support")
    }

    /// Get a field of this record at index `i` as u64
    #[allow(unused)]
    fn get_u64(&self, i: usize) -> u64 {
        unimplemented!("Operation not support")
    }

    /// Set a field of this record at index `i` as u64
    #[allow(unused)]
    fn set_u64(&mut self, i: usize, value: u64) {
        unimplemented!("Operation not support")
    }

    /// Get a field of this record at index `i` as u128
    #[allow(unused)]
    fn get_u128(&self, i: usize) -> u128 {
        unimplemented!("Operation not support")
    }

    /// Set a field of this record at index `i` as u128
    #[allow(unused)]
    fn set_u128(&mut self, i: usize, value: u128) {
        unimplemented!("Operation not support")
    }

    /// Get a field of this record at index `i` as i8
    #[allow(unused)]
    fn get_i8(&self, i: usize) -> i8 {
        unimplemented!("Operation not support")
    }

    /// Set a field of this record at index `i` as i8
    #[allow(unused)]
    fn set_i8(&mut self, i: usize, value: i8) {
        unimplemented!("Operation not support")
    }

    /// Get a field of this record at index `i` as i16
    #[allow(unused)]
    fn get_i16(&self, i: usize) -> i16 {
        unimplemented!("Operation not support")
    }

    /// Set a field of this record at index `i` as i16
    #[allow(unused)]
    fn set_i16(&mut self, i: usize, value: i16) {
        unimplemented!("Operation not support")
    }

    /// Get a field of this record at index `i` as i32
    #[allow(unused)]
    fn get_i32(&self, i: usize) -> i32 {
        unimplemented!("Operation not support")
    }

    /// Set a field of this record at index `i` as i32
    #[allow(unused)]
    fn set_i32(&mut self, i: usize, value: i32) {
        unimplemented!("Operation not support")
    }

    /// Get a field of this record at index `i` as i64
    #[allow(unused)]
    fn get_i64(&self, i: usize) -> i64 {
        unimplemented!("Operation not support")
    }

    /// Set a field of this record at index `i` as i64
    #[allow(unused)]
    fn set_i64(&mut self, i: usize, value: i64) {
        unimplemented!("Operation not support")
    }

    /// Get a field of this record at index `i` as i128
    #[allow(unused)]
    fn get_i128(&self, i: usize) -> i128 {
        unimplemented!("Operation not support")
    }

    /// Set a field of this record at index `i` as i128
    #[allow(unused)]
    fn set_i128(&mut self, i: usize, value: i128) {
        unimplemented!("Operation not support")
    }

    /// Get a field of this record at index `i` as f32
    #[allow(unused)]
    fn get_f32(&self, i: usize) -> f32 {
        unimplemented!("Operation not support")
    }

    /// Set a field of this record at index `i` as f32
    #[allow(unused)]
    fn set_f32(&mut self, i: usize, value: f32) {
        unimplemented!("Operation not support")
    }

    /// Get a field of this record at index `i` as f64
    #[allow(unused)]
    fn get_f64(&self, i: usize) -> f64 {
        unimplemented!("Operation not support")
    }

    /// Set a field of this record at index `i` as f64
    #[allow(unused)]
    fn set_f64(&mut self, i: usize, value: f64) {
        unimplemented!("Operation not support")
    }

    /// Get a field of this record at index `i` as `NaiveDate`
    #[allow(unused)]
    fn get_date(&self, i: usize) -> NaiveDate {
        unimplemented!("Operation not support")
    }

    /// Set a field of this record at index `i` as given NaiveDate
    #[allow(unused)]
    fn set_date(&mut self, i: usize, value: &NaiveDate) {
        unimplemented!("Operation not support")
    }

    /// Get a field of this record at index `i` as `NaiveDateTime`
    #[allow(unused)]
    fn get_datetime(&self, i: usize) -> NaiveDateTime {
        unimplemented!("Operation not support")
    }

    /// Set a field of this record at index `i` as given NaiveDateTime
    #[allow(unused)]
    fn set_datetime(&mut self, i: usize, value: &NaiveDateTime) {
        unimplemented!("Operation not support")
    }
}

/// Standard table operation.
/// It can be indexed to access each record.
/// It can be iterated to read each record in streaming fashion.
/// 
/// All table shall be iteratable and can be directly access by indexing.
/// Both Iterator and indexing will return the same type `ROW`.
pub trait TableOps: IntoIterator<Item=<Self as TableOps>::Row> + Index<usize, Output=<Self as TableOps>::Row> + IndexMut<usize> + FromIterator<<Self as TableOps>::Row> {
    type Row: RecordOps;

    /// Insert all the rows into this table.
    /// Each row shall be clonable.
    fn insert(&mut self, rows: &[Self::Row]) where Self::Row: Clone {
        rows.iter().for_each(|r| self.insert_owned(r.clone()));
    }

    /// Take the row and put it into this table.
    /// This won't clone the row.
    fn insert_owned(&mut self, row: Self::Row);

    /// Perform aggregation operation on this table.
    /// This is just a syntax sugar for `table.iter().fold(initial_value, op)`.
    fn aggregate<F, I>(&self, initial_value: I, op: F) -> I where for<'r> F: FnMut(I, &'r Self::Row) -> I {
        self.iter().fold(initial_value, op)
    }

    /// Join two table together.
    /// It can be used by table1.join(&table2).with(|r1, r2| Some(NewRecord {/* put field here */}));
    fn join<'a, 'b, T1>(&'a self, other: &'b T1) -> JoinConditionBuilder<'a, 'b, Self, T1> where 'a: 'b, T1: TableOps {
        JoinConditionBuilder {
            lhs: self,
            rhs: other
        }
    }

    /// Update table by evaluate each row in the table and feed each row as `&mut` to `op` function
    fn update<F>(&mut self, mut op: F) where for<'r> F: FnMut(&'r mut Self::Row) {
        for i in 0..self.len() {
            op(&mut self[i]);
        }
    }

    /// Make a query operation on table.
    /// The condition and return type will be based on condition function.
    /// The condition is a function that take an instance of RecordOps as parameter
    /// then return a new instance of RecordOps.
    /// This `select` function return new instance of TableOps.
    fn select<F, T>(&self, condition: F) -> T where F: Fn(&Self::Row) -> Option<T::Row>, T: TableOps {
        T::from_iter(
            self.iter().filter_map(|r| {
                condition(r)
            })
        )
    }
    
    /// Return the number of record in this table
    fn len(&self) -> usize;

    /// Return an iterator over table which yield reference to each row.
    /// This is a default implementation where it use running cursor to
    /// index into each element in this table.
    fn iter<'a>(&'a self) -> TableIter<'a, Self, Self::Row> {
        TableIter {
            i: 0,
            table: self
        }
    }
}

/// A very straight forward implementation of generic Iterator for any table.
/// It simply return a record by using indexing and move the cursor by 1.
pub struct TableIter<'a, T, ROW> where T: 'a + TableOps<Row=ROW>, ROW: 'a + RecordOps {
    i: usize,
    table: &'a T
}

impl<'a, T, ROW> Iterator for TableIter<'a, T, ROW> 
    where T: 'a + TableOps<Row=ROW>, ROW: 'a + RecordOps 
{
    type Item=&'a ROW;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.table.len() {
            return None;
        }

        let r = Some(&self.table[self.i]);
        self.i = self.i + 1;
        return r;
    }

    /// Since this iterator use indexing technique under the hood,
    /// we don't need to call `next` for `n` times.
    /// This override such behavior and use direct indexing method.
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if self.i + n < self.table.len() {
            let new_i = self.i + n;
            self.i = new_i;
            Some(&self.table[new_i])
        } else {
            None
        }
    }
}

impl<'a, T, ROW> ExactSizeIterator for TableIter<'a, T, ROW> 
    where T: 'a + TableOps<Row=ROW>, ROW: 'a + RecordOps 
{
    fn len(&self) -> usize {
        self.table.len()
    }
}

impl<'a, T, ROW> FusedIterator for TableIter<'a, T, ROW> 
where T: 'a + TableOps<Row=ROW>, ROW: 'a + RecordOps 
{

}

/// The struct is a result of using `on` function from trait `TableOps`.
/// It'd not be construct manually.
/// See [trait TrainOps](trait.TableOps.html#method.on)
pub struct JoinConditionBuilder<'a, 'b, T1, T2> 
    where 'a: 'b, T1: 'a + TableOps, T2: 'b + TableOps 
{
    lhs: &'a T1,
    rhs: &'b T2
}

impl<'a, 'b, T1, T2> JoinConditionBuilder<'a, 'b, T1, T2> 
    where 'a: 'b, T1: TableOps, T2: TableOps 
{
    pub fn on<F, T3>(self, conditional_fn: F) -> T3 
        where 
            for<'r, 's> F: Fn(&'r T1::Row, &'s T2::Row) -> Option<T3::Row>, 
            T3: TableOps 
    {
        join(self.lhs, self.rhs, conditional_fn)
    }
}

pub struct JoinTableIter<'a, 'b, COND, T1, T2, ROW1, ROW2, ROW3> 
    where
        'a: 'b, 
        COND: for<'r, 's> Fn(&'r ROW1, &'s ROW2) -> Option<ROW3>,
        T1: TableOps<Row=ROW1>,
        T2: TableOps<Row=ROW2>, 
        ROW1: RecordOps, 
        ROW2: RecordOps,
        ROW3: RecordOps 
{
    cond_fn: COND,
    t1_iter: TableIter<'a, T1, ROW1>,
    table2: &'b T2,
    t2_iter: TableIter<'b, T2, ROW2>,
    r1: Option<&'a ROW1>
}

impl<'a, 'b, COND, T1, T2, ROW1, ROW2, ROW3> JoinTableIter<'a, 'b, COND, T1, T2, ROW1, ROW2, ROW3> 
    where
        'a: 'b, 
        COND: for<'r, 's> Fn(&'r ROW1, &'s ROW2) -> Option<ROW3>,
        T1: TableOps<Row=ROW1>,
        T2: TableOps<Row=ROW2>, 
        ROW1: RecordOps, 
        ROW2: RecordOps,
        ROW3: RecordOps 
{
    fn new(table1: &'a T1, table2: &'b T2, cond_fn: COND) -> Self {
        JoinTableIter {
            cond_fn: cond_fn,
            t1_iter: table1.iter(),
            table2: table2,
            t2_iter: table2.iter(),
            r1: None
        }
    }
}

impl<'a, 'b, COND, T1, T2, ROW1, ROW2, ROW3> Iterator for JoinTableIter<'a, 'b, COND, T1, T2, ROW1, ROW2, ROW3>
    where
        'a: 'b,
        COND: for<'r, 's> Fn(&'r ROW1, &'s ROW2) -> Option<ROW3>,
        T1: TableOps<Row=ROW1>,
        T2: TableOps<Row=ROW2>, 
        ROW1: RecordOps, 
        ROW2: RecordOps,
        ROW3: RecordOps
{
    type Item=ROW3;

    fn next(&mut self) -> Option<Self::Item> {
        if self.r1.is_none() {
            self.r1 = self.t1_iter.next()
        }

        while let Some(ref r1) = self.r1 {
            while let Some(r2) = self.t2_iter.next() {
                if let Some(r3) = (self.cond_fn)(r1, r2) {
                    return Some(r3);
                }
            }

            self.r1 = self.t1_iter.next();
            self.t2_iter = self.table2.iter();
        }

        None
    }

    /// It return max number of row between the two joining table.
    /// Typical join operation yield less or equals to highest number of rows between two joining table.
    /// The only exception is the "cross join" type where number of row is equals to 
    /// `table1.len() ^ table2.len()`.
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(std::cmp::max(self.t1_iter.len(), self.table2.len())))
    }
}

impl<'a, 'b, COND, T1, T2, ROW1, ROW2, ROW3> FusedIterator for JoinTableIter<'a, 'b, COND, T1, T2, ROW1, ROW2, ROW3> 
where
    'a: 'b,
    COND: for<'r, 's> Fn(&'r ROW1, &'s ROW2) -> Option<ROW3>,
    T1: TableOps<Row=ROW1>,
    T2: TableOps<Row=ROW2>, 
    ROW1: RecordOps, 
    ROW2: RecordOps,
    ROW3: RecordOps
{

}

pub fn join<T1, T2, T3, ROW1, ROW2, ROW3>(table1: &T1, table2: &T2, cond: impl Fn(&ROW1, &ROW2) -> Option<ROW3>) -> T3 
    where 
        T1: TableOps<Row=ROW1>, 
        T2: TableOps<Row=ROW2>, 
        T3: TableOps<Row=ROW3>, 
        ROW1: RecordOps, 
        ROW2: RecordOps, 
        ROW3: RecordOps 
{
    let join_iter = JoinTableIter::new(table1, table2, cond);
    T3::from_iter(join_iter)
}

/// The table that's entirely stored in memory by Vec.
pub struct InMemoryTable<T> where T: RecordOps {
    rows: Vec<T>
}

impl<T> std::fmt::Debug for InMemoryTable<T> where T: std::fmt::Debug + RecordOps {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.rows)
    }
}

impl<T> PartialEq for InMemoryTable<T> where T: PartialEq + RecordOps {
    fn eq(&self, other: &Self) -> bool {
        self.rows == other.rows
    }
}

impl<T> IntoIterator for InMemoryTable<T> where T: RecordOps {
    type Item=T;
    type IntoIter=std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a InMemoryTable<T> where T: RecordOps {
    type Item=&'a T;
    type IntoIter=std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.as_slice().into_iter()
    }
}

impl<'a, T> IntoIterator for &'a mut InMemoryTable<T> where T: RecordOps {
    type Item=&'a mut T;
    type IntoIter=std::slice::IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.as_mut_slice().into_iter()
    }
}

impl<T> FromIterator<T> for InMemoryTable<T> where T: RecordOps {
    fn from_iter<I: IntoIterator<Item=T>>(iter: I) -> Self {
        InMemoryTable {
            rows: iter.into_iter().map(|item| item).collect()
        }
    }
}

impl<T> Index<usize> for InMemoryTable<T> where T: RecordOps {
    type Output=T;

    fn index(&self, i: usize) -> &Self::Output {
        &self.rows[i]
    }
}

impl<T> IndexMut<usize> for InMemoryTable<T> where T: RecordOps {
    fn index_mut(&mut self, i: usize) -> &mut Self::Output {
        &mut self.rows[i]
    }
}

impl<'a, T> TableOps for InMemoryTable<T> where T: 'a + RecordOps {
    type Row=T;

    fn insert_owned(&mut self, row: Self::Row) {
        self.rows.push(row)
    }

    fn len(&self) -> usize {
        self.rows.len()
    }
}

#[derive(Debug)]
pub struct Header {
    pub db_type: DBFType,
    pub last_update: NaiveDate,
    pub records_count: usize,
    pub first_record_position: usize,
    pub record_len: usize,
    pub table_flag: u8,
    pub codepage: &'static str
}

/// Read a first byte of dbf file and return an Enum that represent the
/// DBF's type. If it doesn't recognize the first byte, it'll be 
/// [DBFType::Undefined](enum.DBFType.html#variant.Undefined)
/// 
/// # Return
/// [DBFType](enum.DBFType.html)
pub fn read_dbf_type<P: std::convert::AsRef<std::path::Path> + std::fmt::Debug>(path: P) -> std::io::Result<DBFType> {
    let mut file = match File::open(&path) {
        Ok(f) => f,
        Err(_) => panic!("Fail to read file from {:?}", path)
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

/// Read header from given file.
/// 
/// According to this [link](http://www.dbfree.org/webdocs/1-documentation/b-dbf_header_specifications.htm)
/// (The link is healthy as of 2019-12-30)
/// ## Header 
/// ---
/// | Byte Offset | Description |
/// | --- | --- |
/// | 0 | DBF File type: <br/>0x02 FoxBASE<br/> 0x03 FoxBASE+/Dbase III plus, no memo<br/> 0x30   Visual FoxPro<br/> 0x31   Visual FoxPro, autoincrement enabled<br/> 0x32   Visual FoxPro with field type Varchar or Varbinary<br/>0x43   dBASE IV SQL table files, no memo<br/>0x63   dBASE IV SQL system files, no memo<br/>0x83   FoxBASE+/dBASE III PLUS, with memo<br/>0x8B   dBASE IV with memo<br/>0xCB   dBASE IV SQL table files, with memo<br/>0xF5   FoxPro 2.x (or earlier) with memo<br/>0xE5   HiPer-Six format with SMT memo file<br/>0xFB   FoxBASE |
/// | 1 - 3 | Last update (YYMMDD) |
/// | 4 - 7 | Number of records in file |
/// | 8 - 9 | Position of first data record |
/// | 10 - 11 | Length of one data record, including delete flag |
/// | 12 - 27 | Reserved |
/// | 28 | Table flags: <br/>0x01 file has structural .cdx<br/>0x02 file has a Memo field<br/>0x04 file is a database (.dbc)<br/>This byte can contain the sum of above value. For example 0x03 = 0x01 + 0x02 |
/// | 29 | Code page mark |
/// | 30 - 31 | Reserved, must be all 0 |
/// | 32 - n | Field subrecords <br/>The number of fields determines the number of field subrecords. One field subrecord exist for each field in the table |
/// | n + 1 | Header record terminator, must be 0x0D |
/// | n + 2 to n + 264 | VFP only. A 263-byte range that contains the backlink, which is relative path of an associated database (.dbc) file, information. If the first byte is 0x00, the file is not associated with a database. Thus database files always have 0x00. |
/// ---
pub async fn read_header<P: std::convert::AsRef<std::path::Path> + Display>(p: P, cp_mapper: impl Fn(u8) -> Result<&'static str, &'static str>) -> std::io::Result<Header> {
    let common = &mut [0; 32];
    let mut f = File::open(p).unwrap();
    f.read_exact(common)?;
    
    Ok(Header {
        db_type: DBFType::parse_type(common[0]),
        last_update: NaiveDate::from_ymd(common[1] as i32, common[2] as u32, common[3] as u32),
        records_count: u32::from_le_bytes(common[4..8].try_into().unwrap()) as usize,
        first_record_position: u16::from_le_bytes(common[8..10].try_into().unwrap()) as usize,
        record_len: u16::from_le_bytes(common[10..=11].try_into().unwrap()) as usize,
        table_flag: common[28],
        codepage: match cp_mapper(common[29]) {
            Ok(cp) => cp,
            Err(msg) => panic!(msg)
        }
    })
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