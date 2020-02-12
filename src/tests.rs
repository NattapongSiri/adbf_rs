use super::*;

#[test]
fn test_convert_byte_to_struct() {
    let record = &[
        b'a', b'b', 
         1u8,    0,    0, 0,    0,    0,    0, 0,
        0xCC, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
    ];

    #[derive(Debug, PartialEq)]
    struct Rec {
        date: NaiveDate,
        name: String,
        cost: f64
    }

    impl RecordOps for Rec {
        fn from_bytes(record: &[u8]) -> Rec {
            let encoding = Encoding::for_label("tis-620".as_bytes()).unwrap();
            let mut decoder = encoding.new_decoder();
            let mut name = String::with_capacity(2);
            let (dec_res, readed, _last) = decoder.decode_to_string(&record[0..2], &mut name, false);
            if readed != 2 {
                match dec_res {
                    CoderResult::InputEmpty => {
                        panic!("Insufficient record data.");
                    },
                    CoderResult::OutputFull => {
                        panic!("Insufficient buffer to store converted string");
                    }
                }
            }
            let cost = i64::from_le_bytes(record[2..10].try_into().unwrap());
            let date = NaiveDate::from_num_days_from_ce(u64::from_le_bytes(record[10..18].try_into().unwrap()) as i32);
            Rec {
                date: date,
                name: name,
                cost: (cost as f64) / 10000f64
            }
        }

        fn to_bytes(&self) -> Vec<u8> {
            let encoding = Encoding::for_label("tis-620".as_bytes()).unwrap();
            let mut encoder = encoding.new_encoder();
            let mut result = Vec::with_capacity(18);
            unsafe {result.set_len(18)}
            let (enc_res, _read_size, write_size, _last) = encoder.encode_from_utf8(self.name.as_str(), &mut result, true);
            if write_size != 2 {
                match enc_res {
                    CoderResult::InputEmpty => {
                        panic!("Insufficient record data.");
                    },
                    CoderResult::OutputFull => {
                        panic!("Insufficient buffer to store converted string");
                    }
                }
            }
            let cost = ((self.cost * 10000f64) as i64).to_le_bytes();
            result[2..10].iter_mut().zip(cost.iter()).for_each(|(r, c)| {*r = *c});
            let days = (self.date - NaiveDate::from_num_days_from_ce(0)).num_days().to_le_bytes();
            result[10..].iter_mut().zip(days.iter()).for_each(|(r, c)| {*r = *c});

            result
        }
    }

    // Convert from bytes into struct
    let rec = Rec::from_bytes(record);

    // Expected Record data comparison
    assert_eq!(rec, Rec{ date: NaiveDate::from_ymd(2020, 2, 29), name: "ab".to_string(), cost: 0.0001});

    // Convert from struct back into bytes then compare to source
    assert_eq!(record, rec.to_bytes().as_slice());
}

#[test]
fn test_in_memory_table() {
    let records1 = &[
        &[
            b'a', b'b', 
            1u8,    0,    0, 0,    0,    0,    0, 0,
            0xCC, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ],
        &[
            b'c', b'd', 
            2u8,    0,    0, 0,    0,    0,    0, 0,
            0xCD, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ]
    ];

    let _records2 = &[
        &[
            b'g', b'g', 
            1u8,    0,    0, 0,    0,    0,    0, 0,
            0xCC, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ],
        &[
            b'f', b'f', 
            2u8,    0,    0, 0,    0,    0,    0, 0,
            0xCD, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ]
    ];

    #[derive(Debug, PartialEq)]
    struct Rec {
        date: NaiveDate,
        name: String,
        cost: f64
    }

    impl RecordOps for Rec {
        fn from_bytes(record: &[u8]) -> Rec {
            let encoding = Encoding::for_label("tis-620".as_bytes()).unwrap();
            let mut decoder = encoding.new_decoder();
            let mut name = String::with_capacity(2);
            let (dec_res, readed, _last) = decoder.decode_to_string(&record[0..2], &mut name, false);
            if readed != 2 {
                match dec_res {
                    CoderResult::InputEmpty => {
                        panic!("Insufficient record data.");
                    },
                    CoderResult::OutputFull => {
                        panic!("Insufficient buffer to store converted string");
                    }
                }
            }
            let cost = i64::from_le_bytes(record[2..10].try_into().unwrap());
            let date = NaiveDate::from_num_days_from_ce(u64::from_le_bytes(record[10..18].try_into().unwrap()) as i32);
            Rec {
                date: date,
                name: name,
                cost: (cost as f64) / 10000f64
            }
        }

        fn to_bytes(&self) -> Vec<u8> {
            let encoding = Encoding::for_label("tis-620".as_bytes()).unwrap();
            let mut encoder = encoding.new_encoder();
            let mut result = Vec::with_capacity(18);
            unsafe {result.set_len(18)}
            let (enc_res, _read_size, write_size, _last) = encoder.encode_from_utf8(self.name.as_str(), &mut result, true);
            if write_size != 2 {
                match enc_res {
                    CoderResult::InputEmpty => {
                        panic!("Insufficient record data.");
                    },
                    CoderResult::OutputFull => {
                        panic!("Insufficient buffer to store converted string");
                    }
                }
            }
            let cost = ((self.cost * 10000f64) as i64).to_le_bytes();
            result[2..10].iter_mut().zip(cost.iter()).for_each(|(r, c)| {*r = *c});
            let days = (self.date - NaiveDate::from_num_days_from_ce(0)).num_days().to_le_bytes();
            result[10..].iter_mut().zip(days.iter()).for_each(|(r, c)| {*r = *c});

            result
        }
    }
    
    let tb1 = InMemoryTable {
        rows: records1.into_iter().map(|r| Rec::from_bytes(*r)).collect()
    };

    assert_eq!(
        tb1, 
        InMemoryTable {
            rows: vec![
                Rec {
                    date: NaiveDate::from_ymd(2020, 2, 29),
                    name: "ab".to_string(),
                    cost: 0.0001
                },
                Rec {
                    date: NaiveDate::from_ymd(2020, 3, 01),
                    name: "cd".to_string(),
                    cost: 0.0002
                }
            ]
        }
    );
}

#[test]
fn test_join_table_fn() {
    let records1 = &[
        &[
            b'a', b'b', 
            1u8,    0,    0, 0,    0,    0,    0, 0,
            0xCC, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ],
        &[
            b'c', b'd', 
            2u8,    0,    0, 0,    0,    0,    0, 0,
            0xCD, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ]
    ];

    let records2 = &[
        &[
            b'g', b'g', 
            1u8,    0,    0, 0,    0,    0,    0, 0,
            0xCC, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ],
        &[
            b'f', b'f', 
            2u8,    0,    0, 0,    0,    0,    0, 0,
            0xCD, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ]
    ];

    #[derive(Debug, PartialEq)]
    struct Rec {
        date: NaiveDate,
        name: String,
        cost: f64
    }

    impl RecordOps for Rec {
        fn from_bytes(record: &[u8]) -> Rec {
            let encoding = Encoding::for_label("tis-620".as_bytes()).unwrap();
            let mut decoder = encoding.new_decoder();
            let mut name = String::with_capacity(2);
            let (dec_res, readed, _last) = decoder.decode_to_string(&record[0..2], &mut name, false);
            if readed != 2 {
                match dec_res {
                    CoderResult::InputEmpty => {
                        panic!("Insufficient record data.");
                    },
                    CoderResult::OutputFull => {
                        panic!("Insufficient buffer to store converted string");
                    }
                }
            }
            let cost = i64::from_le_bytes(record[2..10].try_into().unwrap());
            let date = NaiveDate::from_num_days_from_ce(u64::from_le_bytes(record[10..18].try_into().unwrap()) as i32);
            Rec {
                date: date,
                name: name,
                cost: (cost as f64) / 10000f64
            }
        }

        fn to_bytes(&self) -> Vec<u8> {
            let encoding = Encoding::for_label("tis-620".as_bytes()).unwrap();
            let mut encoder = encoding.new_encoder();
            let mut result = Vec::with_capacity(18);
            unsafe {result.set_len(18)}
            let (enc_res, _read_size, write_size, _last) = encoder.encode_from_utf8(self.name.as_str(), &mut result, true);
            if write_size != 2 {
                match enc_res {
                    CoderResult::InputEmpty => {
                        panic!("Insufficient record data.");
                    },
                    CoderResult::OutputFull => {
                        panic!("Insufficient buffer to store converted string");
                    }
                }
            }
            let cost = ((self.cost * 10000f64) as i64).to_le_bytes();
            result[2..10].iter_mut().zip(cost.iter()).for_each(|(r, c)| {*r = *c});
            let days = (self.date - NaiveDate::from_num_days_from_ce(0)).num_days().to_le_bytes();
            result[10..].iter_mut().zip(days.iter()).for_each(|(r, c)| {*r = *c});

            result
        }
    }

    #[derive(Debug, PartialEq)]
    struct JoinedRec {
        name1: String,
        name2: String
    }

    impl RecordOps for JoinedRec {
        fn from_bytes(_record: &[u8]) -> Self {
            unimplemented!("Doesn't support")
        }

        fn to_bytes(&self) -> Vec<u8> {
            self.name1.as_bytes().into_iter().chain(self.name2.as_bytes().into_iter()).map(|b| *b).collect()
        }
    }
    
    let tb1 = InMemoryTable {
        rows: records1.into_iter().map(|r| Rec::from_bytes(*r)).collect()
    };
    
    let tb2 = InMemoryTable {
        rows: records2.into_iter().map(|r| Rec::from_bytes(*r)).collect()
    };

    let tb3: InMemoryTable<JoinedRec> = join(&tb1, &tb2, |r1, r2| {
        if r1.cost == r2.cost {
            Some(JoinedRec {
                name1: r1.name.clone(),
                name2: r2.name.clone()
            })
        } else {
            None
        }
    });

    assert_eq!(
        tb3, 
        InMemoryTable {
            rows: vec![
                JoinedRec {
                    name1: "ab".to_string(),
                    name2: "gg".to_string()
                },
                JoinedRec {
                    name1: "cd".to_string(),
                    name2: "ff".to_string()
                }
            ]
        }
    );
}

#[test]
fn test_join_table_trait() {
    let records1 = &[
        &[
            b'a', b'b', 
            1u8,    0,    0, 0,    0,    0,    0, 0,
            0xCC, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ],
        &[
            b'c', b'd', 
            2u8,    0,    0, 0,    0,    0,    0, 0,
            0xCD, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ]
    ];

    let records2 = &[
        &[
            b'g', b'g', 
            1u8,    0,    0, 0,    0,    0,    0, 0,
            0xCC, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ],
        &[
            b'f', b'f', 
            2u8,    0,    0, 0,    0,    0,    0, 0,
            0xCD, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ]
    ];

    #[derive(Debug, PartialEq)]
    struct Rec {
        date: NaiveDate,
        name: String,
        cost: f64
    }

    impl RecordOps for Rec {
        fn from_bytes(record: &[u8]) -> Rec {
            let encoding = Encoding::for_label("tis-620".as_bytes()).unwrap();
            let mut decoder = encoding.new_decoder();
            let mut name = String::with_capacity(2);
            let (dec_res, readed, _last) = decoder.decode_to_string(&record[0..2], &mut name, false);
            if readed != 2 {
                match dec_res {
                    CoderResult::InputEmpty => {
                        panic!("Insufficient record data.");
                    },
                    CoderResult::OutputFull => {
                        panic!("Insufficient buffer to store converted string");
                    }
                }
            }
            let cost = i64::from_le_bytes(record[2..10].try_into().unwrap());
            let date = NaiveDate::from_num_days_from_ce(u64::from_le_bytes(record[10..18].try_into().unwrap()) as i32);
            Rec {
                date: date,
                name: name,
                cost: (cost as f64) / 10000f64
            }
        }

        fn to_bytes(&self) -> Vec<u8> {
            let encoding = Encoding::for_label("tis-620".as_bytes()).unwrap();
            let mut encoder = encoding.new_encoder();
            let mut result = Vec::with_capacity(18);
            unsafe {result.set_len(18)}
            let (enc_res, _read_size, write_size, _last) = encoder.encode_from_utf8(self.name.as_str(), &mut result, true);
            if write_size != 2 {
                match enc_res {
                    CoderResult::InputEmpty => {
                        panic!("Insufficient record data.");
                    },
                    CoderResult::OutputFull => {
                        panic!("Insufficient buffer to store converted string");
                    }
                }
            }
            let cost = ((self.cost * 10000f64) as i64).to_le_bytes();
            result[2..10].iter_mut().zip(cost.iter()).for_each(|(r, c)| {*r = *c});
            let days = (self.date - NaiveDate::from_num_days_from_ce(0)).num_days().to_le_bytes();
            result[10..].iter_mut().zip(days.iter()).for_each(|(r, c)| {*r = *c});

            result
        }
    }

    #[derive(Debug, PartialEq)]
    struct JoinedRec {
        name1: String,
        name2: String
    }

    impl RecordOps for JoinedRec {
        fn from_bytes(_record: &[u8]) -> Self {
            unimplemented!("Doesn't support")
        }

        fn to_bytes(&self) -> Vec<u8> {
            self.name1.as_bytes().into_iter().chain(self.name2.as_bytes().into_iter()).map(|b| *b).collect()
        }
    }
    
    let tb1 = InMemoryTable {
        rows: records1.into_iter().map(|r| Rec::from_bytes(*r)).collect()
    };
    
    let tb2 = InMemoryTable {
        rows: records2.into_iter().map(|r| Rec::from_bytes(*r)).collect()
    };

    let tb3: InMemoryTable<JoinedRec> = tb1.join(&tb2).on(|r1, r2| {
        if r1.cost == r2.cost {
            Some(JoinedRec {
                name1: r1.name.clone(),
                name2: r2.name.clone()
            })
        } else {
            None
        }
    });

    assert_eq!(
        tb3, 
        InMemoryTable {
            rows: vec![
                JoinedRec {
                    name1: "ab".to_string(),
                    name2: "gg".to_string()
                },
                JoinedRec {
                    name1: "cd".to_string(),
                    name2: "ff".to_string()
                }
            ]
        }
    );
}

#[test]
fn test_select() {
    let records1 = &[
        &[
            b'a', b'b', 
            1u8,    0,    0, 0,    0,    0,    0, 0,
            0xCC, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ],
        &[
            b'c', b'd', 
            2u8,    0,    0, 0,    0,    0,    0, 0,
            0xCD, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ],
        &[
            b'g', b'g', 
            1u8,    0,    0, 0,    0,    0,    0, 0,
            0xCC, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ],
        &[
            b'f', b'f', 
            2u8,    0,    0, 0,    0,    0,    0, 0,
            0xCD, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ]
    ];

    #[derive(Clone, Debug, PartialEq)]
    struct Rec {
        date: NaiveDate,
        name: String,
        cost: f64
    }

    impl RecordOps for Rec {
        fn from_bytes(record: &[u8]) -> Rec {
            let encoding = Encoding::for_label("tis-620".as_bytes()).unwrap();
            let mut decoder = encoding.new_decoder();
            let mut name = String::with_capacity(2);
            let (dec_res, readed, _last) = decoder.decode_to_string(&record[0..2], &mut name, false);
            if readed != 2 {
                match dec_res {
                    CoderResult::InputEmpty => {
                        panic!("Insufficient record data.");
                    },
                    CoderResult::OutputFull => {
                        panic!("Insufficient buffer to store converted string");
                    }
                }
            }
            let cost = i64::from_le_bytes(record[2..10].try_into().unwrap());
            let date = NaiveDate::from_num_days_from_ce(u64::from_le_bytes(record[10..18].try_into().unwrap()) as i32);
            Rec {
                date: date,
                name: name,
                cost: (cost as f64) / 10000f64
            }
        }

        fn to_bytes(&self) -> Vec<u8> {
            let encoding = Encoding::for_label("tis-620".as_bytes()).unwrap();
            let mut encoder = encoding.new_encoder();
            let mut result = Vec::with_capacity(18);
            unsafe {result.set_len(18)}
            let (enc_res, _read_size, write_size, _last) = encoder.encode_from_utf8(self.name.as_str(), &mut result, true);
            if write_size != 2 {
                match enc_res {
                    CoderResult::InputEmpty => {
                        panic!("Insufficient record data.");
                    },
                    CoderResult::OutputFull => {
                        panic!("Insufficient buffer to store converted string");
                    }
                }
            }
            let cost = ((self.cost * 10000f64) as i64).to_le_bytes();
            result[2..10].iter_mut().zip(cost.iter()).for_each(|(r, c)| {*r = *c});
            let days = (self.date - NaiveDate::from_num_days_from_ce(0)).num_days().to_le_bytes();
            result[10..].iter_mut().zip(days.iter()).for_each(|(r, c)| {*r = *c});

            result
        }
    }
    
    let tb1 = InMemoryTable {
        rows: records1.into_iter().map(|r| Rec::from_bytes(*r)).collect()
    };

    let result: InMemoryTable<Rec> = tb1.select(|r| {
        if r.cost == 0.0001 {
            Some(r.clone())
        } else {
            None
        }
    });

    assert_eq!(
        result, 
        InMemoryTable {
            rows: vec![
                Rec {
                    date: NaiveDate::from_ymd(2020, 2, 29),
                    name: "ab".to_string(),
                    cost: 0.0001
                },
                Rec {
                    date: NaiveDate::from_ymd(2020, 2, 29),
                    name: "gg".to_string(),
                    cost: 0.0001
                }
            ]
        }
    );
}

#[test]
fn test_in_memory_update() {
    let records1 = &[
        &[
            b'a', b'b', 
            1u8,    0,    0, 0,    0,    0,    0, 0,
            0xCC, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ],
        &[
            b'c', b'd', 
            2u8,    0,    0, 0,    0,    0,    0, 0,
            0xCD, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ]
    ];

    #[derive(Debug, PartialEq)]
    struct Rec {
        date: NaiveDate,
        name: String,
        cost: f64
    }

    impl RecordOps for Rec {
        fn from_bytes(record: &[u8]) -> Rec {
            let encoding = Encoding::for_label("tis-620".as_bytes()).unwrap();
            let mut decoder = encoding.new_decoder();
            let mut name = String::with_capacity(2);
            let (dec_res, readed, _last) = decoder.decode_to_string(&record[0..2], &mut name, false);
            if readed != 2 {
                match dec_res {
                    CoderResult::InputEmpty => {
                        panic!("Insufficient record data.");
                    },
                    CoderResult::OutputFull => {
                        panic!("Insufficient buffer to store converted string");
                    }
                }
            }
            let cost = i64::from_le_bytes(record[2..10].try_into().unwrap());
            let date = NaiveDate::from_num_days_from_ce(u64::from_le_bytes(record[10..18].try_into().unwrap()) as i32);
            Rec {
                date: date,
                name: name,
                cost: (cost as f64) / 10000f64
            }
        }

        fn to_bytes(&self) -> Vec<u8> {
            let encoding = Encoding::for_label("tis-620".as_bytes()).unwrap();
            let mut encoder = encoding.new_encoder();
            let mut result = Vec::with_capacity(18);
            unsafe {result.set_len(18)}
            let (enc_res, _read_size, write_size, _last) = encoder.encode_from_utf8(self.name.as_str(), &mut result, true);
            if write_size != 2 {
                match enc_res {
                    CoderResult::InputEmpty => {
                        panic!("Insufficient record data.");
                    },
                    CoderResult::OutputFull => {
                        panic!("Insufficient buffer to store converted string");
                    }
                }
            }
            let cost = ((self.cost * 10000f64) as i64).to_le_bytes();
            result[2..10].iter_mut().zip(cost.iter()).for_each(|(r, c)| {*r = *c});
            let days = (self.date - NaiveDate::from_num_days_from_ce(0)).num_days().to_le_bytes();
            result[10..].iter_mut().zip(days.iter()).for_each(|(r, c)| {*r = *c});

            result
        }
    }
    
    let mut tb1 = InMemoryTable {
        rows: records1.into_iter().map(|r| Rec::from_bytes(*r)).collect()
    };

    tb1.update(|mut r| {
        if r.cost < 0.0002 {
            r.cost = 0.0002;
        }
    });

    assert_eq!(
        tb1, 
        InMemoryTable {
            rows: vec![
                Rec {
                    date: NaiveDate::from_ymd(2020, 2, 29),
                    name: "ab".to_string(),
                    cost: 0.0002
                },
                Rec {
                    date: NaiveDate::from_ymd(2020, 3, 01),
                    name: "cd".to_string(),
                    cost: 0.0002
                }
            ]
        }
    );
}

#[test]
fn test_insert_rows() {
    #[derive(Clone, Debug, PartialEq)]
    struct Rec {
        date: NaiveDate,
        name: String,
        cost: f64
    }

    impl RecordOps for Rec {
        fn from_bytes(record: &[u8]) -> Rec {
            let encoding = Encoding::for_label("tis-620".as_bytes()).unwrap();
            let mut decoder = encoding.new_decoder();
            let mut name = String::with_capacity(2);
            let (dec_res, readed, _last) = decoder.decode_to_string(&record[0..2], &mut name, false);
            if readed != 2 {
                match dec_res {
                    CoderResult::InputEmpty => {
                        panic!("Insufficient record data.");
                    },
                    CoderResult::OutputFull => {
                        panic!("Insufficient buffer to store converted string");
                    }
                }
            }
            let cost = i64::from_le_bytes(record[2..10].try_into().unwrap());
            let date = NaiveDate::from_num_days_from_ce(u64::from_le_bytes(record[10..18].try_into().unwrap()) as i32);
            Rec {
                date: date,
                name: name,
                cost: (cost as f64) / 10000f64
            }
        }

        fn to_bytes(&self) -> Vec<u8> {
            let encoding = Encoding::for_label("tis-620".as_bytes()).unwrap();
            let mut encoder = encoding.new_encoder();
            let mut result = Vec::with_capacity(18);
            unsafe {result.set_len(18)}
            let (enc_res, _read_size, write_size, _last) = encoder.encode_from_utf8(self.name.as_str(), &mut result, true);
            if write_size != 2 {
                match enc_res {
                    CoderResult::InputEmpty => {
                        panic!("Insufficient record data.");
                    },
                    CoderResult::OutputFull => {
                        panic!("Insufficient buffer to store converted string");
                    }
                }
            }
            let cost = ((self.cost * 10000f64) as i64).to_le_bytes();
            result[2..10].iter_mut().zip(cost.iter()).for_each(|(r, c)| {*r = *c});
            let days = (self.date - NaiveDate::from_num_days_from_ce(0)).num_days().to_le_bytes();
            result[10..].iter_mut().zip(days.iter()).for_each(|(r, c)| {*r = *c});

            result
        }
    }

    let mut tb = InMemoryTable {
        rows: Vec::<Rec>::new()
    };

    tb.insert_owned(Rec {
        name: "ab".to_string(),
        cost: 1.0001,
        date: NaiveDate::from_ymd(2020, 10, 20)
    });

    let recs = &[
        Rec {
            name: "cd".to_string(),
            cost: 2.0002,
            date: NaiveDate::from_ymd(2019, 11, 21)
        }, 
        Rec {
            name: "ef".to_string(),
            cost: 3.0003,
            date: NaiveDate::from_ymd(2021, 03, 31)
        }
    ];

    tb.insert(recs);

    assert_eq!(tb, InMemoryTable {
        rows: vec![
            Rec {
                name: "ab".to_string(),
                cost: 1.0001,
                date: NaiveDate::from_ymd(2020, 10, 20)
            },
            Rec {
                name: "cd".to_string(),
                cost: 2.0002,
                date: NaiveDate::from_ymd(2019, 11, 21)
            }, 
            Rec {
                name: "ef".to_string(),
                cost: 3.0003,
                date: NaiveDate::from_ymd(2021, 03, 31)
            }
        ]
    })
}

#[test]
fn test_agg() {
    let records1 = &[
        &[
            b'a', b'b', 
            1u8,    0,    0, 0,    0,    0,    0, 0,
            0xCC, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ],
        &[
            b'c', b'd', 
            2u8,    0,    0, 0,    0,    0,    0, 0,
            0xCD, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        ]
    ];

    #[derive(Clone, Debug, PartialEq)]
    struct Rec {
        date: NaiveDate,
        name: String,
        cost: f64
    }

    impl RecordOps for Rec {
        fn from_bytes(record: &[u8]) -> Rec {
            let encoding = Encoding::for_label("tis-620".as_bytes()).unwrap();
            let mut decoder = encoding.new_decoder();
            let mut name = String::with_capacity(2);
            let (dec_res, readed, _last) = decoder.decode_to_string(&record[0..2], &mut name, false);
            if readed != 2 {
                match dec_res {
                    CoderResult::InputEmpty => {
                        panic!("Insufficient record data.");
                    },
                    CoderResult::OutputFull => {
                        panic!("Insufficient buffer to store converted string");
                    }
                }
            }
            let cost = i64::from_le_bytes(record[2..10].try_into().unwrap());
            let date = NaiveDate::from_num_days_from_ce(u64::from_le_bytes(record[10..18].try_into().unwrap()) as i32);
            Rec {
                date: date,
                name: name,
                cost: (cost as f64) / 10000f64
            }
        }

        fn to_bytes(&self) -> Vec<u8> {
            let encoding = Encoding::for_label("tis-620".as_bytes()).unwrap();
            let mut encoder = encoding.new_encoder();
            let mut result = Vec::with_capacity(18);
            unsafe {result.set_len(18)}
            let (enc_res, _read_size, write_size, _last) = encoder.encode_from_utf8(self.name.as_str(), &mut result, true);
            if write_size != 2 {
                match enc_res {
                    CoderResult::InputEmpty => {
                        panic!("Insufficient record data.");
                    },
                    CoderResult::OutputFull => {
                        panic!("Insufficient buffer to store converted string");
                    }
                }
            }
            let cost = ((self.cost * 10000f64) as i64).to_le_bytes();
            result[2..10].iter_mut().zip(cost.iter()).for_each(|(r, c)| {*r = *c});
            let days = (self.date - NaiveDate::from_num_days_from_ce(0)).num_days().to_le_bytes();
            result[10..].iter_mut().zip(days.iter()).for_each(|(r, c)| {*r = *c});

            result
        }
    }

    let tb = InMemoryTable {
        rows: records1.into_iter().map(|b| Rec::from_bytes(*b)).collect()
    };

    let result = tb.aggregate(100f64, |v, r| v + r.cost);

    assert_eq!(format!("{:.4}", result), "100.0003");

    let result = tb.aggregate("".to_owned(), |v, r| v + &r.name);
    assert_eq!(result, "abcd");
}