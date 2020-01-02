use super::*;

#[test]
fn test_create_records() {
    let record = &[
        b'a', b'b', 
         1u8,    0,    0, 0,    0,    0,    0, 0,
        0xCC, 0x40, 0x0B, 0,    0,    0,    0, 0, // Feb 29, 2020
        0x1E, 0x85, 0x25, 0, 0x80, 0x1C, 0xCA, 2]; // Feb 29, 2020: 13:00:00
    let expected = [
        "ab",
        "0.0001",
        "2020-02-29",
        "2020-02-29 13:00:00"
    ];
    let mut r = Record {
        i: 0,
        fields: vec![
            Box::new(CharField {
                meta: Field {
                    autoincrement: None,
                    binary: None,
                    name: "Just a char field".to_owned(),
                    next_id: 0u32,
                    nullable: None,
                    offset: 0,
                    precision: 0,
                    size: 2,
                    step: 1u32,
                    system: None
                },
                codepage: "tis-620",
                content: String::with_capacity(2 * 4),
                record: record
            }),
            Box::new(CurrencyField {
                meta: Field {
                    autoincrement: None,
                    binary: None,
                    name: "Just a money field".to_owned(),
                    next_id: 0u32,
                    nullable: None,
                    offset: 2,
                    precision: 0,
                    size: 8,
                    step: 1u32,
                    system: None
                },
                content: String::with_capacity(256),
                record: record
            }),
            Box::new(DateField {
                meta: Field {
                    autoincrement: None,
                    binary: None,
                    name: "Just a date field".to_owned(),
                    next_id: 0u32,
                    nullable: None,
                    offset: 10,
                    precision: 0,
                    size: 8,
                    step: 1u32,
                    system: None
                },
                content: NaiveDate::from_num_days_from_ce(0),
                record: record
            }),
            Box::new(DateTimeField {
                meta: Field {
                    autoincrement: None,
                    binary: None,
                    name: "Just a date_time field".to_owned(),
                    next_id: 0u32,
                    nullable: None,
                    offset: 18,
                    precision: 0,
                    size: 8,
                    step: 1u32,
                    system: None
                },
                content: NaiveDate::from_num_days_from_ce(0).and_hms(0, 0, 0),
                record: record
            })
        ]
    };

    for (i, ref mut f) in r.fields.iter_mut().enumerate() {
        f.from_record_bytes();
        assert_eq!(expected[i], format!("{}", f));
    }
}