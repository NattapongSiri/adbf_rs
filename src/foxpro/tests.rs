use super::*;

#[test]
fn test_create_records() {
    let record = &[b'A', 0, 1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
    println!("{:?}", record);
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
                content: String::new(),
                record: record
            }),
            Box::new(CurrencyField {
                meta: Field {
                    autoincrement: None,
                    binary: None,
                    name: "Just a money field".to_owned(),
                    next_id: 0u32,
                    nullable: None,
                    offset: 0,
                    precision: 0,
                    size: 2,
                    step: 1u32,
                    system: None
                },
                content: String::new(),
                record: record
            })
        ]
    };

    r.fields[0].from_record_bytes();
    r.fields[1].from_record_bytes();
    println!("{}:{}", r.fields[0].name(), r.fields[0]);
    println!("{}:{}", r.fields[1].name(), r.fields[1]);
}