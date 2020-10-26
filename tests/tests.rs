use sleek_csv::{ByteRecordArena, Reader};

#[test]
fn test_migration_1() {
    let last_chunk = r#""01000000","R2000000","0040000","00","40000","20190000","漢漢漢漢","ｶｶｶｶ","19980000",201"#;
    let next_chunk = r#"9/10/01 1:11:11,"A000"
"01000000","R2000000","0040000","00","50000","20070000","かかかか漢漢漢漢漢漢","ｶｶｶｶｶｶｶｶｶ","19980000",2019/10/01 1:11:11,"A000"
"01000000","R2000000","0040000","00","50000","20060000","カカカカ漢"#.replace("\n", "\r\n");

    let mut last_arena = ByteRecordArena::new();
    let mut next_arena = ByteRecordArena::new();

    let mut reader = Reader::new(false, b',');

    reader
        .fill_arena(last_chunk.as_bytes(), &mut last_arena)
        .unwrap();
    last_arena.migrate_partial(&mut next_arena);
    reader
        .fill_arena(next_chunk.as_bytes(), &mut next_arena)
        .unwrap();
}

#[test]
fn test_migration_2() {
    let last_chunk = r#""01040000","R2110000","0040000","00","30000","20170000","カカカカカ漢漢漢漢漢","ｶｶｶｶｶｶｶｶｶｶｶｶｶｶｶ","#;
    let next_chunk = r#""20060000",2019/10/01 1:11:11,"A000"
"01040000","R2110000","0040000","00","30000","20130000",""#
        .replace("\n", "\r\n");

    let mut last_arena = ByteRecordArena::new();
    let mut next_arena = ByteRecordArena::new();

    let mut reader = Reader::new(false, b',');

    reader
        .fill_arena(last_chunk.as_bytes(), &mut last_arena)
        .unwrap();
    last_arena.migrate_partial(&mut next_arena);
    reader
        .fill_arena(next_chunk.as_bytes(), &mut next_arena)
        .unwrap();
}

#[test]
fn test_migration_3() {
    let chunk_a = r#""01000000","R2000000","0040000","00","50000","20070000","かかかか漢漢漢漢漢漢","ｶｶｶｶｶｶｶｶｶ","19980000",2019/10/01 1:11:11,"A000"
"01000000","R2000000","0040000","00","50000","20060000","カカカカ漢"#.replace("\n", "\r\n");
    let chunk_b = r#""#;
    let chunk_c = r#"漢漢漢漢"#;
    let chunk_d = r#"漢","ｶｶｶｶｶｶ"#;
    let chunk_e = r#"ｶｶｶ","19980000",2019/10/01 1:11:11,"A000"
"#
    .replace("\n", "\r\n");

    let mut arena_a = ByteRecordArena::new();
    let mut arena_b = ByteRecordArena::new();
    let mut arena_c = ByteRecordArena::new();
    let mut arena_d = ByteRecordArena::new();
    let mut arena_e = ByteRecordArena::new();

    let mut reader = Reader::new(false, b',');

    reader.fill_arena(chunk_a.as_bytes(), &mut arena_a).unwrap();
    arena_a.migrate_partial(&mut arena_b);
    reader.fill_arena(chunk_b.as_bytes(), &mut arena_b).unwrap();
    arena_b.migrate_partial(&mut arena_c);
    reader.fill_arena(chunk_c.as_bytes(), &mut arena_c).unwrap();
    arena_c.migrate_partial(&mut arena_d);
    reader.fill_arena(chunk_d.as_bytes(), &mut arena_d).unwrap();
    arena_d.migrate_partial(&mut arena_e);
    reader.fill_arena(chunk_e.as_bytes(), &mut arena_e).unwrap();

    assert_eq!(arena_e.record_count(), 1);
}

#[test]
fn test_count() {
    let chunk_a = r#""COL_1","COL_2","COL_3","COL_4","COL_5"
"QU","2012060000",77.00,2013/05/12 09:19:55,"N843"
"QU","2012060000",3.00,2013/05/12 09:19:55,"N843"
"#;
    let chunk_b = r#""QU","2013060000",7.00,2013/05/12 09:19:55,"N843"
"QU","2012060000",5.00,2013/05/12 09:19:55,"N843"
"QU","2012060000",4.00,2013/05/12 09:19:55,"N843"
"#;

    let mut arena_a = ByteRecordArena::new();
    let mut arena_b = ByteRecordArena::new();
    let mut reader = Reader::new(true, b',');

    reader.fill_arena(chunk_a.as_bytes(), &mut arena_a).unwrap();
    arena_a.migrate_partial(&mut arena_b);
    reader.fill_arena(chunk_b.as_bytes(), &mut arena_b).unwrap();

    assert_eq!(arena_a.record_count(), 2); // Header doesn't count
    assert_eq!(arena_b.record_count(), 3);
}
