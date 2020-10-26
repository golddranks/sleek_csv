use core::fmt::{self, Write};

pub fn write_ascii_escaped(formatter: &mut fmt::Formatter, input: &[u8]) -> fmt::Result {
    for byte in input {
        for esc_byte in core::ascii::escape_default(*byte) {
            formatter.write_char(esc_byte as char)?;
        }
    }
    Ok(())
}

#[test]
fn test_write_ascii_escaped() {
    use arrayvec::ArrayString;
    use core::fmt::Write;
    struct Test(&'static [u8]);

    impl fmt::Debug for Test {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write_ascii_escaped(f, self.0)
        }
    }

    fn test(input: &'static [u8], expected: &'static str) {
        let mut output = ArrayString::<[_; 100]>::new();

        write!(&mut output, "{:?}", Test(input)).unwrap();
        assert_eq!(&output, expected);
    }

    test(b"test", "test");
    test(b"aaa\tbbb", "aaa\\tbbb");
    test(b"aaa\x00bbb", "aaa\\x00bbb");
    test(b"aaa\xffbbb", "aaa\\xffbbb");
    test(
        b"aaa\xff\xf0\xf0\xf0\xf0bbb",
        "aaa\\xff\\xf0\\xf0\\xf0\\xf0bbb",
    );
}

pub fn write_record<'a, 'b>(
    f: &'a mut fmt::Formatter,
    mut fields: impl Iterator<Item = &'b [u8]>,
) -> fmt::Result {
    f.write_str("{")?;
    if let Some(field) = fields.next() {
        write_ascii_escaped(f, field)?;
    }
    for field in fields {
        f.write_str(",")?;
        write_ascii_escaped(f, field)?;
    }
    f.write_str("}")?;
    Ok(())
}
