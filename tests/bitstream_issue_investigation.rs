/// Comprehensive tests to investigate the 9 claimed issues in test.result
/// Each test targets a specific claim to determine if it's a real bug.
use kuba_tsdb::compression::bit_stream::{BitReader, BitWriter};

/// ISSUE #1: Bit Ordering (MSB vs LSB) Is Inconsistent
/// Claim: "reversed bit patterns" and "incorrect reconstruction of values above 8 bits"
#[test]
fn test_issue1_bit_ordering_consistency() {
    // Test 1: Single byte pattern - should preserve exact bit order
    let mut writer = BitWriter::new();
    writer.write_bits(0b10101100, 8);
    let buffer = writer.finish().unwrap();

    println!("Written pattern: 0b10101100");
    println!("Buffer bytes: {:08b}", buffer[0]);
    assert_eq!(buffer[0], 0b10101100, "Single byte should match exactly");

    let mut reader = BitReader::new(&buffer);
    let read_value = reader.read_bits(8).unwrap();
    assert_eq!(
        read_value, 0b10101100,
        "Read value should match written value"
    );

    // Test 2: Multi-byte pattern (16 bits)
    let mut writer = BitWriter::new();
    writer.write_bits(0b1010110011110000, 16);
    let buffer = writer.finish().unwrap();

    println!("\nWritten 16-bit pattern: 0b1010110011110000");
    println!("Buffer[0]: {:08b}, Buffer[1]: {:08b}", buffer[0], buffer[1]);

    let mut reader = BitReader::new(&buffer);
    let read_value = reader.read_bits(16).unwrap();
    assert_eq!(read_value, 0b1010110011110000, "16-bit value should match");

    // Test 3: Bit-by-bit verification
    let pattern = 0b11010010;
    let mut writer = BitWriter::new();
    writer.write_bits(pattern, 8);
    let buffer = writer.finish().unwrap();

    let mut reader = BitReader::new(&buffer);
    for i in (0..8).rev() {
        let expected_bit = (pattern >> i) & 1 == 1;
        let read_bit = reader.read_bit().unwrap();
        assert_eq!(
            read_bit,
            expected_bit,
            "Bit {} mismatch: expected {}, got {}",
            7 - i,
            expected_bit,
            read_bit
        );
    }
}

/// ISSUE #2: 64-bit Boundary Logic Is Incorrect
/// Claim: "incorrect extraction of the highest bit" and "silent truncation"
#[test]
fn test_issue2_64bit_boundary() {
    let test_cases = vec![
        0xFFFFFFFFFFFFFFFF, // All 1s
        0x0000000000000000, // All 0s
        0x8000000000000000, // Only MSB set
        0x0000000000000001, // Only LSB set
        0x8000000000000001, // MSB and LSB set
        0xAAAAAAAAAAAAAAAA, // Alternating 1010...
        0x5555555555555555, // Alternating 0101...
    ];

    for &value in &test_cases {
        let mut writer = BitWriter::new();
        writer.write_bits(value, 64);
        let buffer = writer.finish().unwrap();

        let mut reader = BitReader::new(&buffer);
        let read_value = reader.read_bits(64).unwrap();

        assert_eq!(
            read_value, value,
            "64-bit value mismatch: written 0x{:016X}, read 0x{:016X}",
            value, read_value
        );
    }
}

/// ISSUE #3: Cross-Byte Writes Cause Off-By-One Errors
/// Claim: "shifted bit sequences", "double-writes", "overwritten bits"
#[test]
fn test_issue3_cross_byte_boundaries() {
    // Write sequence: 1 bit, 7 bits, 2 bits (crosses byte boundaries)
    let mut writer = BitWriter::new();
    writer.write_bits(0b1, 1); // 1 bit: 1
    writer.write_bits(0b0101010, 7); // 7 bits: 0101010
    writer.write_bits(0b11, 2); // 2 bits: 11

    let buffer = writer.finish().unwrap();
    println!("Cross-byte buffer: {:08b} {:08b}", buffer[0], buffer[1]);

    // Expected layout:
    // Byte 0: [1][0101010] = 10101010 = 0xAA
    // Byte 1: [11][000000] = 11000000 = 0xC0
    assert_eq!(buffer[0], 0b10101010, "First byte should be 0b10101010");
    assert_eq!(buffer[1], 0b11000000, "Second byte should be 0b11000000");

    // Verify by reading back
    let mut reader = BitReader::new(&buffer);
    assert_eq!(reader.read_bits(1).unwrap(), 0b1, "First bit mismatch");
    assert_eq!(
        reader.read_bits(7).unwrap(),
        0b0101010,
        "Next 7 bits mismatch"
    );
    assert_eq!(reader.read_bits(2).unwrap(), 0b11, "Last 2 bits mismatch");
}

/// ISSUE #4: is_at_end() Gives Wrong Results
/// Claim: "returns true even though data remains" when bits remain in final byte
#[test]
fn test_issue4_is_at_end_semantics() {
    let data = vec![0xFF]; // One byte
    let mut reader = BitReader::new(&data);

    // Initially not at end
    assert!(!reader.is_at_end(), "Should not be at end initially");

    // Read 5 bits (still in first byte)
    reader.read_bits(5).unwrap();
    let at_end_after_5_bits = reader.is_at_end();
    println!(
        "is_at_end() after reading 5 of 8 bits: {}",
        at_end_after_5_bits
    );
    println!("Position after 5 bits: {:?}", reader.position());

    // The document claims this should return false (data remains)
    // but current implementation might return true

    // Read remaining 3 bits
    reader.read_bits(3).unwrap();
    println!("Position after all 8 bits: {:?}", reader.position());

    // Now should definitely be at end
    assert!(
        reader.is_at_end(),
        "Should be at end after reading all bits"
    );

    // Try to read more - should fail
    assert!(reader.read_bit().is_err(), "Reading past end should error");
}

/// ISSUE #5: Partial Byte Padding Behavior Is Not Defined Properly
/// Claim: Padding zeros not guaranteed, flush behavior unclear
#[test]
fn test_issue5_partial_byte_padding() {
    // Write only 4 bits
    let mut writer = BitWriter::new();
    writer.write_bits(0b1011, 4);
    let buffer = writer.finish().unwrap();

    assert_eq!(buffer.len(), 1, "Should produce 1 byte");
    println!("4-bit write (0b1011) produces byte: {:08b}", buffer[0]);

    // Expected: 0b10110000 (4 bits data + 4 bits zero padding)
    // The claim suggests padding might not be zeros
    assert_eq!(buffer[0], 0b10110000, "Padding should be zeros");

    // Verify padding bits are actually zero
    let padding_bits = buffer[0] & 0b00001111;
    assert_eq!(padding_bits, 0, "Lower 4 bits should be zero-padded");
}

/// ISSUE #6: Value Masking Behavior Is Incorrect for Small bit-widths
/// Claim: "doesn't mask the input value to the requested number of bits"
#[test]
fn test_issue6_value_masking() {
    // Write value 0b10110000 but only use 4 bits
    // Should only write lower 4 bits: 0b0000
    let mut writer = BitWriter::new();
    writer.write_bits(0b10110000, 4);
    let buffer = writer.finish().unwrap();

    println!("Writing 0b10110000 with num_bits=4");
    println!("Result byte: {:08b}", buffer[0]);

    // The implementation writes MSB-first from bit positions (num_bits-1) down to 0
    // For value=0b10110000, num_bits=4:
    //   Bit 3: (0b10110000 >> 3) & 1 = 0b00010110 & 1 = 0
    //   Bit 2: (0b10110000 >> 2) & 1 = 0b00101100 & 1 = 0
    //   Bit 1: (0b10110000 >> 1) & 1 = 0b01011000 & 1 = 0
    //   Bit 0: (0b10110000 >> 0) & 1 = 0b10110000 & 1 = 0
    // So it should write 0b0000, padded to 0b00000000

    // But if masking is wrong, it might write from higher bits
    // Let's test what actually happens
    let mut reader = BitReader::new(&buffer);
    let read_value = reader.read_bits(4).unwrap();
    println!("Read back (4 bits): 0b{:04b}", read_value);

    // Correct behavior: should read the lower 4 bits of input (0b0000)
    // If bug exists: might read upper 4 bits (0b1011)

    // Test with another value
    let mut writer = BitWriter::new();
    writer.write_bits(0xFF, 4); // All bits set, but only write 4
    let buffer = writer.finish().unwrap();

    let mut reader = BitReader::new(&buffer);
    let read_value = reader.read_bits(4).unwrap();
    println!(
        "Writing 0xFF with num_bits=4, read back: 0b{:04b}",
        read_value
    );

    // Should be 0b1111 (lower 4 bits of 0xFF)
    assert_eq!(read_value, 0b1111, "Should read lower 4 bits");
}

/// ISSUE #7: No Protection Against Over-Reading
/// Claim: "garbage data being returned" when reading beyond buffer
#[test]
fn test_issue7_over_reading_protection() {
    let data = vec![0xFF]; // One byte = 8 bits
    let mut reader = BitReader::new(&data);

    // Read all 8 bits - should succeed
    let result = reader.read_bits(8);
    assert!(result.is_ok(), "Reading 8 bits should succeed");

    // Try to read 1 more bit - should fail
    let result = reader.read_bit();
    println!("Reading past end result: {:?}", result);
    assert!(result.is_err(), "Reading past end should return error");

    // Try to read multiple bits past end
    let result = reader.read_bits(8);
    assert!(result.is_err(), "Reading bits past end should return error");

    // Test partial over-read: buffer has 8 bits, try to read 10
    let data = vec![0xFF];
    let mut reader = BitReader::new(&data);
    let result = reader.read_bits(10);
    println!("Reading 10 bits from 8-bit buffer: {:?}", result);
    assert!(
        result.is_err(),
        "Reading more bits than available should error"
    );
}

/// ISSUE #8: Cursor Synchronization Errors (Writer vs Reader)
/// Claim: Different assumptions about bit order, flush timing, padding, end conditions
#[test]
fn test_issue8_writer_reader_synchronization() {
    // Test various patterns to ensure writer and reader are in sync

    // Test 1: Aligned writes (8-bit boundaries)
    let mut writer = BitWriter::new();
    writer.write_bits(0xAA, 8);
    writer.write_bits(0x55, 8);
    writer.write_bits(0xFF, 8);
    let buffer = writer.finish().unwrap();

    let mut reader = BitReader::new(&buffer);
    assert_eq!(reader.read_bits(8).unwrap(), 0xAA);
    assert_eq!(reader.read_bits(8).unwrap(), 0x55);
    assert_eq!(reader.read_bits(8).unwrap(), 0xFF);

    // Test 2: Unaligned writes
    let mut writer = BitWriter::new();
    writer.write_bits(0b111, 3);
    writer.write_bits(0b00000, 5);
    writer.write_bits(0b1010, 4);
    writer.write_bits(0b0101, 4);
    let buffer = writer.finish().unwrap();

    let mut reader = BitReader::new(&buffer);
    assert_eq!(reader.read_bits(3).unwrap(), 0b111);
    assert_eq!(reader.read_bits(5).unwrap(), 0b00000);
    assert_eq!(reader.read_bits(4).unwrap(), 0b1010);
    assert_eq!(reader.read_bits(4).unwrap(), 0b0101);

    // Test 3: Mixed bit/bits operations
    let mut writer = BitWriter::new();
    writer.write_bit(true);
    writer.write_bits(0b101, 3);
    writer.write_bit(false);
    writer.write_bits(0b11, 2);
    writer.write_bit(true);
    let buffer = writer.finish().unwrap();

    let mut reader = BitReader::new(&buffer);
    assert!(reader.read_bit().unwrap());
    assert_eq!(reader.read_bits(3).unwrap(), 0b101);
    assert!(!reader.read_bit().unwrap());
    assert_eq!(reader.read_bits(2).unwrap(), 0b11);
    assert!(reader.read_bit().unwrap());
}

/// ISSUE #9: Randomized Fuzzing Shows Non-Deterministic Failures
/// Claim: "certain bit combinations decode to different values than written"
#[test]
fn test_issue9_fuzz_verification() {
    use rand::Rng;

    let mut rng = rand::rng();
    let mut failures = Vec::new();

    for iteration in 0..100_000 {
        let num_bits: u8 = rng.random_range(1..=64);
        let value: u64 = rng.random();

        let mut writer = BitWriter::new();
        writer.write_bits(value, num_bits);
        let buffer = writer.finish().unwrap();

        let mut reader = BitReader::new(&buffer);
        let read_value = reader.read_bits(num_bits).unwrap();

        let mask = if num_bits == 64 {
            u64::MAX
        } else {
            (1u64 << num_bits) - 1
        };

        let expected = value & mask;

        if read_value != expected {
            failures.push((iteration, num_bits, value, expected, read_value));
            if failures.len() >= 10 {
                break; // Stop after finding 10 failures
            }
        }
    }

    if !failures.is_empty() {
        println!("\nFound {} failures in fuzz testing:", failures.len());
        for (iter, bits, val, exp, got) in &failures {
            println!(
                "  Iteration {}: {} bits, value=0x{:016X}, expected=0x{:016X}, got=0x{:016X}",
                iter, bits, val, exp, got
            );
        }
        panic!("Fuzz test found {} failures", failures.len());
    }
}
