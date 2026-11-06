use rune::runtime::{Bytes, Mut, Ref};
use rune::Any;
use std::fs::File;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom};

/// Byte order for typed reads, selected per call with the `fs::LE` / `fs::BE`
/// constants.
#[derive(Clone, Copy, Debug)]
pub enum ByteOrder {
    Le,
    Be,
}

impl ByteOrder {
    fn parse(order: &str) -> io::Result<ByteOrder> {
        if order.eq_ignore_ascii_case("le") {
            Ok(ByteOrder::Le)
        } else if order.eq_ignore_ascii_case("be") {
            Ok(ByteOrder::Be)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("byte order must be fs::LE (\"le\") or fs::BE (\"be\"), got \"{order}\""),
            ))
        }
    }
}

/// A binary file opened for reading typed values (scalars and arrays, in the
/// byte order given per call) at arbitrary offsets. Enables workloads to read
/// fixed-record binary data files without loading them into memory. First
/// `seek` to a record's offset, then read its fields. Reads are buffered and
/// advance the position. Hitting end-of-file surfaces as an `UnexpectedEof`
/// error.
#[derive(Any, Debug)]
pub struct BinaryFile {
    reader: BufReader<File>,
    len: u64,
}

#[allow(clippy::len_without_is_empty)]
impl BinaryFile {
    pub fn new(path: &str) -> io::Result<Self> {
        let file = File::open(path)
            .map_err(|e| io::Error::new(e.kind(), format!("Failed to open file {path}: {e}")))?;
        let len = file.metadata()?.len();
        Ok(BinaryFile {
            reader: BufReader::new(file),
            len,
        })
    }

    pub fn len(&self) -> i64 {
        self.len as i64
    }

    pub fn seek(&mut self, offset: i64) -> io::Result<()> {
        let offset: u64 = offset.try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("seek: offset must be non-negative, got {offset}"),
            )
        })?;
        if offset > self.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "seek: offset {offset} is beyond the file length {}",
                    self.len
                ),
            ));
        }
        self.reader.seek(SeekFrom::Start(offset))?;
        Ok(())
    }

    pub fn seek_relative(&mut self, delta: i64) -> io::Result<()> {
        let target = self.reader.stream_position()? as i128 + delta as i128;
        if target < 0 || target > self.len as i128 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "seek_relative: target position {target} is outside the file (length {})",
                    self.len
                ),
            ));
        }
        self.reader.seek_relative(delta)
    }

    pub fn read_u32(&mut self, order: ByteOrder) -> io::Result<i64> {
        let b = self.read_array()?;
        Ok(match order {
            ByteOrder::Le => u32::from_le_bytes(b),
            ByteOrder::Be => u32::from_be_bytes(b),
        } as i64)
    }

    pub fn read_i32(&mut self, order: ByteOrder) -> io::Result<i64> {
        let b = self.read_array()?;
        Ok(match order {
            ByteOrder::Le => i32::from_le_bytes(b),
            ByteOrder::Be => i32::from_be_bytes(b),
        } as i64)
    }

    pub fn read_i64(&mut self, order: ByteOrder) -> io::Result<i64> {
        let b = self.read_array()?;
        Ok(match order {
            ByteOrder::Le => i64::from_le_bytes(b),
            ByteOrder::Be => i64::from_be_bytes(b),
        })
    }

    pub fn read_f32(&mut self, order: ByteOrder) -> io::Result<f64> {
        let b = self.read_array()?;
        Ok(match order {
            ByteOrder::Le => f32::from_le_bytes(b),
            ByteOrder::Be => f32::from_be_bytes(b),
        } as f64)
    }

    pub fn read_f64(&mut self, order: ByteOrder) -> io::Result<f64> {
        let b = self.read_array()?;
        Ok(match order {
            ByteOrder::Le => f64::from_le_bytes(b),
            ByteOrder::Be => f64::from_be_bytes(b),
        })
    }

    pub fn read_f32_vec(&mut self, order: ByteOrder, count: i64) -> io::Result<Vec<f64>> {
        let buf = self.read_bulk(count, 4, "read_f32_vec")?;
        Ok(buf
            .chunks_exact(4)
            .map(|c| {
                let b = c.try_into().unwrap();
                (match order {
                    ByteOrder::Le => f32::from_le_bytes(b),
                    ByteOrder::Be => f32::from_be_bytes(b),
                }) as f64
            })
            .collect())
    }

    pub fn read_u32_vec(&mut self, order: ByteOrder, count: i64) -> io::Result<Vec<i64>> {
        let buf = self.read_bulk(count, 4, "read_u32_vec")?;
        Ok(buf
            .chunks_exact(4)
            .map(|c| {
                let b = c.try_into().unwrap();
                (match order {
                    ByteOrder::Le => u32::from_le_bytes(b),
                    ByteOrder::Be => u32::from_be_bytes(b),
                }) as i64
            })
            .collect())
    }

    pub fn read_f64_vec(&mut self, order: ByteOrder, count: i64) -> io::Result<Vec<f64>> {
        let buf = self.read_bulk(count, 8, "read_f64_vec")?;
        Ok(buf
            .chunks_exact(8)
            .map(|c| {
                let b = c.try_into().unwrap();
                match order {
                    ByteOrder::Le => f64::from_le_bytes(b),
                    ByteOrder::Be => f64::from_be_bytes(b),
                }
            })
            .collect())
    }

    pub fn read_i32_vec(&mut self, order: ByteOrder, count: i64) -> io::Result<Vec<i64>> {
        let buf = self.read_bulk(count, 4, "read_i32_vec")?;
        Ok(buf
            .chunks_exact(4)
            .map(|c| {
                let b = c.try_into().unwrap();
                (match order {
                    ByteOrder::Le => i32::from_le_bytes(b),
                    ByteOrder::Be => i32::from_be_bytes(b),
                }) as i64
            })
            .collect())
    }

    pub fn read_i64_vec(&mut self, order: ByteOrder, count: i64) -> io::Result<Vec<i64>> {
        let buf = self.read_bulk(count, 8, "read_i64_vec")?;
        Ok(buf
            .chunks_exact(8)
            .map(|c| {
                let b = c.try_into().unwrap();
                match order {
                    ByteOrder::Le => i64::from_le_bytes(b),
                    ByteOrder::Be => i64::from_be_bytes(b),
                }
            })
            .collect())
    }

    pub fn read_bytes(&mut self, count: i64) -> io::Result<Vec<u8>> {
        self.read_bulk(count, 1, "read_bytes")
    }

    fn read_array<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        let mut buf = [0u8; N];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn read_bulk(&mut self, count: i64, elem_size: usize, what: &str) -> io::Result<Vec<u8>> {
        let count: usize = count.try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{what}: element count must be non-negative, got {count}"),
            )
        })?;
        let bytes = count.checked_mul(elem_size).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{what}: element count {count} is too large"),
            )
        })?;
        let remaining = self.len.saturating_sub(self.reader.stream_position()?);
        if bytes as u64 > remaining {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("{what}: requested {bytes} bytes but only {remaining} remain in the file"),
            ));
        }
        let mut buf = vec![0u8; bytes];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }
}

/// Opens a binary file for reading. Returns a `BinaryFile` positioned at the
/// start of the file.
#[rune::function]
pub fn open_binary(path: &str) -> io::Result<BinaryFile> {
    BinaryFile::new(path)
}

/// Returns the file size in bytes.
#[rune::function(instance)]
pub fn len(file: Mut<BinaryFile>) -> i64 {
    file.len()
}

/// Moves the read position to an absolute byte offset from the start of the
/// file. The offset must lie within `0..=len`.
#[rune::function(instance)]
pub fn seek(mut file: Mut<BinaryFile>, offset: i64) -> io::Result<()> {
    BinaryFile::seek(&mut file, offset)
}

/// Moves the read position by the given number of bytes relative to the
/// current position (negative moves backwards). The resulting position must
/// lie within `0..=len`.
#[rune::function(instance)]
pub fn seek_relative(mut file: Mut<BinaryFile>, delta: i64) -> io::Result<()> {
    BinaryFile::seek_relative(&mut file, delta)
}

/// Reads an unsigned 32-bit integer.
#[rune::function(instance)]
pub fn read_u32(mut file: Mut<BinaryFile>, order: Ref<str>) -> io::Result<i64> {
    BinaryFile::read_u32(&mut file, ByteOrder::parse(&order)?)
}

/// Reads a signed 32-bit integer.
#[rune::function(instance)]
pub fn read_i32(mut file: Mut<BinaryFile>, order: Ref<str>) -> io::Result<i64> {
    BinaryFile::read_i32(&mut file, ByteOrder::parse(&order)?)
}

/// Reads a signed 64-bit integer.
#[rune::function(instance)]
pub fn read_i64(mut file: Mut<BinaryFile>, order: Ref<str>) -> io::Result<i64> {
    BinaryFile::read_i64(&mut file, ByteOrder::parse(&order)?)
}

/// Reads a 32-bit float.
#[rune::function(instance)]
pub fn read_f32(mut file: Mut<BinaryFile>, order: Ref<str>) -> io::Result<f64> {
    BinaryFile::read_f32(&mut file, ByteOrder::parse(&order)?)
}

/// Reads a 64-bit float.
#[rune::function(instance)]
pub fn read_f64(mut file: Mut<BinaryFile>, order: Ref<str>) -> io::Result<f64> {
    BinaryFile::read_f64(&mut file, ByteOrder::parse(&order)?)
}

/// Reads `count` 32-bit floats into a vector in one buffered read. This is
/// the preferred way to read a whole fixed-size record.
#[rune::function(instance)]
pub fn read_f32_vec(
    mut file: Mut<BinaryFile>,
    order: Ref<str>,
    count: i64,
) -> io::Result<Vec<f64>> {
    BinaryFile::read_f32_vec(&mut file, ByteOrder::parse(&order)?, count)
}

/// Reads `count` 64-bit floats into a vector in one buffered read.
#[rune::function(instance)]
pub fn read_f64_vec(
    mut file: Mut<BinaryFile>,
    order: Ref<str>,
    count: i64,
) -> io::Result<Vec<f64>> {
    BinaryFile::read_f64_vec(&mut file, ByteOrder::parse(&order)?, count)
}

/// Reads `count` unsigned 32-bit integers into a vector in one buffered read.
#[rune::function(instance)]
pub fn read_u32_vec(
    mut file: Mut<BinaryFile>,
    order: Ref<str>,
    count: i64,
) -> io::Result<Vec<i64>> {
    BinaryFile::read_u32_vec(&mut file, ByteOrder::parse(&order)?, count)
}

/// Reads `count` signed 32-bit integers into a vector in one buffered read.
#[rune::function(instance)]
pub fn read_i32_vec(
    mut file: Mut<BinaryFile>,
    order: Ref<str>,
    count: i64,
) -> io::Result<Vec<i64>> {
    BinaryFile::read_i32_vec(&mut file, ByteOrder::parse(&order)?, count)
}

/// Reads `count` signed 64-bit integers into a vector in one buffered read.
#[rune::function(instance)]
pub fn read_i64_vec(
    mut file: Mut<BinaryFile>,
    order: Ref<str>,
    count: i64,
) -> io::Result<Vec<i64>> {
    BinaryFile::read_i64_vec(&mut file, ByteOrder::parse(&order)?, count)
}

/// Reads `count` raw bytes into a byte string, with no interpretation of the
/// contents. Keeping fixed-size records as raw bytes instead of value vectors
/// avoids per-element overhead.
#[rune::function(instance)]
pub fn read_bytes(mut file: Mut<BinaryFile>, count: i64) -> io::Result<Bytes> {
    let buf = BinaryFile::read_bytes(&mut file, count)?;
    // Adopts the Vec allocation instead of copying the bytes.
    Bytes::try_from(buf).map_err(|e| io::Error::new(io::ErrorKind::OutOfMemory, e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const LE: ByteOrder = ByteOrder::Le;
    const BE: ByteOrder = ByteOrder::Be;

    fn write_temp(bytes: &[u8]) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(bytes).unwrap();
        file.flush().unwrap();
        file
    }

    fn open(file: &tempfile::NamedTempFile) -> BinaryFile {
        BinaryFile::new(file.path().to_str().unwrap()).unwrap()
    }

    #[test]
    fn open_missing_file_names_the_path() {
        let err = BinaryFile::new("/nonexistent/data.bin").unwrap_err();
        assert!(err.to_string().contains("/nonexistent/data.bin"));
    }

    #[test]
    fn parses_byte_order() {
        assert!(matches!(ByteOrder::parse("le").unwrap(), ByteOrder::Le));
        assert!(matches!(ByteOrder::parse("be").unwrap(), ByteOrder::Be));
        assert!(matches!(ByteOrder::parse("LE").unwrap(), ByteOrder::Le));
        assert!(matches!(ByteOrder::parse("Be").unwrap(), ByteOrder::Be));
        let err = ByteOrder::parse("mixed").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("fs::LE"));
    }

    #[test]
    fn reads_scalars_in_order() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&3_000_000_000u32.to_le_bytes());
        bytes.extend_from_slice(&(-7i32).to_le_bytes());
        bytes.extend_from_slice(&(-1234567890123i64).to_le_bytes());
        bytes.extend_from_slice(&1.5f32.to_le_bytes());
        bytes.extend_from_slice(&(-2.25f64).to_le_bytes());
        let tmp = write_temp(&bytes);
        let mut f = open(&tmp);

        // 3_000_000_000 doesn't fit in i32 - proves the unsigned read widens.
        assert_eq!(f.read_u32(LE).unwrap(), 3_000_000_000);
        assert_eq!(f.read_i32(LE).unwrap(), -7);
        assert_eq!(f.read_i64(LE).unwrap(), -1234567890123);
        assert_eq!(f.read_f32(LE).unwrap(), 1.5);
        assert_eq!(f.read_f64(LE).unwrap(), -2.25);
    }

    #[test]
    fn reads_scalars_big_endian() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&3_000_000_000u32.to_be_bytes());
        bytes.extend_from_slice(&(-7i32).to_be_bytes());
        bytes.extend_from_slice(&(-1234567890123i64).to_be_bytes());
        bytes.extend_from_slice(&1.5f32.to_be_bytes());
        bytes.extend_from_slice(&(-2.25f64).to_be_bytes());
        let tmp = write_temp(&bytes);
        let mut f = open(&tmp);

        assert_eq!(f.read_u32(BE).unwrap(), 3_000_000_000);
        assert_eq!(f.read_i32(BE).unwrap(), -7);
        assert_eq!(f.read_i64(BE).unwrap(), -1234567890123);
        assert_eq!(f.read_f32(BE).unwrap(), 1.5);
        assert_eq!(f.read_f64(BE).unwrap(), -2.25);
    }

    #[test]
    fn bulk_read_past_eof_errors_before_allocating() {
        let tmp = write_temp(&1.0f32.to_le_bytes());
        let mut f = open(&tmp);
        // A corrupt header can request far more data than the file holds; this
        // must fail as a clean EOF error, not attempt a 4 TB allocation.
        let err = f.read_f32_vec(LE, 1_000_000_000_000).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        // The file is still usable at its previous position.
        assert_eq!(f.read_f32(LE).unwrap(), 1.0);
    }

    #[test]
    fn reads_bulk_vectors() {
        let mut bytes = Vec::new();
        for v in [0.5f32, -1.0, 2.5] {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        for v in [10i32, -20, 30] {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        for v in [0.25f64, -3.5] {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        for v in [-40i64, 50] {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        let tmp = write_temp(&bytes);
        let mut f = open(&tmp);

        assert_eq!(f.read_f32_vec(LE, 3).unwrap(), vec![0.5, -1.0, 2.5]);
        assert_eq!(f.read_i32_vec(LE, 3).unwrap(), vec![10, -20, 30]);
        f.seek_relative(-3 * 4).unwrap();
        // The same i32 bytes reread as u32: -20 becomes 2^32 - 20.
        assert_eq!(f.read_u32_vec(LE, 3).unwrap(), vec![10, 4294967276, 30]);
        assert_eq!(f.read_f64_vec(LE, 2).unwrap(), vec![0.25, -3.5]);
        assert_eq!(f.read_i64_vec(LE, 2).unwrap(), vec![-40, 50]);
        assert_eq!(f.read_f32_vec(LE, 0).unwrap(), Vec::<f64>::new());
    }

    #[test]
    fn reads_bulk_vectors_big_endian() {
        let mut bytes = Vec::new();
        for v in [0.5f32, -1.0] {
            bytes.extend_from_slice(&v.to_be_bytes());
        }
        for v in [-40i64, 50] {
            bytes.extend_from_slice(&v.to_be_bytes());
        }
        let tmp = write_temp(&bytes);
        let mut f = open(&tmp);

        assert_eq!(f.read_f32_vec(BE, 2).unwrap(), vec![0.5, -1.0]);
        assert_eq!(f.read_i64_vec(BE, 2).unwrap(), vec![-40, 50]);
    }

    #[test]
    fn seek_and_seek_relative_position_reads() {
        // A fixed-record layout: 8-byte header (count=3, record_len=2), then
        // three records of two f32 each - seek straight to record 1.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes());
        for v in [0.0f32, 0.1, 1.0, 1.1, 2.0, 2.1] {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        let tmp = write_temp(&bytes);
        let mut f = open(&tmp);
        assert_eq!(f.len(), bytes.len() as i64);

        f.seek(8 + 2 * 4).unwrap();
        assert_eq!(f.read_f32(LE).unwrap(), 1.0);
        f.seek_relative(4).unwrap(); // skip the second component of record 1
        assert_eq!(f.read_f32(LE).unwrap(), 2.0);
        f.seek_relative(-2 * 4).unwrap(); // back to that skipped component
        assert_eq!(f.read_f32(LE).unwrap() as f32, 1.1);

        let err = f.seek(-1).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn seeks_outside_the_file_are_rejected() {
        let tmp = write_temp(&[0u8; 8]);
        let mut f = open(&tmp);

        // The end of the file is a valid position; one past it is not.
        f.seek(8).unwrap();
        let err = f.seek(9).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("file length 8"), "got: {err}");

        f.seek(4).unwrap();
        let err = f.seek_relative(-5).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("-1"), "got: {err}");
        let err = f.seek_relative(5).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        // The position is unchanged after a rejected seek.
        assert_eq!(f.read_i32(LE).unwrap(), 0);
    }

    #[test]
    fn byte_order_changes_interpretation() {
        // One byte pattern, two readings: the orders must actually differ.
        let tmp = write_temp(&[0x44, 0x33, 0x22, 0x11]);
        let mut f = open(&tmp);
        assert_eq!(f.read_u32(LE).unwrap(), 0x11223344);
        f.seek(0).unwrap();
        assert_eq!(f.read_u32(BE).unwrap(), 0x44332211);
    }

    #[test]
    fn mixed_orders_within_one_file() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&7u32.to_le_bytes());
        bytes.extend_from_slice(&7u32.to_be_bytes());
        bytes.extend_from_slice(&1.5f32.to_be_bytes());
        bytes.extend_from_slice(&(-9i64).to_le_bytes());
        let tmp = write_temp(&bytes);
        let mut f = open(&tmp);

        assert_eq!(f.read_u32(LE).unwrap(), 7);
        assert_eq!(f.read_u32(BE).unwrap(), 7);
        assert_eq!(f.read_f32(BE).unwrap(), 1.5);
        assert_eq!(f.read_i64(LE).unwrap(), -9);
    }

    #[test]
    fn eof_is_an_error() {
        let tmp = write_temp(&1u32.to_le_bytes());
        let mut f = open(&tmp);
        assert_eq!(f.read_u32(LE).unwrap(), 1);
        let err = f.read_u32(LE).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        let err = f.read_f32_vec(LE, 5).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn reads_raw_bytes() {
        let tmp = write_temp(&[1, 2, 3, 4, 5]);
        let mut f = open(&tmp);
        assert_eq!(f.read_bytes(4).unwrap(), vec![1, 2, 3, 4]);
        assert_eq!(
            f.read_bytes(2).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }

    #[test]
    fn negative_count_is_rejected() {
        let tmp = write_temp(&[]);
        let mut f = open(&tmp);
        let err = f.read_f32_vec(LE, -1).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("read_f32_vec"));
    }
}
