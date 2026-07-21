use crate::error::Result;
use crate::reporting::contract_writer::atomic_write_stream_deferred;
use serde::Serialize;
use std::io::Write;
use std::path::Path;

pub fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    atomic_write_stream_deferred(path, |file| {
        serde_json::to_writer_pretty(&mut *file, value)?;
        file.write_all(b"\n")?;
        Ok(())
    })
}

pub fn write_json_lines<T: Serialize>(
    path: &Path,
    values: impl IntoIterator<Item = T>,
) -> Result<()> {
    atomic_write_stream_deferred(path, |file| {
        for value in values {
            serde_json::to_writer(&mut *file, &value)?;
            file.write_all(b"\n")?;
        }
        Ok(())
    })
}
