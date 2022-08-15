use std::{fmt::Debug, sync::atomic::{AtomicU64, Ordering}};

use lazy_static::lazy_static;

use crate::{Transport, ui};

pub struct DebugTransport {
    path: Vec<String>,
    path_text: String,
    inner: Box<dyn Transport>
}

impl DebugTransport {
    pub fn new(inner: Box<dyn Transport>) -> Self {
        Self { inner, path: vec![], path_text: "".into() }
    }
}

const DIR_ITER_ID: AtomicU64 = AtomicU64::new(1);

impl Transport for DebugTransport {
    fn iter_dir_entries(
        &self,
        path: &str,
    ) -> std::io::Result<Box<dyn Iterator<Item = std::io::Result<super::DirEntry>>>> {
        match self.inner.iter_dir_entries(path) {
            Ok(iter) => {
                let id = DIR_ITER_ID.fetch_add(1, Ordering::AcqRel);
                ui::println(&format!("iter_dir_entries: {}/{} -> #{}", self.path_text, path, id));
                Ok(
                    Box::new(iter.inspect(move |entry| {
                        match entry {
                            Ok(entry) => {
                                ui::println(&format!(" #{}: {:?}", id, entry));
                            },
                            Err(error) => {
                                ui::println(&format!(" #{}: {:?}", id, error));
                            }
                        }
                    }))
                )
            },
            Err(error) => {
                ui::println(&format!("iter_dir_entries: {}/{} ({})", self.path_text, path, error));
                Err(error)
            }
        }
    }

    fn read_file(&self, path: &str) -> std::io::Result<bytes::Bytes> {
        ui::println(&format!("read_file: {}/{}", self.path_text, path));
        self.inner.read_file(path)
    }

    fn create_dir(&self, relpath: &str) -> std::io::Result<()> {
        ui::println(&format!("create_dir: {}/{}", self.path_text, relpath));
        self.inner.create_dir(relpath)
    }

    fn write_file(&self, relpath: &str, content: &[u8]) -> std::io::Result<()> {
        ui::println(&format!("write_file: {}/{} ({} bytes)", self.path_text, relpath, content.len()));
        self.inner.write_file(relpath, content)
    }

    fn metadata(&self, relpath: &str) -> std::io::Result<super::Metadata> {
        ui::println(&format!("metadata: {}/{}", self.path_text, relpath));
        self.inner.metadata(relpath)
    }

    fn remove_file(&self, relpath: &str) -> std::io::Result<()> {
        ui::println(&format!("remove_file: {}/{}", self.path_text, relpath));
        self.inner.remove_file(relpath)
    }

    fn remove_dir(&self, relpath: &str) -> std::io::Result<()> {
        ui::println(&format!("remove_dir: {}/{}", self.path_text, relpath));
        self.inner.remove_dir(relpath)
    }

    fn remove_dir_all(&self, relpath: &str) -> std::io::Result<()> {
        ui::println(&format!("remove_dir_all: {}/{}", self.path_text, relpath));
        self.inner.remove_dir_all(relpath)
    }

    fn sub_transport(&self, relpath: &str) -> Box<dyn Transport> {
        ui::println(&format!("sub_transport: {}/{}", self.path_text, relpath));

        let mut path = self.path.clone();
        path.push(relpath.to_string());

        Box::new(DebugTransport{ 
            inner: self.inner.sub_transport(relpath),
            path_text: path.join("/"),
            path,
        })
    }

    fn url_scheme(&self) -> &'static str {
        self.inner.url_scheme()
    }
}

impl Debug for DebugTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DebugTransport").field("inner", &self.inner).finish()
    }
}