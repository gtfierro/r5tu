pub mod header;
pub mod reader;
pub mod writer;

pub use reader::{GraphRef, R5tuFile};
pub use writer::{Quint, StreamingWriter, Term};

pub type Result<T> = std::result::Result<T, crate::reader::R5Error>;
