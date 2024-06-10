pub use cloudquery_entrypoint::cloudquery_transform;
pub use bytes;
pub use arrow;

use std::mem::MaybeUninit;

#[link(wasm_import_module = "env")]
extern "C" {
    #[link_name = "log"]
    fn _log(ptr: u32, size: u32);

}

#[cfg_attr(all(target_arch = "wasm32"), export_name = "allocate")]
#[no_mangle]
pub extern "C" fn allocate(size: usize) -> *mut u8 {
    let vec: Vec<MaybeUninit<u8>> = Vec::with_capacity(size);
    Box::into_raw(vec.into_boxed_slice()) as *mut u8
}

pub fn log(message: &String) {
    let (ptr, len) = (message.as_ptr() as u32, message.len() as u32);
    unsafe { _log(ptr, len) };
}
