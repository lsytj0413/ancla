trait ByteReadMarker {}

impl ByteReadMarker for u16 {}
impl ByteReadMarker for u32 {}
impl ByteReadMarker for u64 {}

#[allow(private_bounds)]
pub(crate) fn read_value<T: ByteReadMarker>(data: &[u8], offset: usize) -> T {
    let ptr: *const u8 = data.as_ptr();
    unsafe {
        let offset_ptr = ptr.add(offset) as *const T;
        offset_ptr.read_unaligned()
    }
}
