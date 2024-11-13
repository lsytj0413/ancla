trait ByteValue {}

impl ByteValue for u16 {}
impl ByteValue for u32 {}
impl ByteValue for u64 {}

pub(crate) fn read_value<T: ByteValue>(data: &Vec<u8>, offset: usize) -> T {
    let ptr: *const u8 = data.as_ptr();
    unsafe {
        let offset_ptr = ptr.offset(offset as isize) as *const T;
        return offset_ptr.read_unaligned();
    }
}
