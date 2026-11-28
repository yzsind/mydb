// CPython objects never cross a Rust thread boundary. The entry points are
// called through PyDLL, which holds the interpreter lock throughout the call.
use serde_json::Value;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::io;
use std::ptr::{null, null_mut, NonNull};
use std::rc::Rc;

pub(super) type Result<T> = io::Result<T>;
pub(super) type Py = c_void;

extern "C" {
    fn Py_IncRef(object: *mut Py);
    fn Py_DecRef(object: *mut Py);
    fn PyLong_FromLongLong(value: i64) -> *mut Py;
    fn PyLong_AsLongLong(value: *mut Py) -> i64;
    fn PyFloat_FromDouble(value: f64) -> *mut Py;
    fn PyBool_FromLong(value: std::ffi::c_long) -> *mut Py;
    fn PyUnicode_FromStringAndSize(value: *const c_char, size: isize) -> *mut Py;
    fn PyDict_New() -> *mut Py;
    fn PyDict_GetItemWithError(dict: *mut Py, key: *mut Py) -> *mut Py;
    fn PyDict_SetItem(dict: *mut Py, key: *mut Py, value: *mut Py) -> c_int;
    fn PyDict_Next(dict: *mut Py, pos: *mut isize, key: *mut *mut Py, value: *mut *mut Py)
        -> c_int;
    fn PyList_New(len: isize) -> *mut Py;
    fn PyList_Append(list: *mut Py, item: *mut Py) -> c_int;
    fn PyList_Sort(list: *mut Py) -> c_int;
    fn PyTuple_New(len: isize) -> *mut Py;
    fn PyTuple_SetItem(tuple: *mut Py, index: isize, item: *mut Py) -> c_int;
    fn PyTuple_GetItem(tuple: *mut Py, index: isize) -> *mut Py;
    fn PySet_New(iterable: *mut Py) -> *mut Py;
    fn PySet_Contains(set: *mut Py, item: *mut Py) -> c_int;
    fn PySet_Add(set: *mut Py, item: *mut Py) -> c_int;
    fn PyObject_GetAttrString(object: *mut Py, name: *const c_char) -> *mut Py;
    fn PyObject_SetAttrString(object: *mut Py, name: *const c_char, value: *mut Py) -> c_int;
    fn PyObject_Vectorcall(
        callable: *mut Py,
        args: *const *mut Py,
        count: usize,
        names: *const Py,
    ) -> *mut Py;
    fn PyErr_Occurred() -> *mut Py;
    fn PyErr_SetString(kind: *mut Py, message: *const c_char);
    fn PyEval_SaveThread() -> *mut c_void;
    fn PyEval_RestoreThread(thread: *mut c_void);
    static mut PyExc_RuntimeError: *mut Py;
}

// Only run Rust work here. The saved thread must be restored before accessing
// any Python object or reporting an error, including on unwind.
pub(super) fn allow_threads<T>(work: impl FnOnce() -> T) -> T {
    struct Restore(*mut c_void);
    impl Drop for Restore {
        fn drop(&mut self) {
            unsafe { PyEval_RestoreThread(self.0) }
        }
    }
    let _restore = Restore(unsafe { PyEval_SaveThread() });
    work()
}

fn failure() -> io::Error {
    io::Error::other("CPython index construction failed")
}

fn check(value: c_int) -> Result<()> {
    if value == 0 {
        Ok(())
    } else {
        Err(failure())
    }
}

pub(super) fn report(message: String) {
    unsafe {
        if PyErr_Occurred().is_null() {
            let message = CString::new(message.replace('\0', "\\0")).unwrap();
            PyErr_SetString(PyExc_RuntimeError, message.as_ptr());
        }
    }
}

#[repr(transparent)]
pub(super) struct Object(NonNull<Py>, std::marker::PhantomData<Rc<()>>);

impl Object {
    fn new(pointer: *mut Py) -> Result<Self> {
        NonNull::new(pointer)
            .map(|p| Self(p, std::marker::PhantomData))
            .ok_or_else(failure)
    }

    pub(super) unsafe fn borrow(pointer: *mut Py) -> Result<Self> {
        let value = Self::new(pointer)?;
        Py_IncRef(pointer);
        Ok(value)
    }

    fn as_ptr(&self) -> *mut Py {
        self.0.as_ptr()
    }

    pub(super) fn into_ptr(self) -> *mut Py {
        let pointer = self.as_ptr();
        std::mem::forget(self);
        pointer
    }

    pub(super) fn int(value: i64) -> Result<Self> {
        Self::new(unsafe { PyLong_FromLongLong(value) })
    }

    pub(super) fn str(value: &str) -> Result<Self> {
        Self::new(unsafe {
            PyUnicode_FromStringAndSize(value.as_ptr().cast(), value.len() as isize)
        })
    }

    pub(super) fn bool(value: bool) -> Result<Self> {
        Self::new(unsafe { PyBool_FromLong(value.into()) })
    }

    pub(super) fn dict() -> Result<Self> {
        Self::new(unsafe { PyDict_New() })
    }

    pub(super) fn list() -> Result<Self> {
        Self::new(unsafe { PyList_New(0) })
    }

    pub(super) fn set() -> Result<Self> {
        Self::new(unsafe { PySet_New(null_mut()) })
    }

    pub(super) fn tuple<const N: usize>(items: [&Self; N]) -> Result<Self> {
        let tuple = Self::new(unsafe { PyTuple_New(N as isize) })?;
        for (index, value) in items.into_iter().enumerate() {
            unsafe { Py_IncRef(value.as_ptr()) };
            check(unsafe { PyTuple_SetItem(tuple.as_ptr(), index as isize, value.as_ptr()) })?;
        }
        Ok(tuple)
    }

    pub(super) fn call<const N: usize>(&self, args: [&Self; N]) -> Result<Self> {
        let args = args.map(Self::as_ptr);
        Self::new(unsafe { PyObject_Vectorcall(self.as_ptr(), args.as_ptr(), N, null()) })
    }

    pub(super) fn attr(&self, name: &CStr) -> Result<Self> {
        Self::new(unsafe { PyObject_GetAttrString(self.as_ptr(), name.as_ptr()) })
    }

    pub(super) fn set_attr(&self, name: &CStr, value: &Self) -> Result<()> {
        check(unsafe { PyObject_SetAttrString(self.as_ptr(), name.as_ptr(), value.as_ptr()) })
    }

    pub(super) fn get(&self, key: &Self) -> Result<Option<Self>> {
        let ptr = unsafe { PyDict_GetItemWithError(self.as_ptr(), key.as_ptr()) };
        if ptr.is_null() {
            if unsafe { PyErr_Occurred() }.is_null() {
                Ok(None)
            } else {
                Err(failure())
            }
        } else {
            unsafe { Self::borrow(ptr).map(Some) }
        }
    }

    pub(super) fn put(&self, key: &Self, value: &Self) -> Result<()> {
        check(unsafe { PyDict_SetItem(self.as_ptr(), key.as_ptr(), value.as_ptr()) })
    }

    pub(super) fn group(&self, key: &Self, make: fn() -> Result<Self>) -> Result<Self> {
        if let Some(group) = self.get(key)? {
            return Ok(group);
        }
        let group = make()?;
        self.put(key, &group)?;
        Ok(group)
    }

    pub(super) fn append(&self, value: &Self) -> Result<()> {
        check(unsafe { PyList_Append(self.as_ptr(), value.as_ptr()) })
    }

    pub(super) fn sort(&self) -> Result<()> {
        check(unsafe { PyList_Sort(self.as_ptr()) })
    }

    pub(super) fn add(&self, item: &Self) -> Result<()> {
        check(unsafe { PySet_Add(self.as_ptr(), item.as_ptr()) })
    }

    pub(super) fn contains(&self, item: &Self) -> Result<bool> {
        match unsafe { PySet_Contains(self.as_ptr(), item.as_ptr()) } {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(failure()),
        }
    }

    pub(super) fn as_int(&self) -> Result<i64> {
        let value = unsafe { PyLong_AsLongLong(self.as_ptr()) };
        if value == -1 && !unsafe { PyErr_Occurred() }.is_null() {
            return Err(failure());
        }
        Ok(value)
    }

    pub(super) fn int_item(&self, index: isize) -> Result<i64> {
        unsafe { Self::borrow(PyTuple_GetItem(self.as_ptr(), index))?.as_int() }
    }

    pub(super) fn each(&self, mut visit: impl FnMut(&Self, &Self) -> Result<()>) -> Result<()> {
        let mut position = 0;
        let mut key = null_mut();
        let mut value = null_mut();
        while unsafe { PyDict_Next(self.as_ptr(), &mut position, &mut key, &mut value) } != 0 {
            unsafe { visit(&Self::borrow(key)?, &Self::borrow(value)?)? };
        }
        Ok(())
    }

    pub(super) fn json(value: &Value, none: &Self) -> Result<Self> {
        match value {
            Value::Null => unsafe { Self::borrow(none.as_ptr()) },
            Value::Bool(value) => Self::bool(*value),
            Value::Number(value) => {
                if let Some(signed) = value.as_i64() {
                    Self::int(signed)
                } else if let Some(unsigned) = value.as_u64() {
                    extern "C" {
                        fn PyLong_FromUnsignedLongLong(value: u64) -> *mut Py;
                    }
                    Self::new(unsafe { PyLong_FromUnsignedLongLong(unsigned) })
                } else {
                    Self::new(unsafe { PyFloat_FromDouble(value.as_f64().expect("JSON number")) })
                }
            }
            Value::String(value) => Self::str(value),
            Value::Array(values) => {
                let list = Self::list()?;
                for value in values {
                    list.append(&Self::json(value, none)?)?;
                }
                Ok(list)
            }
            Value::Object(values) => {
                let dict = Self::dict()?;
                for (key, value) in values {
                    dict.put(&Self::str(key)?, &Self::json(value, none)?)?;
                }
                Ok(dict)
            }
        }
    }
}

impl Drop for Object {
    fn drop(&mut self) {
        unsafe { Py_DecRef(self.as_ptr()) }
    }
}
