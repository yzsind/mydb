//! Copy-on-write metadata with mutation versions. A mutable borrow always
//! invalidates lookup recipes, including edits that keep the same row count.
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AuthCow<T> {
    value: Arc<T>,
    #[serde(skip)]
    revision: u64,
}
impl<T> AuthCow<T> {
    pub fn revision(&self) -> u64 {
        self.revision
    }
}
impl<T: Default> Default for AuthCow<T> {
    fn default() -> Self {
        T::default().into()
    }
}
impl<T> From<T> for AuthCow<T> {
    fn from(value: T) -> Self {
        Self {
            value: Arc::new(value),
            revision: 0,
        }
    }
}
impl<T> Deref for AuthCow<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}
impl<T: Clone> DerefMut for AuthCow<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.revision = self.revision.wrapping_add(1);
        Arc::make_mut(&mut self.value)
    }
}
impl<T, A> FromIterator<A> for AuthCow<T>
where
    T: FromIterator<A>,
{
    fn from_iter<I: IntoIterator<Item = A>>(iter: I) -> Self {
        T::from_iter(iter).into()
    }
}
impl<'a, T> IntoIterator for &'a AuthCow<T>
where
    &'a T: IntoIterator,
{
    type Item = <&'a T as IntoIterator>::Item;
    type IntoIter = <&'a T as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.value.as_ref().into_iter()
    }
}
impl<'a, T: Clone> IntoIterator for &'a mut AuthCow<T>
where
    &'a mut T: IntoIterator,
{
    type Item = <&'a mut T as IntoIterator>::Item;
    type IntoIter = <&'a mut T as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.deref_mut().into_iter()
    }
}
impl<T> AuthCow<T> {
    pub fn weak(&self) -> std::sync::Weak<T> {
        Arc::downgrade(&self.value)
    }
    pub fn same_storage(&self, weak: &std::sync::Weak<T>) -> bool {
        Arc::as_ptr(&self.value) == weak.as_ptr()
    }
}
impl<T: PartialEq> PartialEq for AuthCow<T> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.value, &other.value) || self.value == other.value
    }
}
impl<T: Eq> Eq for AuthCow<T> {}
