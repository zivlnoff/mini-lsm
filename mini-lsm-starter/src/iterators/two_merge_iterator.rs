#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    read_from: char, // Add fields as need
                     // todo end_bound
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let read_from = if a.is_valid() && b.is_valid() {
            if a.key() <= b.key() {
                'a'
            } else {
                'b'
            }
        } else if a.is_valid() && !b.is_valid() {
            'a'
        } else if !a.is_valid() && b.is_valid() {
            'b'
        } else {
            'n'
        };

        let two_merge_iterator = TwoMergeIterator { a, b, read_from };

        Ok(two_merge_iterator)
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.read_from.eq(&'a') {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.read_from.eq(&'a') {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        !self.read_from.eq(&'n')
    }

    fn next(&mut self) -> Result<()> {
        if self.read_from.eq(&'a') {
            if self.b.is_valid() && self.a.key().eq(&self.b.key()) {
                self.b.next()?;
            };
            self.a.next()?;
        } else if self.read_from.eq(&'b') {
            if self.a.is_valid() && self.b.key().eq(&self.a.key()) {
                self.a.next()?;
            };
            self.b.next()?;
        } else {
            return Ok(());
        }

        if !self.a.is_valid() && !self.b.is_valid() {
            self.read_from = 'n';
            return Ok(());
        }

        if !self.a.is_valid() {
            self.read_from = 'b';
            return Ok(());
        }

        if !self.b.is_valid() {
            self.read_from = 'a';
            return Ok(());
        }

        if self.a.key().le(&self.b.key()) {
            self.read_from = 'a';
        } else {
            self.read_from = 'b';
        }

        Ok(())
    }
}
