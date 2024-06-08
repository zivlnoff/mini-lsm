#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::{binary_heap, BinaryHeap};
use std::iter;
use std::ops::DerefMut;
use std::sync::Arc;
use std::thread::current;

use anyhow::{Ok, Result};
use nom::character::complete::hex_digit0;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut binary_heap: BinaryHeap<HeapWrapper<I>> = BinaryHeap::new();
        for (index, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                binary_heap.push(HeapWrapper(index, iter));
            }
        }

        let current = binary_heap.pop();
        MergeIterator {
            iters: binary_heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        // if other iter has the same key as current, invoke next()
        while let Some(mut iter) = self.iters.peek_mut() {
            if iter.1.key() == self.current.as_ref().unwrap().1.key() {
                match iter.1.next() {
                    Err(err) => {
                        PeekMut::pop(iter);
                        return Err(err);
                    }
                    _ => (),
                }
                if !iter.1.is_valid() {
                    PeekMut::pop(iter);
                }
            } else {
                break;
            }
        }

        // current iter invoke next() and bring back to the heap
        self.current.as_mut().unwrap().1.next()?;
        let current = self.current.take();
        if current.as_ref().unwrap().1.is_valid() {
            self.iters.push(current.unwrap());
        }

        // peek until valid
        while !self.iters.is_empty() && !self.iters.peek().unwrap().1.is_valid() {
            self.iters.pop();
        }

        if !self.iters.is_empty() {
            self.current = self.iters.pop();
        }

        Ok(())
    }
}
