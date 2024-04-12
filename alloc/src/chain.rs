// Copyright 2024-Present Datadog, Inc. https://www.datadoghq.com/
// SPDX-License-Identifier: Apache-2.0

use crate::LinearAllocator;
use allocator_api2::alloc::{AllocError, Allocator};
use core::alloc::Layout;
use core::cell::UnsafeCell;
use core::mem::size_of;
use core::ptr::NonNull;

pub struct ChainAllocator<A: Allocator + Clone> {
    top: UnsafeCell<ChainNodePtr<A>>,
    /// The size hint for the linear allocator's chunk.
    node_size: usize,
    allocator: A,
}

#[derive(Clone, Copy)]
struct ChainNodePtr<A: Allocator> {
    ptr: Option<NonNull<ChainNode<A>>>,
}

impl<A: Allocator> ChainNodePtr<A> {
    const fn new() -> Self {
        Self { ptr: None }
    }

    fn as_ref(&self) -> Option<&ChainNode<A>> {
        // SAFETY: active as long as not-null, never give out mut refs.
        self.ptr.map(|p| unsafe { p.as_ref() })
    }
}

/// The node exists inside the allocation owned by `linear`.
struct ChainNode<A: Allocator> {
    prev: UnsafeCell<ChainNodePtr<A>>,
    linear: LinearAllocator<A>,
}

impl<A: Allocator> ChainNode<A> {
    fn remaining_capacity(&self) -> usize {
        self.linear.remaining_capacity()
    }
}

impl<A: Allocator + Clone> ChainAllocator<A> {
    /// The individual nodes need to be big enough that the overhead of a chain
    /// is worth it. This is somewhat arbitrarily chosen at the moment.
    const MIN_NODE_SIZE: usize = 4 * size_of::<Self>();

    pub const fn new_in(chunk_size_hint: usize, allocator: A) -> Self {
        Self {
            top: UnsafeCell::new(ChainNodePtr::new()),
            // max is not a const fn, do it manually.
            node_size: if chunk_size_hint < Self::MIN_NODE_SIZE {
                Self::MIN_NODE_SIZE
            } else {
                chunk_size_hint
            },
            allocator,
        }
    }

    #[cold]
    #[inline(never)]
    fn grow(&self) -> Result<(), AllocError> {
        let top = self.top.get();
        let chain_layout = Layout::new::<ChainNode<A>>();

        let linear = {
            let layout = Layout::from_size_align(self.node_size, chain_layout.align())
                .map_err(|_| AllocError)?;
            LinearAllocator::new_in(layout, self.allocator.clone())?
        };

        // This shouldn't fail.
        let chain_node_addr = linear
            .allocate(chain_layout)?
            .as_ptr()
            .cast::<ChainNode<A>>();
        let chain_node = ChainNode {
            prev: UnsafeCell::new(ChainNodePtr {
                // SAFETY: todo
                ptr: unsafe { (*top).ptr },
            }),
            linear,
        };

        // SAFETY: todo
        unsafe { chain_node_addr.write(chain_node) };

        let chain_node_ptr = ChainNodePtr {
            // SAFETY: derived from allocation (not null).
            ptr: Some(unsafe { NonNull::new_unchecked(chain_node_addr) }),
        };
        // SAFETY: todo
        unsafe { self.top.get().write(chain_node_ptr) };

        Ok(())
    }

    fn remaining_capacity(&self) -> usize {
        let chain_ptr = self.top.get();
        // SAFETY: todo
        match unsafe { (*chain_ptr).as_ref() } {
            None => 0,
            Some(chain_node) => chain_node.remaining_capacity(),
        }
    }
}

unsafe impl<A: Allocator + Clone> Allocator for ChainAllocator<A> {
    #[cfg_attr(debug_assertions, track_caller)]
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let layout = layout.pad_to_align();

        // Too large for ChainAllocator to deal with.
        let header_overhead = size_of::<ChainNode<A>>();
        let maximum_capacity = self.node_size - header_overhead;
        if layout.size() > maximum_capacity {
            return Err(AllocError);
        }

        let remaining_capacity = self.remaining_capacity();
        if layout.size() > remaining_capacity {
            self.grow()?;
        }

        // At this point:
        //  1. There's a top node.
        //  2. It has enough capacity for the allocation.

        let top = self.top.get();
        let chain_node = unsafe { (*top).as_ref().unwrap_unchecked() };

        debug_assert!(chain_node.remaining_capacity() >= layout.size());

        let result = chain_node.linear.allocate(layout);
        // If this fails, there's a bug in the allocator.
        debug_assert!(result.is_ok());
        result
    }

    unsafe fn deallocate(&self, _ptr: NonNull<u8>, _layout: Layout) {}
}

impl<A: Allocator + Clone> Drop for ChainAllocator<A> {
    fn drop(&mut self) {
        let mut chain_node_ptr = unsafe { self.top.get().read() };

        loop {
            match chain_node_ptr.ptr {
                None => break,
                Some(nonnull) => {
                    // SAFETY: todo
                    chain_node_ptr = unsafe {
                        core::ptr::addr_of!((*nonnull.as_ptr()).prev)
                            .read()
                            .get()
                            .read()
                    };

                    // SAFETY: todo
                    let alloc =
                        unsafe { core::ptr::addr_of_mut!((*nonnull.as_ptr()).linear).read() };
                    drop(alloc);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use allocator_api2::alloc::Global;

    #[test]
    fn test_basics() {
        let allocator = ChainAllocator::new_in(4096, Global);
        let layout = Layout::new::<[u8; 8]>();
        let ptr = allocator.allocate(layout).unwrap();

        // deallocate doesn't return memory to the allocator, but it shouldn't
        // panic, as that prevents its use in containers like Vec.
        unsafe { allocator.deallocate(ptr.cast(), layout) };
    }

    #[track_caller]
    fn fill_to_capacity<A: Allocator + Clone>(allocator: &ChainAllocator<A>) {
        let remaining_capacity = allocator.remaining_capacity();
        if remaining_capacity != 0 {
            let layout = Layout::from_size_align(remaining_capacity, 1).unwrap();
            let ptr = allocator.allocate(layout).unwrap();
            // Doesn't return memory, just ensuring we don't panic.
            unsafe { allocator.deallocate(ptr.cast(), layout) };
        }
        let remaining_capacity = allocator.remaining_capacity();
        assert_eq!(0, remaining_capacity);
    }

    #[test]
    fn test_growth() {
        let page_size = crate::os::page_size().unwrap();
        let allocator = ChainAllocator::new_in(page_size, Global);

        let bool_layout = Layout::new::<bool>();

        // test that it fills to capacity a few times.
        for _ in 0..100 {
            fill_to_capacity(&allocator);

            // Trigger it to grow.
            let ptr = allocator.allocate(bool_layout).unwrap();

            // Doesn't free, shouldn't panic though.
            unsafe { allocator.deallocate(ptr.cast(), bool_layout) };
        }
    }
}
