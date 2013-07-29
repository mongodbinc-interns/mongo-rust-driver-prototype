/* Copyright 2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::hash::Hash;
use std::hashmap::*;
use std::container::Container;
use std::option::Option;
use std::vec::*;

///A hashmap which maintains iteration order using a list.
pub struct OrderedHashmap<K,V> {
    priv map: HashMap<K,V>,
    priv order: ~[(@K,@V)]
}

impl<K: Hash + Eq,V> Container for OrderedHashmap<K,V> {
    pub fn len(&self) -> uint { self.map.len() }
    pub fn is_empty(&self) -> bool { self.map.is_empty() }
}

impl<K: Hash + Eq,V> Mutable for OrderedHashmap<K,V> {
    pub fn clear(&mut self) {
        self.map = HashMap::new();
        self.order = ~[];
    }
}

impl<K:Hash + Eq,V: Eq> Eq for OrderedHashmap<K,V> {
    fn eq(&self, other: &OrderedHashmap<K,V>) -> bool {
        self.map == other.map && self.order == other.order
    }
    fn ne(&self, other: &OrderedHashmap<K,V>) -> bool {
        self.map != other.map || self.order != other.order
    }
}

///Expose most of the Hashmap implementation.
impl<'self, K: Hash + Eq + Copy,V: Copy> OrderedHashmap<K,V> {
    pub fn len(&self) -> uint { self.map.len() }
    pub fn contains_key(&self, k: &K) -> bool { self.map.contains_key(k) }
    pub fn iter(&'self self) -> VecIterator<'self, (@K, @V)> {
        self.order.iter()
    }
    pub fn rev_iter(&'self self) -> VecRevIterator<'self, (@K, @V)> {
        self.order.rev_iter()
    }
    pub fn find<'a>(&'a self, k: &K) -> Option<&'a V> {
        self.map.find(k)
    }
    pub fn find_mut<'a>(&'a mut self, k: &K) -> Option<&'a mut V> {
        self.map.find_mut(k)
    }
    pub fn insert(&mut self, k: K, v: V) -> bool {
        let success = self.map.insert(copy k, copy v);
        if success { self.order.push((@k, @v)) }
        success
    }

    pub fn new() -> OrderedHashmap<K,V> {
        OrderedHashmap { map: HashMap::new(), order: ~[] }
    }
}

impl<K:Hash + Eq + ToStr + Copy,V:ToStr + Copy> ToStr for OrderedHashmap<K,V> {
    pub fn to_str(&self) -> ~str {
        let mut s = ~"{";
        for self.iter().advance |&(@k, @v)| {
            s.push_str(fmt!(" %s: %s, ", k.to_str(), v.to_str()));
        }
        s = s.slice(0, s.len()-2).to_owned();
        s.push_str("}");
        s
    }
}
