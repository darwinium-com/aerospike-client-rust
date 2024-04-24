// Copyright 2015-2020 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

//! Traits and Implementations for reading data into structs and variables

use serde::de::EnumAccess;
use serde::de::Error as _;
use serde::de::MapAccess;
use serde::de::SeqAccess;
use serde::de::VariantAccess;
use serde::de::Visitor;
use serde::Deserialize;
use serde::Deserializer;

use crate::errors::Result;
use crate::Error;
use crate::ParticleType;
use std::collections::VecDeque;
use std::convert::{TryInto, TryFrom};

// This serializer represents all the bins in a record.
pub(crate) struct BinsDeserializer {
    pub bins: VecDeque<PreParsedValue>,
}

impl serde::de::Error for crate::errors::Error {
    fn custom<T>(msg:T) -> Self where T:std::fmt::Display {
        crate::errors::Error::from_kind(crate::ErrorKind::Derive(msg.to_string()))
    }
}

impl<'de> serde::de::Deserializer<'de> for BinsDeserializer {
    type Error = crate::errors::Error;

    fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_bool<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_i8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_i16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_i32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_i64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_u8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_u16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_u32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_u64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_f32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_f64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_char<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_str<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_string<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_option<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_some(self)
    }

    fn deserialize_unit<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        if self.bins.len() != len {
            struct ExpectedLen(usize);
            let len = ExpectedLen(len);
            impl serde::de::Expected for ExpectedLen {
                fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                    write!(formatter, "{}", self.0)
                }
            }
            Err(<Self::Error as serde::de::Error>::invalid_length(self.bins.len(), &len))
        } else {
            visitor.visit_map(self)
        }
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_map<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_struct<V>(
        self,
        __name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_map(self)
    }
}


struct DeserializeStr<'a>(&'a str);
impl<'a, 'de> serde::de::Deserializer<'de> for DeserializeStr<'a> {
    type Error = crate::errors::Error;

    fn deserialize_any<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_bool<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_i8<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_i16<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_i32<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_i64<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_u8<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_u16<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_u32<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_u64<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_f32<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_f64<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_char<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_str<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_string<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_option<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_unit<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_seq<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_map<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_str(self.0)
    }
}

impl<'de> serde::de::MapAccess<'de> for BinsDeserializer {
    type Error = crate::errors::Error;

    fn next_key_seed<K>(&mut self, seed: K) -> std::result::Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'de> {

        if let Some(next_key) = self.bins.front() {
            Some(seed.deserialize(DeserializeStr(next_key.name()?))).transpose()
        } else {
            Ok(None)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'de> {
        seed.deserialize(self.bins.pop_front().unwrap())
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.bins.len())
    }
}

impl<'de> serde::de::Deserializer<'de> for PreParsedValue {
    type Error = crate::errors::Error;

    fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        match self.particle_type() {
            ParticleType::NULL => {
                visitor.visit_none()
            }
            ParticleType::INTEGER => {
                visitor.visit_i64(self.as_int()?)
            }
            ParticleType::FLOAT => {
                visitor.visit_f64(self.as_float()?)
            }
            ParticleType::STRING | ParticleType::GEOJSON => {
                visitor.visit_string(self.into_string()?)
            }
            ParticleType::BLOB | ParticleType::HLL => {
                visitor.visit_byte_buf(self.into_blob())
            }
            ParticleType::BOOL => {
                visitor.visit_bool(self.as_bool()?)
            }
            ParticleType::MAP | ParticleType::LIST => {
                let mut read = 0;
                let cdt_reader = CDTDecoder(self.particle(), &mut read);
                cdt_reader.deserialize_any(visitor)
            }
            ParticleType::DIGEST => todo!(),
            ParticleType::LDT => todo!(),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        self.deserialize_any(visitor)
    }

    fn deserialize_i8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        if self.particle_type() == ParticleType::INTEGER {
            let integer = self.as_int()?;
            if let Ok(as_int) = integer.try_into() {
                return visitor.visit_i8(as_int);
            }
        }
        self.deserialize_any(visitor)
    }

    fn deserialize_i16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        if self.particle_type() == ParticleType::INTEGER {
            let integer = self.as_int()?;
            if let Ok(as_int) = integer.try_into() {
                return visitor.visit_i16(as_int);
            }
        }
        self.deserialize_any(visitor)
    }

    fn deserialize_i32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        if self.particle_type() == ParticleType::INTEGER {
            let integer = self.as_int()?;
            return visitor.visit_i32(integer.try_into()?);
        } else {
            self.deserialize_any(visitor)
        }
    }

    fn deserialize_i64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        if self.particle_type() == ParticleType::INTEGER {
            let integer = self.as_int()?;
            return visitor.visit_i64(integer);
        } else {
            self.deserialize_any(visitor)
        }
    }

    fn deserialize_u8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        if self.particle_type() == ParticleType::INTEGER {
            let integer = self.as_int()?;
            visitor.visit_u8(integer.try_into()?)
        } else {
            self.deserialize_any(visitor)
        }
    }

    fn deserialize_u16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        if self.particle_type() == ParticleType::INTEGER {
            let integer = self.as_int()?;
            if let Ok(as_int) = integer.try_into() {
                return visitor.visit_u16(as_int);
            }
        }
        self.deserialize_any(visitor)
    }

    fn deserialize_u32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        if self.particle_type() == ParticleType::INTEGER {
            let integer = self.as_int()?;
            visitor.visit_u32(integer.try_into()?)
        } else {
            self.deserialize_any(visitor)
        }
    }

    fn deserialize_u64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        if self.particle_type() == ParticleType::INTEGER {
            let integer = self.as_int()?;
            return visitor.visit_u64(integer.try_into()?);
        } else {
            self.deserialize_any(visitor)
        }
    }

    fn deserialize_f32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        if self.particle_type() == ParticleType::FLOAT {
            let flt = self.as_float()?;
            visitor.visit_f32(flt as f32)
        } else {
            self.deserialize_any(visitor)
        }
    }

    fn deserialize_f64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        if self.particle_type() == ParticleType::FLOAT {
            let flt = self.as_float()?;
            visitor.visit_f64(flt)
        } else {
            self.deserialize_any(visitor)
        }
    }

    fn deserialize_char<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        self.deserialize_any(visitor)
    }

    fn deserialize_str<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        self.deserialize_any(visitor)
    }

    fn deserialize_string<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        self.deserialize_any(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        match self.particle_type() {
            ParticleType::NULL => {
                visitor.visit_none()
            }
            _ => visitor.visit_bytes(self.particle())
        }
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        match self.particle_type() {
            ParticleType::NULL => {
                visitor.visit_none()
            }
            _ => visitor.visit_byte_buf(self.into_blob())
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        if self.particle_type() != ParticleType::NULL {
            visitor.visit_some(self)
        } else {
            visitor.visit_none()
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        self.deserialize_any(visitor)
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        self.deserialize_any(visitor)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        self.deserialize_any(visitor)
    }

    fn deserialize_seq<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        self.deserialize_any(visitor)
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        self.deserialize_any(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        self.deserialize_any(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        self.deserialize_any(visitor)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        self.deserialize_any(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_enum(EnumAdaptor{ particle_type: self.particle_type(), deserializer: self})
    }

    fn deserialize_identifier<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        self.deserialize_any(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        visitor.visit_none()
    }
}

struct EnumAdaptor<V: for<'a> Deserializer<'a, Error = crate::Error>> {
    particle_type: ParticleType,
    deserializer: V,
}

// This is specially designed for Value type, to retain current performance.
// There is a possibility that we can do this directly using a u8 enum tag.
impl<'de, Var: for<'a> Deserializer<'a, Error = crate::Error>> EnumAccess<'de> for EnumAdaptor<Var> {
    type Error = crate::Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> std::prelude::v1::Result<(V::Value, Self::Variant), Self::Error>
    where
        V: serde::de::DeserializeSeed<'de> {
        let name = match self.particle_type {
            ParticleType::NULL => "Nil",
            ParticleType::INTEGER => "Int",
            ParticleType::FLOAT => "Float",
            ParticleType::STRING => "String",
            ParticleType::BLOB => "Blob",
            ParticleType::DIGEST => todo!(),
            ParticleType::BOOL => "Bool",
            ParticleType::HLL => "HLL",
            ParticleType::MAP => "HashMap",
            ParticleType::LIST => "List",
            ParticleType::LDT => todo!(),
            ParticleType::GEOJSON => "GeoJSON",
        };
        let val = seed.deserialize(DeserializeStr(name))?;
        Ok((val, self))
    }
}

impl<'de, Var: for<'a> Deserializer<'a, Error = crate::Error>> VariantAccess<'de> for EnumAdaptor<Var> {
    type Error = crate::errors::Error;

    fn unit_variant(self) -> std::prelude::v1::Result<(), Self::Error> {
        if self.particle_type == ParticleType::NULL {
            Ok(())
        } else {
            Err(serde::de::Error::invalid_type(serde::de::Unexpected::NewtypeVariant, &"unit variant"))
        }
    }

    fn newtype_variant_seed<T>(self, seed: T) -> std::prelude::v1::Result<T::Value, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de> {
        seed.deserialize(self.deserializer)
    }

    fn tuple_variant<V>(self, _len: usize, _visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        Err(serde::de::Error::invalid_type(serde::de::Unexpected::NewtypeVariant, &"tuple variant"))
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        Err(serde::de::Error::invalid_type(serde::de::Unexpected::NewtypeVariant, &"struct variant"))
    }
} 

struct CDTDecoder<'m>(&'m [u8], &'m mut usize);

struct CDTListOrMap<'m>(usize, &'m [u8], &'m mut usize);

impl<'m> CDTDecoder<'m> {
    fn as_unexpected(mut self, ptype: u8) -> std::result::Result<serde::de::Unexpected<'m>, Error> {
        Ok(match ptype {
            0x00..=0x7f => serde::de::Unexpected::Unsigned(ptype as u64),
            0x80..=0x8f => serde::de::Unexpected::Map,
            0x90..=0x9f => serde::de::Unexpected::Seq,
            0xa0..=0xbf => serde::de::Unexpected::Bytes(self.take_nbyte((ptype & 0x1f) as usize)?),
            0xc0 => serde::de::Unexpected::Unit,
            0xc1 => serde::de::Unexpected::Unit, // Don't actually support this type
            0xc2 => serde::de::Unexpected::Bool(false),
            0xc3 => serde::de::Unexpected::Bool(true),
            0xc4 | 0xd9 => {
                let count = u8::from_be_bytes(self.take_bytes()?) as usize;
                serde::de::Unexpected::Bytes(self.take_nbyte(count)?)
            }
            0xc5 | 0xda => {
                let count = u16::from_be_bytes(self.take_bytes()?) as usize;
                serde::de::Unexpected::Bytes(self.take_nbyte(count)?)
            }
            0xc6 | 0xdb => {
                let count = u32::from_be_bytes(self.take_bytes()?) as usize;
                serde::de::Unexpected::Bytes(self.take_nbyte(count)?)
            }
            0xc7 | 0xc8 | 0xc9 => serde::de::Unexpected::Unit, // Don't actually support this type
            0xca => serde::de::Unexpected::Float(f32::from_be_bytes(self.take_bytes()?) as f64),
            0xcb => serde::de::Unexpected::Float(f64::from_be_bytes(self.take_bytes()?)),
            0xcc => serde::de::Unexpected::Unsigned(u8::from_be_bytes(self.take_bytes()?) as u64),
            0xcd => serde::de::Unexpected::Unsigned(u16::from_be_bytes(self.take_bytes()?) as u64),
            0xce => serde::de::Unexpected::Unsigned(u32::from_be_bytes(self.take_bytes()?) as u64),
            0xcf => serde::de::Unexpected::Unsigned(u64::from_be_bytes(self.take_bytes()?) as u64),
            0xd0 => serde::de::Unexpected::Signed(i8::from_be_bytes(self.take_bytes()?) as i64),
            0xd1 => serde::de::Unexpected::Signed(i16::from_be_bytes(self.take_bytes()?) as i64),
            0xd2 => serde::de::Unexpected::Signed(i32::from_be_bytes(self.take_bytes()?) as i64),
            0xd3 => serde::de::Unexpected::Signed(i64::from_be_bytes(self.take_bytes()?) as i64),
            0xd4..=0xd8 => serde::de::Unexpected::Unit, // Don't actually support this type
            0xdc => serde::de::Unexpected::Seq,
            0xdd => serde::de::Unexpected::Seq,
            0xde => serde::de::Unexpected::Map,
            0xdf => serde::de::Unexpected::Map,
            0xe0..=0xff => {
                let value = i64::from(ptype) - 0xe0 - 32;
                serde::de::Unexpected::Signed(value)
            }
        })
    }

    fn take_byte(&mut self) -> std::result::Result<u8, Error> {
        if *self.1 >= self.0.len() {
            Err(Error::from_kind(crate::errors::ErrorKind::Derive("Ran out of data".to_string())))
        } else {
            let out = self.0[*self.1];
            *self.1 += 1;
            Ok(out)
        }
    }

    fn take_bytes<const N: usize>(&mut self) -> std::result::Result<[u8; N], Error> {
        if *self.1 + N > self.0.len() {
            Err(Error::from_kind(crate::errors::ErrorKind::Derive("Ran out of data".to_string())))
        } else {
            let offset = *self.1 as isize;
            *self.1 += N;
            // SAFETY: ok because we just checked that the length fits
            unsafe {
                let ptr = self.0.as_ptr().offset(offset) as *const [u8; N];
                Ok(*ptr)
            }   
         }
    }


    fn take_nbyte(&mut self, mid: usize) -> std::result::Result<&'m [u8], Error> {
        if *self.1 + mid > self.0.len() {
            Err(Error::from_kind(crate::errors::ErrorKind::Derive("Ran out of data".to_string())))
        } else {
            let first = &self.0[*self.1..(*self.1 + mid)];
            *self.1 += mid;
            Ok(first)
        }
    }
}

impl<'l, 'm> Deserializer<'l> for CDTDecoder<'m> {
    type Error = crate::errors::Error;

    fn deserialize_any<V>(mut self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        fn deserialize_any_buffer<'l, 'm, V>(mut deserializer: CDTDecoder<'m>, visitor: V, count: usize) -> std::prelude::v1::Result<V::Value, crate::errors::Error>
        where
            V: serde::de::Visitor<'l> {
            let ptype = ParticleType::from(deserializer.take_byte()?);
            let body = deserializer.take_nbyte(count - 1)?;
            if matches!(ptype, ParticleType::STRING | ParticleType::GEOJSON) {
                visitor.visit_str(std::str::from_utf8(body)?)
            } else {
                visitor.visit_bytes(body)
            }
        }

        let ptype = self.take_byte()?;
        match ptype {
            0x00..=0x7f => visitor.visit_u8(ptype as u8),
            0x80..=0x8f => visitor.visit_map(CDTListOrMap((ptype & 0x0f) as usize, self.0, self.1)),
            0x90..=0x9f => visitor.visit_seq(CDTListOrMap((ptype & 0x0f) as usize, self.0, self.1)),
            0xa0..=0xbf => deserialize_any_buffer(self, visitor, (ptype & 0x1f) as usize),
            0xc0 => visitor.visit_none(),
            0xc2 => visitor.visit_bool(false),
            0xc3 => visitor.visit_bool(true),
            0xc4 | 0xd9 => {
                let count = u8::from_be_bytes(self.take_bytes()?);
                deserialize_any_buffer(self, visitor, count as usize)
            }
            0xc5 | 0xda => {
                let count = u16::from_be_bytes(self.take_bytes()?);
                deserialize_any_buffer(self, visitor, count as usize)
            }
            0xc6 | 0xdb => {
                let count = u32::from_be_bytes(self.take_bytes()?);
                deserialize_any_buffer(self, visitor, count as usize)
            }
            0xca => visitor.visit_f32(f32::from_be_bytes(self.take_bytes()?)),
            0xcb => visitor.visit_f64(f64::from_be_bytes(self.take_bytes()?)),
            0xcc => visitor.visit_u8(u8::from_be_bytes(self.take_bytes()?)),
            0xcd => visitor.visit_u16(u16::from_be_bytes(self.take_bytes()?)),
            0xce => visitor.visit_u32(u32::from_be_bytes(self.take_bytes()?)),
            0xcf => visitor.visit_u64(u64::from_be_bytes(self.take_bytes()?)),
            0xd0 => visitor.visit_i8(i8::from_be_bytes(self.take_bytes()?)),
            0xd1 => visitor.visit_i16(i16::from_be_bytes(self.take_bytes()?)),
            0xd2 => visitor.visit_i32(i32::from_be_bytes(self.take_bytes()?)),
            0xd3 => visitor.visit_i64(i64::from_be_bytes(self.take_bytes()?)),
            0xdc => {
                let count = u16::from_be_bytes(self.take_bytes()?) as usize;
                visitor.visit_seq(CDTListOrMap(count, self.0, self.1))
            }
            0xdd => {
                let count = u32::from_be_bytes(self.take_bytes()?) as usize;
                visitor.visit_seq(CDTListOrMap(count, self.0, self.1))
            }
            0xde => {
                let count = u16::from_be_bytes(self.take_bytes()?) as usize;
                visitor.visit_map(CDTListOrMap(count, self.0, self.1))
            }
            0xdf => {
                let count = u32::from_be_bytes(self.take_bytes()?) as usize;
                visitor.visit_map(CDTListOrMap(count, self.0, self.1))
            }
            0xe0..=0xff => {
                let value = (ptype - 0xe0) as i8 - 32;
                visitor.visit_i8(value)
            }
            _ => todo!()
        }
    }

    fn deserialize_bool<V>(mut self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        let ptype = self.take_byte()?;
        match ptype {
            0xc2 => visitor.visit_bool(false),
            0xc3 => visitor.visit_bool(true),
            _ => Err(Self::Error::invalid_type(self.as_unexpected(ptype)?, &visitor))
        }
    }

    fn deserialize_i8<V>(mut self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        let ptype = self.take_byte()?;
        match ptype {
            0x00..=0x7f => visitor.visit_i8(ptype as i8),
            0xcc => visitor.visit_i8(i8::try_from(u8::from_be_bytes(self.take_bytes()?))?),
            0xd0 => visitor.visit_i8(i8::from_be_bytes(self.take_bytes()?)),
            _ => Err(Self::Error::invalid_type(self.as_unexpected(ptype)?, &visitor))
        }
    }

    fn deserialize_i16<V>(mut self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        let ptype = self.take_byte()?;
        match ptype {
            0x00..=0x7f => visitor.visit_i16(ptype as i16),
            0xcc => visitor.visit_i16(u8::from_be_bytes(self.take_bytes()?) as i16),
            0xd0 => visitor.visit_i16(i8::from_be_bytes(self.take_bytes()?) as i16),
            0xcd => visitor.visit_i16(u16::from_be_bytes(self.take_bytes()?).try_into()?),
            0xd1 => visitor.visit_i16(i16::from_be_bytes(self.take_bytes()?)),
            _ => Err(Self::Error::invalid_type(self.as_unexpected(ptype)?, &visitor))
        }
    }

    fn deserialize_i32<V>(mut self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        let ptype = self.take_byte()?;
        match ptype {
            0x00..=0x7f => visitor.visit_i32(ptype as i32),
            0xcc => visitor.visit_i32(u8::from_be_bytes(self.take_bytes()?) as i32),
            0xcd => visitor.visit_i32(u16::from_be_bytes(self.take_bytes()?) as i32),
            0xce => visitor.visit_i32(u32::from_be_bytes(self.take_bytes()?).try_into()?),
            0xd0 => visitor.visit_i32(i8::from_be_bytes(self.take_bytes()?) as i32),
            0xd1 => visitor.visit_i32(i16::from_be_bytes(self.take_bytes()?) as i32),
            0xd2 => visitor.visit_i32(i32::from_be_bytes(self.take_bytes()?)),
            _ => Err(Self::Error::invalid_type(self.as_unexpected(ptype)?, &visitor))
        }
    }

    fn deserialize_i64<V>(mut self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        let ptype = self.take_byte()?;
        match ptype {
            0x00..=0x7f => visitor.visit_i64(ptype as i64),
            0xcc => visitor.visit_i64(u8::from_be_bytes(self.take_bytes()?) as i64),
            0xcd => visitor.visit_i64(u16::from_be_bytes(self.take_bytes()?) as i64),
            0xce => visitor.visit_i64(u32::from_be_bytes(self.take_bytes()?) as i64),
            0xcf => visitor.visit_i64(u64::from_be_bytes(self.take_bytes()?).try_into()?),
            0xd0 => visitor.visit_i64(i8::from_be_bytes(self.take_bytes()?) as i64),
            0xd1 => visitor.visit_i64(i16::from_be_bytes(self.take_bytes()?) as i64),
            0xd2 => visitor.visit_i64(i32::from_be_bytes(self.take_bytes()?) as i64),
            0xd3 => visitor.visit_i64(i64::from_be_bytes(self.take_bytes()?)),
            _ => Err(Self::Error::invalid_type(self.as_unexpected(ptype)?, &visitor))
        }
    }

    fn deserialize_u8<V>(mut self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        let ptype = self.take_byte()?;
        match ptype {
            0x00..=0x7f => visitor.visit_u8(ptype),
            0xcc => visitor.visit_u8(u8::from_be_bytes(self.take_bytes()?)),
            0xd0 => visitor.visit_u8(i8::from_be_bytes(self.take_bytes()?).try_into()?),
            _ => Err(Self::Error::invalid_type(self.as_unexpected(ptype)?, &visitor))
        }
    }

    fn deserialize_u16<V>(mut self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        let ptype = self.take_byte()?;
        match ptype {
            0x00..=0x7f => visitor.visit_i16(ptype as i16),
            0xcc => visitor.visit_i16(u8::from_be_bytes(self.take_bytes()?) as i16),
            0xd0 => visitor.visit_i16(i8::from_be_bytes(self.take_bytes()?) as i16),
            0xcd => visitor.visit_i16(u16::from_be_bytes(self.take_bytes()?).try_into()?),
            0xd1 => visitor.visit_i16(i16::from_be_bytes(self.take_bytes()?)),
            _ => Err(Self::Error::invalid_type(self.as_unexpected(ptype)?, &visitor))
        }
    }

    fn deserialize_u32<V>(mut self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        let ptype = self.take_byte()?;
        match ptype {
            0x00..=0x7f => visitor.visit_u64(ptype as u64),
            0xcc => visitor.visit_u32(u8::from_be_bytes(self.take_bytes()?) as u32),
            0xcd => visitor.visit_u32(u16::from_be_bytes(self.take_bytes()?) as u32),
            0xce => visitor.visit_u32(u32::from_be_bytes(self.take_bytes()?)),
            0xd0 => visitor.visit_u32(i8::from_be_bytes(self.take_bytes()?) as u32),
            0xd1 => visitor.visit_u32(i16::from_be_bytes(self.take_bytes()?) as u32),
            0xd2 => visitor.visit_u32(i32::from_be_bytes(self.take_bytes()?).try_into()?),
            _ => Err(Self::Error::invalid_type(self.as_unexpected(ptype)?, &visitor))
        }
    }

    fn deserialize_u64<V>(mut self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        let ptype = self.take_byte()?;
        match ptype {
            0x00..=0x7f => visitor.visit_u64(ptype as u64),
            0xcc => visitor.visit_u64(u8::from_be_bytes(self.take_bytes()?) as u64),
            0xcd => visitor.visit_u64(u16::from_be_bytes(self.take_bytes()?) as u64),
            0xce => visitor.visit_u64(u32::from_be_bytes(self.take_bytes()?) as u64),
            0xcf => visitor.visit_u64(u64::from_be_bytes(self.take_bytes()?)),
            0xd0 => visitor.visit_u64(i8::from_be_bytes(self.take_bytes()?) as u64),
            0xd1 => visitor.visit_u64(i16::from_be_bytes(self.take_bytes()?) as u64),
            0xd2 => visitor.visit_u64(i32::from_be_bytes(self.take_bytes()?) as u64),
            0xd3 => visitor.visit_u64(i64::from_be_bytes(self.take_bytes()?).try_into()?),
            _ => Err(Self::Error::invalid_type(self.as_unexpected(ptype)?, &visitor))
        }
    }

    fn deserialize_f32<V>(mut self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        let ptype = self.take_byte()?;
        match ptype {
            0xca => visitor.visit_f32(f32::from_be_bytes(self.take_bytes()?)),
            0xcb => visitor.visit_f32(f64::from_be_bytes(self.take_bytes()?) as f32),
            _ => Err(Self::Error::invalid_type(self.as_unexpected(ptype)?, &visitor))
        }
    }

    fn deserialize_f64<V>(mut self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        let ptype = self.take_byte()?;
        match ptype {
            0xca => visitor.visit_f64(f32::from_be_bytes(self.take_bytes()?).into()),
            0xcb => visitor.visit_f64(f64::from_be_bytes(self.take_bytes()?)),
            _ => Err(Self::Error::invalid_type(self.as_unexpected(ptype)?, &visitor))
        }
    }

    fn deserialize_char<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        self.deserialize_any(visitor)
    }

    fn deserialize_str<V>(mut self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        fn deserialize_any_buffer<'l, 'm, V>(mut deserializer: CDTDecoder<'m>, visitor: V, count: usize) -> std::prelude::v1::Result<V::Value, crate::errors::Error>
        where
            V: serde::de::Visitor<'l> {
            let ptype = ParticleType::from(deserializer.take_byte()?);
            let body = deserializer.take_nbyte(count - 1)?;
            if matches!(ptype, ParticleType::STRING | ParticleType::GEOJSON) {
                visitor.visit_str(std::str::from_utf8(body)?)
            } else {
                Err(crate::errors::Error::invalid_type(serde::de::Unexpected::Bytes(body), &visitor))
            }
        }

        let ptype = self.take_byte()?;
        match ptype {
            0xa0..=0xbf => deserialize_any_buffer(self, visitor, (ptype & 0x1f) as usize),
            0xc4 | 0xd9 => {
                let count = u8::from_be_bytes(self.take_bytes()?);
                deserialize_any_buffer(self, visitor, count as usize)
            }
            0xc5 | 0xda => {
                let count = u16::from_be_bytes(self.take_bytes()?);
                deserialize_any_buffer(self, visitor, count as usize)
            }
            0xc6 | 0xdb => {
                let count = u32::from_be_bytes(self.take_bytes()?);
                deserialize_any_buffer(self, visitor, count as usize)
            }
            _ => Err(Self::Error::invalid_type(self.as_unexpected(ptype)?, &visitor))
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
       self.deserialize_str(visitor)
    }

    // this is a very permissive handler that allows 
    fn deserialize_bytes<V>(mut self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        fn deserialize_any_buffer<'l, 'm, V>(mut deserializer: CDTDecoder<'m>, visitor: V, count: usize) -> std::prelude::v1::Result<V::Value, crate::errors::Error>
        where
            V: serde::de::Visitor<'l> {
            let body = deserializer.take_nbyte(count)?;
            // Allows string or geojson to be gotten as bytes
            visitor.visit_bytes(&body[1..])
        }

        // Since we allow the permissive parsing below, do not tamper with what we have here
        let ptype = self.0[*self.1];
        match ptype {
            0xa0..=0xbf => {
                let _ = self.take_byte();
                deserialize_any_buffer(self, visitor, (ptype & 0x1f) as usize)
            }
            0xc4 | 0xd9 => {
                let _ = self.take_byte();
                let count = u8::from_be_bytes(self.take_bytes()?);
                deserialize_any_buffer(self, visitor, count as usize)
            }
            0xc5 | 0xda => {
                let _ = self.take_byte();
                let count = u16::from_be_bytes(self.take_bytes()?);
                deserialize_any_buffer(self, visitor, count as usize)
            }
            0xc6 | 0xdb => {
                let _ = self.take_byte();
                let count = u32::from_be_bytes(self.take_bytes()?);
                deserialize_any_buffer(self, visitor, count as usize)
            }
            _ => {
                // Permissivly parse _anything_ into a byte array for pass-through.
                // The byte array is always immediately after this particle.
                let start_at = *self.1 + 1;
                // Deserialize whatever we have here to see how long it is.
                serde::de::IgnoredAny::deserialize(CDTDecoder(self.0, self.1))?;
                // The end of whatever is here must be where the upto pointer is now at.
                visitor.visit_bytes(&self.0[start_at..*self.1])
            }
        }
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        if self.0[*self.1] == 0xc0 {
            *self.1 += 1;
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        self.deserialize_any(visitor)
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        self.deserialize_any(visitor)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        self.deserialize_any(visitor)
    }

    fn deserialize_seq<V>(mut self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        let ptype = self.take_byte()?;
        match ptype {
            0x90..=0x9f => visitor.visit_seq(CDTListOrMap((ptype & 0x0f) as usize, self.0, self.1)),
            0xdc => {
                let count = u16::from_be_bytes(self.take_bytes()?) as usize;
                visitor.visit_seq(CDTListOrMap(count, self.0, self.1))
            }
            0xdd => {
                let count = u32::from_be_bytes(self.take_bytes()?) as usize;
                visitor.visit_seq(CDTListOrMap(count, self.0, self.1))
            }
            _ => Err(Self::Error::invalid_type(self.as_unexpected(ptype)?, &visitor))
        }
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        self.deserialize_any(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        self.deserialize_any(visitor)
    }

    fn deserialize_map<V>(mut self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        let ptype = self.take_byte()?;
        match ptype {
            0x80..=0x8f => visitor.visit_map(CDTListOrMap((ptype & 0x0f) as usize, self.0, self.1)),
            0xde => {
                let count = u16::from_be_bytes(self.take_bytes()?) as usize;
                visitor.visit_map(CDTListOrMap(count, self.0, self.1))
            }
            0xdf => {
                let count = u32::from_be_bytes(self.take_bytes()?) as usize;
                visitor.visit_map(CDTListOrMap(count, self.0, self.1))
            }
            _ => Err(Self::Error::invalid_type(self.as_unexpected(ptype)?, &visitor))
        }
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {

        let ptype = self.0[*self.1];
        let particle_type = match ptype {
            0x00..=0x7f | 0xcc | 0xcd | 0xce | 0xcf | 0xd0 | 0xd1 | 0xd2 | 0xd3 | 0xe0..=0xff => ParticleType::INTEGER,
            0x80..=0x8f | 0xde | 0xdf => ParticleType::MAP,
            0x90..=0x9f | 0xdc | 0xdd => ParticleType::LIST,
            0xc0 => ParticleType::NULL,
            0xc2 | 0xc3 => ParticleType::BOOL,
            // In blobs, the particle type is hiding just after the length
            0xa0..=0xbf => ParticleType::from(self.0[*self.1 + 1]),
            0xc4 | 0xd9 => ParticleType::from(self.0[*self.1 + 2]),
            0xc5 | 0xda => ParticleType::from(self.0[*self.1 + 3]),
            0xc6 | 0xdb => ParticleType::from(self.0[*self.1 + 5]),
            0xca | 0xcb => ParticleType::FLOAT,
            _ => ParticleType::NULL
        };
        visitor.visit_enum(EnumAdaptor{particle_type, deserializer: self})
    }

    fn deserialize_identifier<V>(self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(mut self, visitor: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'l> {
        fn ignore_values<'l, 'm>(deserializer: CDTDecoder<'m>, entries: usize) {
            struct IgnoreVisitor;
            impl<'l> Visitor<'l> for IgnoreVisitor {
                type Value = ();
            
                fn visit_none<E>(self) -> std::prelude::v1::Result<Self::Value, E>
                    where
                        E: serde::de::Error, {
                    Ok(())
                }
                fn expecting(&self, _formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                    Ok(())
                }
            }
            for _ in 0..entries {
                let _ = CDTDecoder(deserializer.0, deserializer.1).deserialize_ignored_any(IgnoreVisitor);
            }
        }

        let ptype = self.take_byte()?;
        match ptype {
            0x80..=0x8f => ignore_values(self, (ptype & 0x0f) as usize * 2),
            0x90..=0x9f => ignore_values(self, (ptype & 0x0f) as usize),
            0xa0..=0xbf => { *self.1 += (ptype & 0x1f) as usize; },
            0xc4 | 0xd9 => {
                let count = u8::from_be_bytes(self.take_bytes()?);
                *self.1 += count as usize;
            }
            0xc5 | 0xda => {
                let count = u16::from_be_bytes(self.take_bytes()?);
                *self.1 += count as usize;
            }
            0xc6 | 0xdb => {
                let count = u32::from_be_bytes(self.take_bytes()?);
                *self.1 += count as usize;
            }
            0xcc | 0xd0 => {self.take_byte()?;}
            0xcd | 0xd1 => {self.take_bytes::<2>()?;}
            0xca | 0xce | 0xd2 => {self.take_bytes::<4>()?;}
            0xcb | 0xcf | 0xd3 => {self.take_bytes::<8>()?;}
            0xdc => {
                let count = u16::from_be_bytes(self.take_bytes()?) as usize;
                ignore_values(self, count)
            }
            0xdd => {
                let count = u32::from_be_bytes(self.take_bytes()?) as usize;
                ignore_values(self, count)
            }
            0xde => {
                let count = u16::from_be_bytes(self.take_bytes()?) as usize;
                ignore_values(self, count * 2)
            }
            0xdf => {
                let count = u32::from_be_bytes(self.take_bytes()?) as usize;
                ignore_values(self, count * 2)
            }
            _ => ()
        }
        visitor.visit_none()
    }
}

impl<'l, 'm> MapAccess<'l> for CDTListOrMap<'m> {
    type Error = crate::errors::Error;

    fn next_key_seed<K>(&mut self, seed: K) -> std::prelude::v1::Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'l> {
        if self.0 == 0 {
            Ok(None)
        } else {
            self.0 -= 1;
            seed.deserialize(CDTDecoder(self.1, self.2)).map(Some)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> std::prelude::v1::Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'l> {
        seed.deserialize(CDTDecoder(self.1, self.2))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.0)
    }
}

impl<'l, 'm> SeqAccess<'l> for CDTListOrMap<'m> {
    type Error = crate::errors::Error;

    fn next_element_seed<T>(&mut self, seed: T) -> std::prelude::v1::Result<Option<T::Value>, Self::Error>
    where
        T: serde::de::DeserializeSeed<'l> {
        if self.0 == 0 {
            Ok(None)
        } else {
            self.0 -= 1;
            seed.deserialize(CDTDecoder(self.1, self.2)).map(Some)
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.0)
    }
}

/// Includes the data for the Value part of a Bin.
#[derive(Debug, Clone)]
pub(crate) struct PreParsedValue{
    pub particle_type: u8,
    pub name_len: u8,
    pub name: [u8; 15],
    pub particle: Vec<u8>,
}

impl PreParsedValue {
    fn particle_type(&self) -> ParticleType {
        ParticleType::from(self.particle_type)
    }

    fn name(&self) -> Result<&str> {
        let len = self.name_len as usize;
        let s = std::str::from_utf8(&self.name[..len])?;
        Ok(s)
    }

    fn particle(&self) -> &[u8] {
        &self.particle
    }

    fn as_bool(&self) -> Result<bool> {
        let [element]: [u8; 1] = self.particle().try_into()?;
        Ok(element != 0)
    }

    fn as_int(&self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.particle().try_into()?))
    }

    fn as_float(&self) -> Result<f64> {
        Ok(f64::from_be_bytes(self.particle().try_into()?))
    }

    fn into_blob(self) -> Vec<u8> {
        self.particle
    }

    fn into_string(self) -> Result<String> {
        Ok(std::string::String::from_utf8(self.particle)?)
    }
}


#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde::Deserialize;

    use crate::derive::readable::PreParsedValue;

    #[derive(Deserialize)]
    struct SomeTupleThing(i32, String);
    
    #[derive(Deserialize, Clone, Copy)]
    struct ANormalStruct {
        one: u32,
        two: i16,
    }

    #[derive(Deserialize)]
    struct MoreComplexStruct {
        binname: SomeTupleThing,
        another_binname: Option<ANormalStruct>,
    }

    fn new_preparsed(particle_type: u8, name: &str, particle: Vec<u8>) -> PreParsedValue {
        let mut namebuf = [0_u8; 15];
        let name_len = name.as_bytes().len();
        namebuf[..name_len].copy_from_slice(name.as_bytes());
        PreParsedValue {
            particle_type,
            name_len: name_len as u8,
            name: namebuf,
            particle,
        }
    }

    #[test]
    fn destream_structs() {
        let mut buffer = crate::Buffer::new(1024);
        let myval = crate::Value::List(vec![
            crate::Value::Int(2),
            crate::Value::String("Hello world".to_string()),
        ]);
        
        buffer.resize_buffer(myval.estimate_size()).unwrap();
        myval.write_to(&mut buffer);

        let as_bin = new_preparsed(20, "binname", buffer.data_buffer);

        let deserialized = SomeTupleThing::deserialize(as_bin.clone()).unwrap();
        assert_eq!(deserialized.0, 2);
        assert_eq!(deserialized.1, "Hello world");

        let deserialized = MoreComplexStruct::deserialize(crate::derive::readable::BinsDeserializer{bins: vec![as_bin.clone()].into()}).unwrap();
        assert_eq!(deserialized.binname.0, 2);
        assert_eq!(deserialized.binname.1, "Hello world");
        assert!(deserialized.another_binname.is_none());

        let mut buffer = crate::Buffer::new(1024);
        let myval = crate::Value::HashMap(HashMap::from([
            (crate::Value::String("one".to_string()), crate::Value::Int(1)),
            (crate::Value::String("two".to_string()), crate::Value::Int(2)),
        ]));
        
        buffer.resize_buffer(myval.estimate_size()).unwrap();
        myval.write_to(&mut buffer);

        let another_bin = new_preparsed(20, "another_binname", buffer.data_buffer);

        let deserialized = ANormalStruct::deserialize(another_bin.clone()).unwrap();
        assert_eq!(deserialized.one, 1);
        assert_eq!(deserialized.two, 2);

        let deserialized = MoreComplexStruct::deserialize(crate::derive::readable::BinsDeserializer{bins: vec![as_bin.clone(), another_bin.clone()].into()}).unwrap();
        assert_eq!(deserialized.binname.0, 2);
        assert_eq!(deserialized.binname.1, "Hello world");
        assert_eq!(deserialized.another_binname.unwrap().one, 1);
        assert_eq!(deserialized.another_binname.unwrap().two, 2);
    }


    #[test]
    fn destream_value() {
        let mut buffer = crate::Buffer::new(1024);
        let myval = crate::Value::String("Hello world".to_string());
        
        buffer.resize_buffer(myval.estimate_size()).unwrap();
        myval.write_to(&mut buffer);

        let as_bin = new_preparsed(myval.particle_type() as u8, "binname", buffer.data_buffer);

        let deserialized = crate::Value::deserialize(as_bin.clone()).unwrap();
        assert_eq!(deserialized, crate::Value::String("Hello world".to_string()));

        let mut buffer = crate::Buffer::new(1024);
        let myval = crate::Value::List(vec![
            crate::Value::Int(2),
            crate::Value::String("Hello world".to_string()),
        ]);
        
        buffer.resize_buffer(myval.estimate_size()).unwrap();
        myval.write_to(&mut buffer);

        let as_bin = new_preparsed(20, "binname", buffer.data_buffer);
        let deserialized = crate::Value::deserialize(as_bin.clone()).unwrap();
        assert_eq!(deserialized, crate::Value::List(vec![
            crate::Value::Int(2),
            crate::Value::String("Hello world".to_string()),
        ]));
    }

    #[test]
    fn destream_f64_value() {
        let mut buffer = crate::Buffer::new(1024);
        let myval = crate::Value::from(0.0023_f64);
        
        buffer.resize_buffer(myval.estimate_size()).unwrap();
        myval.write_to(&mut buffer);

        let as_bin = new_preparsed(myval.particle_type() as u8, "binname", buffer.data_buffer);

        let deserialized = crate::Value::deserialize(as_bin.clone()).unwrap();
        assert_eq!(deserialized, myval);

        let mut buffer = crate::Buffer::new(1024);
        let myval = crate::Value::List(vec![
            myval
        ]);
        
        buffer.resize_buffer(myval.estimate_size()).unwrap();
        myval.write_to(&mut buffer);

        let as_bin = new_preparsed(20, "binname", buffer.data_buffer);
        let deserialized = crate::Value::deserialize(as_bin.clone()).unwrap();
        assert_eq!(deserialized, myval);
    }

    #[test]
    fn destream_f32_value() {
        let myval = crate::Value::from(0.0023_f32);
        
        let mut buffer = crate::Buffer::new(1024);
        let myval = crate::Value::List(vec![
            myval
        ]);
        
        buffer.resize_buffer(myval.estimate_size()).unwrap();
        myval.write_to(&mut buffer);

        let as_bin = new_preparsed(20, "binname", buffer.data_buffer);
        let deserialized = crate::Value::deserialize(as_bin.clone()).unwrap();
        assert_eq!(deserialized, myval);
    }
}
