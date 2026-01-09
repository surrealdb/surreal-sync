//! Protobuf decoder implementation.
//!
//! This module provides the runtime protobuf decoder that uses the schema
//! from the parser module and produces ProtoMessage/ProtoFieldValue from
//! kafka-types.

use crate::error::{Error, Result};
use kafka_types::{
    ProtoFieldDescriptor, ProtoFieldValue, ProtoMessage, ProtoMessageDescriptor, ProtoSchema,
    ProtoType,
};
use protobuf::CodedInputStream;
use std::collections::HashMap;

/// Runtime protobuf decoder.
///
/// Decodes binary protobuf data into ProtoMessage using a parsed schema.
pub struct ProtoDecoder {
    schema: ProtoSchema,
}

impl ProtoDecoder {
    /// Create a new decoder from a schema.
    pub fn new(schema: ProtoSchema) -> Self {
        Self { schema }
    }

    /// Get a reference to the schema.
    pub fn schema(&self) -> &ProtoSchema {
        &self.schema
    }

    /// Decode a protobuf message from bytes.
    pub fn decode(&self, message_type: &str, data: &[u8]) -> Result<ProtoMessage> {
        let descriptor = self
            .schema
            .get_message(message_type)
            .ok_or_else(|| Error::MessageTypeNotFound(message_type.to_string()))?;
        let mut stream = CodedInputStream::from_bytes(data);
        self.decode_message(descriptor, &mut stream)
    }

    fn decode_message(
        &self,
        descriptor: &ProtoMessageDescriptor,
        stream: &mut CodedInputStream,
    ) -> Result<ProtoMessage> {
        let mut fields = HashMap::new();

        loop {
            if stream
                .eof()
                .map_err(|e| Error::ProtobufDecode(e.to_string()))?
            {
                break;
            }

            let tag = stream
                .read_raw_varint32()
                .map_err(|e| Error::ProtobufDecode(e.to_string()))?;

            if tag == 0 {
                break;
            }

            let field_number = (tag >> 3) as i32;

            // Find the field descriptor by number
            let field_desc = descriptor
                .fields
                .values()
                .find(|f| f.number == field_number)
                .ok_or_else(|| {
                    Error::ProtobufDecode(format!(
                        "Unknown field number: {} in message {}",
                        field_number, descriptor.name
                    ))
                })?;

            let value = if field_desc.is_repeated {
                let existing = fields
                    .entry(field_desc.name.clone())
                    .or_insert_with(|| ProtoFieldValue::Repeated(Vec::new()));

                if let ProtoFieldValue::Repeated(values) = existing {
                    let new_value = self.decode_field_value(field_desc, stream)?;
                    values.push(new_value);
                }
                continue;
            } else {
                self.decode_field_value(field_desc, stream)?
            };

            fields.insert(field_desc.name.clone(), value);
        }

        Ok(ProtoMessage {
            message_type: descriptor.name.clone(),
            fields,
            descriptor: descriptor.clone(),
        })
    }

    fn decode_field_value(
        &self,
        field_desc: &ProtoFieldDescriptor,
        stream: &mut CodedInputStream,
    ) -> Result<ProtoFieldValue> {
        match &field_desc.field_type {
            ProtoType::Double => {
                let v = stream
                    .read_double()
                    .map_err(|e| Error::ProtobufDecode(e.to_string()))?;
                Ok(ProtoFieldValue::Double(v))
            }
            ProtoType::Float => {
                let v = stream
                    .read_float()
                    .map_err(|e| Error::ProtobufDecode(e.to_string()))?;
                Ok(ProtoFieldValue::Float(v))
            }
            ProtoType::Int32 | ProtoType::Sint32 | ProtoType::Sfixed32 => {
                let v = stream
                    .read_int32()
                    .map_err(|e| Error::ProtobufDecode(e.to_string()))?;
                Ok(ProtoFieldValue::Int32(v))
            }
            ProtoType::Int64 | ProtoType::Sint64 | ProtoType::Sfixed64 => {
                let v = stream
                    .read_int64()
                    .map_err(|e| Error::ProtobufDecode(e.to_string()))?;
                Ok(ProtoFieldValue::Int64(v))
            }
            ProtoType::Uint32 | ProtoType::Fixed32 => {
                let v = stream
                    .read_uint32()
                    .map_err(|e| Error::ProtobufDecode(e.to_string()))?;
                Ok(ProtoFieldValue::Uint32(v))
            }
            ProtoType::Uint64 | ProtoType::Fixed64 => {
                let v = stream
                    .read_uint64()
                    .map_err(|e| Error::ProtobufDecode(e.to_string()))?;
                Ok(ProtoFieldValue::Uint64(v))
            }
            ProtoType::Bool => {
                let v = stream
                    .read_bool()
                    .map_err(|e| Error::ProtobufDecode(e.to_string()))?;
                Ok(ProtoFieldValue::Bool(v))
            }
            ProtoType::String => {
                let v = stream
                    .read_string()
                    .map_err(|e| Error::ProtobufDecode(e.to_string()))?;
                Ok(ProtoFieldValue::String(v))
            }
            ProtoType::Bytes => {
                let v = stream
                    .read_bytes()
                    .map_err(|e| Error::ProtobufDecode(e.to_string()))?;
                Ok(ProtoFieldValue::Bytes(v.to_vec()))
            }
            ProtoType::Message(type_name) => {
                let len = stream
                    .read_uint32()
                    .map_err(|e| Error::ProtobufDecode(e.to_string()))?;
                let old_limit = stream
                    .push_limit(len as u64)
                    .map_err(|e| Error::ProtobufDecode(e.to_string()))?;

                // Extract just the type name without package prefix
                let simple_type = type_name.split('.').next_back().unwrap_or(type_name);
                let nested_descriptor = self
                    .schema
                    .get_message(simple_type)
                    .ok_or_else(|| Error::MessageTypeNotFound(simple_type.to_string()))?;
                let nested_message = self.decode_message(nested_descriptor, stream)?;

                stream.pop_limit(old_limit);

                Ok(ProtoFieldValue::Message(Box::new(nested_message)))
            }
            ProtoType::Enum(_) => {
                let v = stream
                    .read_int32()
                    .map_err(|e| Error::ProtobufDecode(e.to_string()))?;
                Ok(ProtoFieldValue::Int32(v))
            }
            _ => Err(Error::ProtobufDecode(format!(
                "Unsupported field type: {:?}",
                field_desc.field_type
            ))),
        }
    }
}
