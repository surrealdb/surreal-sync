use super::parser::{ProtoFieldDescriptor, ProtoMessageDescriptor, ProtoSchema, ProtoType};
use crate::error::{Error, Result};
use protobuf::CodedInputStream;
use std::collections::HashMap;

/// Represents a decoded protobuf message
#[derive(Debug, Clone)]
pub struct ProtoMessage {
    /// Message type name
    pub message_type: String,
    /// Decoded field values
    pub fields: HashMap<String, ProtoFieldValue>,
    /// Schema reference for field introspection
    pub descriptor: ProtoMessageDescriptor,
}

/// Represents a field value in a decoded message
#[derive(Debug, Clone)]
pub enum ProtoFieldValue {
    Double(f64),
    Float(f32),
    Int32(i32),
    Int64(i64),
    Uint32(u32),
    Uint64(u64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
    Message(Box<ProtoMessage>),
    Repeated(Vec<ProtoFieldValue>),
    Null,
}

impl ProtoFieldValue {
    /// Get the type name of this field value
    pub fn proto_field_type(&self) -> ProtoType {
        match self {
            ProtoFieldValue::Double(_) => ProtoType::Double,
            ProtoFieldValue::Float(_) => ProtoType::Float,
            ProtoFieldValue::Int32(_) => ProtoType::Int32,
            ProtoFieldValue::Int64(_) => ProtoType::Int64,
            ProtoFieldValue::Uint32(_) => ProtoType::Uint32,
            ProtoFieldValue::Uint64(_) => ProtoType::Uint64,
            ProtoFieldValue::Bool(_) => ProtoType::Bool,
            ProtoFieldValue::String(_) => ProtoType::String,
            ProtoFieldValue::Bytes(_) => ProtoType::Bytes,
            ProtoFieldValue::Message(msg) => ProtoType::Message(msg.message_type.clone()),
            ProtoFieldValue::Repeated(_) => ProtoType::Repeated(Box::new(ProtoType::Null)), // Placeholder,
            ProtoFieldValue::Null => ProtoType::Null,
        }
    }
}

/// Runtime protobuf decoder
pub struct ProtoDecoder {
    schema: ProtoSchema,
}

impl ProtoDecoder {
    /// Create a new decoder from a schema
    pub fn new(schema: ProtoSchema) -> Self {
        Self { schema }
    }

    /// Decode a protobuf message from bytes
    pub fn decode(&self, message_type: &str, data: &[u8]) -> Result<ProtoMessage> {
        let descriptor = self.schema.get_message(message_type)?;
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
                let nested_descriptor = self.schema.get_message(simple_type)?;
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

impl ProtoMessage {
    /// List all field names in this message
    pub fn list_fields(&self) -> Vec<String> {
        self.descriptor.field_order.clone()
    }

    /// Get a field value by name
    pub fn get_field(&self, name: &str) -> Result<&ProtoFieldValue> {
        self.fields
            .get(name)
            .ok_or_else(|| Error::FieldNotFound(name.to_string()))
    }

    /// Get a field value as a specific type
    pub fn get_string(&self, name: &str) -> Result<&str> {
        match self.get_field(name)? {
            ProtoFieldValue::String(s) => Ok(s),
            other => Err(Error::InvalidFieldType {
                expected: ProtoType::String,
                actual: other.proto_field_type(),
            }),
        }
    }

    pub fn get_int64(&self, name: &str) -> Result<i64> {
        match self.get_field(name)? {
            ProtoFieldValue::Int64(v) => Ok(*v),
            other => Err(Error::InvalidFieldType {
                expected: ProtoType::Int64,
                actual: other.proto_field_type(),
            }),
        }
    }

    pub fn get_int32(&self, name: &str) -> Result<i32> {
        match self.get_field(name)? {
            ProtoFieldValue::Int32(v) => Ok(*v),
            other => Err(Error::InvalidFieldType {
                expected: ProtoType::Int32,
                actual: other.proto_field_type(),
            }),
        }
    }

    pub fn get_uint64(&self, name: &str) -> Result<u64> {
        match self.get_field(name)? {
            ProtoFieldValue::Uint64(v) => Ok(*v),
            other => Err(Error::InvalidFieldType {
                expected: ProtoType::Uint64,
                actual: other.proto_field_type(),
            }),
        }
    }

    pub fn get_uint32(&self, name: &str) -> Result<u32> {
        match self.get_field(name)? {
            ProtoFieldValue::Uint32(v) => Ok(*v),
            other => Err(Error::InvalidFieldType {
                expected: ProtoType::Uint32,
                actual: other.proto_field_type(),
            }),
        }
    }

    pub fn get_bool(&self, name: &str) -> Result<bool> {
        match self.get_field(name)? {
            ProtoFieldValue::Bool(v) => Ok(*v),
            other => Err(Error::InvalidFieldType {
                expected: ProtoType::Bool,
                actual: other.proto_field_type(),
            }),
        }
    }

    pub fn get_double(&self, name: &str) -> Result<f64> {
        match self.get_field(name)? {
            ProtoFieldValue::Double(v) => Ok(*v),
            other => Err(Error::InvalidFieldType {
                expected: ProtoType::Double,
                actual: other.proto_field_type(),
            }),
        }
    }

    pub fn get_float(&self, name: &str) -> Result<f32> {
        match self.get_field(name)? {
            ProtoFieldValue::Float(v) => Ok(*v),
            other => Err(Error::InvalidFieldType {
                expected: ProtoType::Float,
                actual: other.proto_field_type(),
            }),
        }
    }

    pub fn get_bytes(&self, name: &str) -> Result<&[u8]> {
        match self.get_field(name)? {
            ProtoFieldValue::Bytes(v) => Ok(v),
            other => Err(Error::InvalidFieldType {
                expected: ProtoType::Bytes,
                actual: other.proto_field_type(),
            }),
        }
    }

    pub fn get_message(&self, name: &str) -> Result<&ProtoMessage> {
        match self.get_field(name)? {
            ProtoFieldValue::Message(m) => Ok(m),
            other => Err(Error::InvalidFieldType {
                expected: ProtoType::Message("".to_string()), // Placeholder
                actual: other.proto_field_type(),
            }),
        }
    }

    pub fn get_repeated(&self, name: &str) -> Result<&[ProtoFieldValue]> {
        match self.get_field(name)? {
            ProtoFieldValue::Repeated(values) => Ok(values),
            other => Err(Error::InvalidFieldType {
                expected: ProtoType::Repeated(Box::new(ProtoType::Null)), // Placeholder
                actual: other.proto_field_type(),
            }),
        }
    }
}
