//! Static value generator and YAML to UniversalValue conversion.

use serde_yaml::Value as YamlValue;
use sync_core::UniversalValue;

/// Convert a YAML value to a serde_json::Value.
fn yaml_to_json_value(yaml: &YamlValue) -> serde_json::Value {
    match yaml {
        YamlValue::Null => serde_json::Value::Null,
        YamlValue::Bool(b) => serde_json::Value::Bool(*b),
        YamlValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                serde_json::Value::Number(i.into())
            } else if let Some(f) = n.as_f64() {
                serde_json::Number::from_f64(f)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null)
            } else {
                serde_json::Value::String(n.to_string())
            }
        }
        YamlValue::String(s) => serde_json::Value::String(s.clone()),
        YamlValue::Sequence(arr) => {
            let values: Vec<serde_json::Value> = arr.iter().map(yaml_to_json_value).collect();
            serde_json::Value::Array(values)
        }
        YamlValue::Mapping(map) => {
            let values: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .filter_map(|(k, v)| {
                    let key = match k {
                        YamlValue::String(s) => s.clone(),
                        _ => k.as_str().map(|s| s.to_string())?,
                    };
                    Some((key, yaml_to_json_value(v)))
                })
                .collect();
            serde_json::Value::Object(values)
        }
        YamlValue::Tagged(tagged) => yaml_to_json_value(&tagged.value),
    }
}

/// Convert a YAML value to a UniversalValue.
pub fn yaml_to_generated_value(yaml: &YamlValue) -> UniversalValue {
    match yaml {
        YamlValue::Null => UniversalValue::Null,
        YamlValue::Bool(b) => UniversalValue::Bool(*b),
        YamlValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                UniversalValue::Int64(i)
            } else if let Some(f) = n.as_f64() {
                UniversalValue::Float64(f)
            } else {
                UniversalValue::Text(n.to_string())
            }
        }
        YamlValue::String(s) => UniversalValue::Text(s.clone()),
        YamlValue::Sequence(arr) => {
            let values: Vec<UniversalValue> = arr.iter().map(yaml_to_generated_value).collect();
            // Use Text as default element type for YAML arrays
            UniversalValue::Array {
                elements: values,
                element_type: Box::new(sync_core::UniversalType::Text),
            }
        }
        YamlValue::Mapping(map) => {
            let values: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .filter_map(|(k, v)| {
                    let key = match k {
                        YamlValue::String(s) => s.clone(),
                        _ => k.as_str().map(|s| s.to_string())?,
                    };
                    Some((key, yaml_to_json_value(v)))
                })
                .collect();
            UniversalValue::Json(Box::new(serde_json::Value::Object(values)))
        }
        YamlValue::Tagged(tagged) => yaml_to_generated_value(&tagged.value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_yaml_null() {
        let yaml = YamlValue::Null;
        assert_eq!(yaml_to_generated_value(&yaml), UniversalValue::Null);
    }

    #[test]
    fn test_yaml_bool() {
        let yaml = YamlValue::Bool(true);
        assert_eq!(yaml_to_generated_value(&yaml), UniversalValue::Bool(true));
    }

    #[test]
    fn test_yaml_int() {
        let yaml: YamlValue = serde_yaml::from_str("42").unwrap();
        assert_eq!(yaml_to_generated_value(&yaml), UniversalValue::Int64(42));
    }

    #[test]
    fn test_yaml_float() {
        let yaml: YamlValue = serde_yaml::from_str("1.234").unwrap();
        if let UniversalValue::Float64(f) = yaml_to_generated_value(&yaml) {
            assert!((f - 1.234).abs() < 0.001);
        } else {
            panic!("Expected Double");
        }
    }

    #[test]
    fn test_yaml_string() {
        let yaml = YamlValue::String("hello".to_string());
        assert_eq!(
            yaml_to_generated_value(&yaml),
            UniversalValue::Text("hello".to_string())
        );
    }

    #[test]
    fn test_yaml_array() {
        let yaml: YamlValue = serde_yaml::from_str("[1, 2, 3]").unwrap();
        if let UniversalValue::Array { elements, .. } = yaml_to_generated_value(&yaml) {
            assert_eq!(elements.len(), 3);
            assert_eq!(elements[0], UniversalValue::Int64(1));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_yaml_object() {
        let yaml: YamlValue = serde_yaml::from_str("{ version: 1, name: test }").unwrap();
        if let UniversalValue::Json(payload) = yaml_to_generated_value(&yaml) {
            if let serde_json::Value::Object(obj) = &*payload {
                assert_eq!(obj.get("version"), Some(&serde_json::json!(1)));
                assert_eq!(obj.get("name"), Some(&serde_json::json!("test")));
            } else {
                panic!("Expected Object payload");
            }
        } else {
            panic!("Expected Json");
        }
    }
}
