//! Static value generator and YAML to UniversalValue conversion.

use serde_yaml::Value as YamlValue;
use std::collections::HashMap;
use sync_core::UniversalValue;

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
                UniversalValue::String(n.to_string())
            }
        }
        YamlValue::String(s) => UniversalValue::String(s.clone()),
        YamlValue::Sequence(arr) => {
            let values: Vec<UniversalValue> = arr.iter().map(yaml_to_generated_value).collect();
            UniversalValue::Array(values)
        }
        YamlValue::Mapping(map) => {
            let values: HashMap<String, UniversalValue> = map
                .iter()
                .filter_map(|(k, v)| {
                    let key = match k {
                        YamlValue::String(s) => s.clone(),
                        _ => k.as_str().map(|s| s.to_string())?,
                    };
                    Some((key, yaml_to_generated_value(v)))
                })
                .collect();
            UniversalValue::Object(values)
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
            panic!("Expected Float64");
        }
    }

    #[test]
    fn test_yaml_string() {
        let yaml = YamlValue::String("hello".to_string());
        assert_eq!(
            yaml_to_generated_value(&yaml),
            UniversalValue::String("hello".to_string())
        );
    }

    #[test]
    fn test_yaml_array() {
        let yaml: YamlValue = serde_yaml::from_str("[1, 2, 3]").unwrap();
        if let UniversalValue::Array(arr) = yaml_to_generated_value(&yaml) {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], UniversalValue::Int64(1));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_yaml_object() {
        let yaml: YamlValue = serde_yaml::from_str("{ version: 1, name: test }").unwrap();
        if let UniversalValue::Object(obj) = yaml_to_generated_value(&yaml) {
            assert_eq!(obj.get("version"), Some(&UniversalValue::Int64(1)));
            assert_eq!(
                obj.get("name"),
                Some(&UniversalValue::String("test".to_string()))
            );
        } else {
            panic!("Expected Object");
        }
    }
}
