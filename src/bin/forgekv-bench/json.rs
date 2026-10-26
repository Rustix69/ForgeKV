//! A minimal JSON value and pretty-printer (no serde dependency). Object keys
//! keep insertion order so result files diff cleanly.

#[derive(Debug, Clone, PartialEq)]
pub enum J {
    Null,
    Bool(bool),
    Int(u64),
    Num(f64),
    Str(String),
    Arr(Vec<J>),
    Obj(Vec<(String, J)>),
}

impl J {
    pub fn str(s: impl Into<String>) -> J {
        J::Str(s.into())
    }

    pub fn obj<K: Into<String>>(fields: impl IntoIterator<Item = (K, J)>) -> J {
        J::Obj(fields.into_iter().map(|(k, v)| (k.into(), v)).collect())
    }

    pub fn opt<T>(v: Option<T>, f: impl FnOnce(T) -> J) -> J {
        v.map_or(J::Null, f)
    }

    pub fn render(&self) -> String {
        let mut out = String::new();
        self.write(&mut out, 0);
        out.push('\n');
        out
    }

    fn write(&self, out: &mut String, indent: usize) {
        match self {
            J::Null => out.push_str("null"),
            J::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            J::Int(i) => out.push_str(&i.to_string()),
            // Rust's `Display` for f64 never uses exponent notation.
            J::Num(x) if x.is_finite() => out.push_str(&x.to_string()),
            J::Num(_) => out.push_str("null"),
            J::Str(s) => escape(s, out),
            J::Arr(items) => {
                if items.is_empty() {
                    out.push_str("[]");
                    return;
                }
                out.push('[');
                for (i, v) in items.iter().enumerate() {
                    out.push_str(if i == 0 { "\n" } else { ",\n" });
                    pad(out, indent + 1);
                    v.write(out, indent + 1);
                }
                out.push('\n');
                pad(out, indent);
                out.push(']');
            }
            J::Obj(fields) => {
                if fields.is_empty() {
                    out.push_str("{}");
                    return;
                }
                out.push('{');
                for (i, (k, v)) in fields.iter().enumerate() {
                    out.push_str(if i == 0 { "\n" } else { ",\n" });
                    pad(out, indent + 1);
                    escape(k, out);
                    out.push_str(": ");
                    v.write(out, indent + 1);
                }
                out.push('\n');
                pad(out, indent);
                out.push('}');
            }
        }
    }
}

fn pad(out: &mut String, indent: usize) {
    for _ in 0..indent {
        out.push_str("  ");
    }
}

fn escape(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_nested_values_and_escapes() {
        let j = J::obj([
            ("a", J::Int(1)),
            ("b", J::Arr(vec![J::Num(1.5), J::Null, J::Bool(true)])),
            ("c", J::str("q\"\\\n\u{1}")),
            ("d", J::Num(f64::NAN)),
            ("e", J::Obj(vec![])),
        ]);
        assert_eq!(
            j.render(),
            "{\n  \"a\": 1,\n  \"b\": [\n    1.5,\n    null,\n    true\n  ],\n  \"c\": \"q\\\"\\\\\\n\\u0001\",\n  \"d\": null,\n  \"e\": {}\n}\n"
        );
        assert_eq!(J::Num(0.000001).render(), "0.000001\n");
    }
}
