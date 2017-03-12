use bson::Bson;
use nalgebra::ApproxEq;

pub trait NumEq {
    fn float_eq(&self, f: f64) -> bool;
    fn int_eq(&self, i: i64) -> bool;
}

#[cfg_attr(feature = "clippy", allow(cast_precision_loss))]
impl NumEq for Bson {
    fn float_eq(&self, f: f64) -> bool {
        match *self {
            Bson::FloatingPoint(ref ff) => f.approx_eq(ff),
            Bson::I32(i) => f.approx_eq(&(i as f64)),
            Bson::I64(i) => f.approx_eq(&(i as f64)),
            _ => false,
        }
    }

    fn int_eq(&self, i: i64) -> bool {
        match *self {
            Bson::FloatingPoint(ref f) => f.approx_eq(&(i as f64)),
            Bson::I32(ii) => i == (ii as i64),
            Bson::I64(ii) => i == ii,
            _ => false,
        }
    }
}

pub fn bson_eq(b1: &Bson, b2: &Bson) -> bool {
    match *b1 {
        Bson::FloatingPoint(f) => b2.float_eq(f),
        Bson::I32(i) => b2.int_eq(i as i64),
        Bson::I64(i) => b2.int_eq(i),
        Bson::String(ref s) => var_match!(*b2, Bson::String(ref ss) => s == ss),
        Bson::Array(ref arr) => {
            var_match!(*b2, Bson::Array(ref other_arr) => {
            for val1 in arr {
                for val2 in other_arr {
                    if !bson_eq(val1, val2) {
                        return false
                    }
                }
            }
            true
        })
        }
        Bson::Document(ref doc) => {
            var_match!(*b2, Bson::Document(ref other_doc) => doc == other_doc)
        }
        Bson::Boolean(b) => var_match!(*b2, Bson::Boolean(bb) => b == bb),
        Bson::Null => var_match!(*b2, Bson::Null => true),
        Bson::RegExp(ref s1, ref s2) => {
            var_match!(*b2, Bson::RegExp(ref ss1, ref ss2) =>
                       s1 == ss1 && s2 == ss2)
        }
        Bson::JavaScriptCode(ref s) => var_match!(*b2, Bson::JavaScriptCode(ref ss) => s == ss),
        Bson::JavaScriptCodeWithScope(ref s, ref doc) => {
            var_match!(*b2, Bson::JavaScriptCodeWithScope(ref ss, ref other_doc) =>
                       s == ss && doc == other_doc)
        }
        Bson::TimeStamp(i) => var_match!(*b2, Bson::TimeStamp(ii) => i == ii),
        Bson::Binary(sub_ty, ref bits) => {
            var_match!(*b2, Bson::Binary(other_sub_ty, ref other_bits) =>
                       sub_ty == other_sub_ty && bits == other_bits)
        }
        Bson::ObjectId(ref bits) => {
            var_match!(*b2, Bson::ObjectId(ref other_bits) =>
                                                bits == other_bits)
        }
        Bson::UtcDatetime(date_time) => {
            var_match!(*b2, Bson::UtcDatetime(other_date_time) =>
                       date_time == other_date_time)
        }
        Bson::Symbol(ref s1) => {
            var_match!(*b2, Bson::Symbol(ref s2) => s1 == s2)
        }
    }
}
