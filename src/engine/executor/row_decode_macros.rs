// Reject unsupported typed lanes without allocating fallback Value envelopes.
macro_rules! reject_decode_lanes {
    ($reject:ident; $($lane:ident),+ $(,)?) => {
        $(reject_decode_lanes!(@lane $reject; $lane);)+
    };
    (@lane $reject:ident; put_i64) => {
        fn put_i64(&mut self, _col: usize, _value0: i64) { self.$reject(); }
    };
    (@lane $reject:ident; put_u64) => {
        fn put_u64(&mut self, _col: usize, _value0: u64) { self.$reject(); }
    };
    (@lane $reject:ident; put_f32) => {
        fn put_f32(&mut self, _col: usize, _value0: f32) { self.$reject(); }
    };
    (@lane $reject:ident; put_f64) => {
        fn put_f64(&mut self, _col: usize, _value0: f64) { self.$reject(); }
    };
    (@lane $reject:ident; put_decimal_compact) => {
        fn put_decimal_compact(&mut self, _col: usize, _value0: i64, _value1: u8) { self.$reject(); }
    };
    (@lane $reject:ident; put_decimal_wide) => {
        fn put_decimal_wide(&mut self, _col: usize, _value0: i128, _value1: u8) { self.$reject(); }
    };
    (@lane $reject:ident; put_date) => {
        fn put_date(&mut self, _col: usize, _value0: i32) { self.$reject(); }
    };
    (@lane $reject:ident; put_year) => {
        fn put_year(&mut self, _col: usize, _value0: u8) { self.$reject(); }
    };
    (@lane $reject:ident; put_time) => {
        fn put_time(&mut self, _col: usize, _value0: i64) { self.$reject(); }
    };
    (@lane $reject:ident; put_datetime) => {
        fn put_datetime(&mut self, _col: usize, _value0: i64) { self.$reject(); }
    };
    (@lane $reject:ident; put_bytes) => {
        fn put_bytes(&mut self, _col: usize, _value0: &[u8]) { self.$reject(); }
    };
    (@lane $reject:ident; put_string) => {
        fn put_string(&mut self, _col: usize, _value0: &[u8]) { self.$reject(); }
    };
    (@lane $reject:ident; put_json) => {
        fn put_json(&mut self, _col: usize, _value0: &[u8]) { self.$reject(); }
    };
    (@lane $reject:ident; put_geometry) => {
        fn put_geometry(&mut self, _col: usize, _value0: &[u8]) { self.$reject(); }
    };
    (@lane $reject:ident; put_vector) => {
        fn put_vector(&mut self, _col: usize, _value0: &[u8]) { self.$reject(); }
    };
    (@lane $reject:ident; put_value) => {
        fn put_value(&mut self, _col: usize, _value0: crate::types::Value) { self.$reject(); }
    };
}
