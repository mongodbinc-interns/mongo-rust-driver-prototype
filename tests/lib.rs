// Clippy lints
#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]
#![cfg_attr(feature = "clippy", allow(
    doc_markdown,
    // allow double_parens for bson/doc macro.
    double_parens,
    // more explicit than catch-alls.
    match_wild_err_arm,
    too_many_arguments,
))]
#![cfg_attr(all(test, feature = "clippy"), allow(
    large_enum_variant,
    print_stdout,
    result_unwrap_used
))]
#![cfg_attr(feature = "clippy", warn(
    cast_precision_loss,
    enum_glob_use,
    filter_map,
    if_not_else,
    invalid_upcast_comparisons,
    items_after_statements,
    mem_forget,
    mut_mut,
    mutex_integer,
    non_ascii_literal,
    nonminimal_bool,
    option_map_unwrap_or,
    option_map_unwrap_or_else,
    shadow_reuse,
    shadow_same,
    shadow_unrelated,
    similar_names,
    unicode_not_nfc,
    unseparated_literal_suffix,
    used_underscore_binding,
    wrong_pub_self_convention,
))]

#[macro_use(bson, doc)]
extern crate bson;
extern crate mongodb;
extern crate nalgebra;
extern crate rand;
extern crate serde_json;

mod apm;
mod auth;
mod client;
mod json;
mod sdam;
mod server_selection;
#[cfg(feature = "ssl")]
mod ssl;
