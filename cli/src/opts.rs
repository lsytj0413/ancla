use clap::Args;
use clap_verbosity_flag::{LogLevel, VerbosityFilter};
use cling::prelude::*;

#[derive(Args, Collect, Clone, Default)]
pub struct CommonOpts {
    #[clap(flatten)]
    pub(crate) verbose: clap_verbosity_flag::Verbosity<Quiet>,

    #[arg(long)]
    pub(crate) db: String,
}

#[derive(Clone, Default)]
pub(crate) struct Quiet;
impl LogLevel for Quiet {
    fn default_filter() -> VerbosityFilter {
        VerbosityFilter::Error
    }

    fn verbose_long_help() -> Option<&'static str> {
        None
    }

    fn quiet_help() -> Option<&'static str> {
        None
    }

    fn quiet_long_help() -> Option<&'static str> {
        None
    }
}
