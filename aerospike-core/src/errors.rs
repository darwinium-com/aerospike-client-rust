//! Types functions and macros used for errors, extracted from expanded error-chain
//! and replaced the error-chain internal state with an alternative

use std::error;
use error_chain::*;
use crate::result_code::ResultCode;

/// convenience typename for result
#[allow(missing_docs)]
pub type Result<T> = std::result::Result<T, Error>;

/// Alternative implementation of Error that does not use error-chains automatic backtrace
#[derive(Debug)]
pub struct Error(
    /// The kind of the error.
    pub ErrorKind,
    /// Contains the error chain and the backtrace.
    #[doc(hidden)]
    pub State,
);

impl error_chain::ChainedError for Error {
    type ErrorKind = ErrorKind;

    fn new(kind: ErrorKind, state: error_chain::State) -> Error {
        //panic!("received error chain state, which can include backtrace");
        Error( ErrorKind::Msg("Unexpected backtrace state".to_string()), State { next_error: state.next_error, backtrace: NoInternalBacktrace {}})
            .chain_err(|| kind)
    }

    fn from_kind(kind: Self::ErrorKind) -> Self {
        Self::from_kind(kind)
    }

    fn with_chain<E, K>(error: E, kind: K)
                        -> Self
        where E: ::std::error::Error + Send + 'static,
              K: Into<Self::ErrorKind>
    {
        Self::with_chain(error, kind)
    }

    fn kind(&self) -> &Self::ErrorKind {
        self.kind()
    }

    fn iter(&self) -> error_chain::Iter {
        Iter::new(Some(self))
    }

    fn chain_err<F, EK>(self, error: F) -> Self
        where F: FnOnce() -> EK,
              EK: Into<ErrorKind> {
        self.chain_err(error)
    }

    fn backtrace(&self) -> Option<&error_chain::Backtrace> {
        self.backtrace()
    }

    fn extract_backtrace(_e: &(dyn error::Error + Send + 'static)) -> Option<error_chain::InternalBacktrace> where Self: Sized {
        None
    }
}

#[allow(dead_code)]
impl Error {
    /// Constructs an error from a kind, and uses our alternate state
    pub fn new(kind: ErrorKind, state: State) -> Error {
        Error(
            kind,
            state,
        )
    }

    /// Constructs an error from a kind, and generates a backtrace.
    pub fn from_kind(kind: ErrorKind) -> Error {
        Error(
            kind,
            State::default(),
        )
    }

    /// Constructs a chained error from another error and a kind
    pub fn with_chain<E, K>(error: E, kind: K)
                            -> Error
        where E: ::std::error::Error + Send + 'static,
              K: Into<ErrorKind>
    {
        Error::with_boxed_chain(Box::new(error), kind)
    }

    /// Construct a chained error from another boxed error and a kind
    #[allow(unknown_lints, bare_trait_objects)]
    pub fn with_boxed_chain<K>(error: Box<dyn (::std::error::Error) + Send>, kind: K)
                               -> Error
        where K: Into<ErrorKind>
    {
        Error(
            kind.into(),
            State::new::<Error>(error),
        )
    }

    /// Returns the kind of the error.
    pub fn kind(&self) -> &ErrorKind {
        &self.0
    }

    /// Iterates over the error chain.
    pub fn iter(&self) -> error_chain::Iter {
        error_chain::ChainedError::iter(self)
    }

    /// Returns the backtrace associated with this error.
    pub fn backtrace(&self) -> Option<&error_chain::Backtrace> {
        self.1.backtrace()
    }

    /// Extends the error chain with a new entry.
    pub fn chain_err<F, EK>(self, error: F) -> Error
        where F: FnOnce() -> EK,
              EK: Into<ErrorKind> {
        Error::with_chain(self, Self::from_kind(error().into()))
    }

    /// A short description of the error.
    /// This method is identical to [`Error::description()`](https://doc.rust-lang.org/nightly/std/error/trait.Error.html#tymethod.description)
    pub fn description(&self) -> &str {
        self.0.description()
    }
}

impl ::std::error::Error for Error {
    #[cfg(not(has_error_description_deprecated))]
    fn description(&self) -> &str {
        self.description()
    }

    impl_error_chain_cause_or_source! {
                types {
                    ErrorKind
                }
                foreign_links {
                    Base64 ( ::base64::DecodeError )
                    # [ doc = "Error decoding Base64 encoded value" ] ; InvalidUtf8 ( ::std::str::Utf8Error )
                    # [ doc = "Error interpreting a sequence of u8 as a UTF-8 encoded string." ] ; Io ( ::std::io::Error )
                    # [ doc = "Error during an I/O operation" ] ; MpscRecv ( ::std::sync::mpsc::RecvError )
                    # [ doc = "Error returned from the `recv` function on an MPSC `Receiver`" ] ; ParseAddr ( ::std::net::AddrParseError )
                    # [ doc = "Error parsing an IP or socket address" ] ; ParseInt ( ::std::num::ParseIntError )
                    # [ doc = "Error parsing an integer" ] ; PwHash ( ::pwhash::error::Error )
                    # [ doc = "Error returned while hashing a password for user authentication" ] ;
                }
            }
}

impl ::std::fmt::Display for Error {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::std::fmt::Display::fmt(&self.0, f)
    }
}


#[doc = "Error decoding Base64 encoded value"]
impl From<::base64::DecodeError> for Error {
    fn from(e: ::base64::DecodeError) -> Self {
        Error::from_kind(
            ErrorKind::Base64(e)
        )
    }
}

#[doc = "Error interpreting a sequence of u8 as a UTF-8 encoded string."]
impl From<::std::str::Utf8Error> for Error {
    fn from(e: ::std::str::Utf8Error) -> Self {
        Error::from_kind(
            ErrorKind::InvalidUtf8(e)
        )
    }
}

#[doc = "Error during an I/O operation"]
impl From<::std::io::Error> for Error {
    fn from(e: ::std::io::Error) -> Self {
        Error::from_kind(
            ErrorKind::Io(e)
        )
    }
}

#[doc = "Error returned from the `recv` function on an MPSC `Receiver`"]
impl From<::std::sync::mpsc::RecvError> for Error {
    fn from(e: ::std::sync::mpsc::RecvError) -> Self {
        Error::from_kind(
            ErrorKind::MpscRecv(e)
        )
    }
}

#[doc = "Error parsing an IP or socket address"]
impl From<::std::net::AddrParseError> for Error {
    fn from(e: ::std::net::AddrParseError) -> Self {
        Error::from_kind(
            ErrorKind::ParseAddr(e)
        )
    }
}

#[doc = "Error parsing an integer"]
impl From<::std::num::ParseIntError> for Error {
    fn from(e: ::std::num::ParseIntError) -> Self {
        Error::from_kind(
            ErrorKind::ParseInt(e)
        )
    }
}

#[doc = "Error returned while hashing a password for user authentication"]
impl From<::pwhash::error::Error> for Error {
    fn from(e: ::pwhash::error::Error) -> Self {
        Error::from_kind(
            ErrorKind::PwHash(e)
        )
    }
}

impl From<ErrorKind> for Error {
    fn from(e: ErrorKind) -> Self {
        Error::from_kind(e)
    }
}

impl From<String> for ErrorKind {
    fn from(e: String) -> Self {
        ErrorKind::Msg(e)
    }
}

impl From<&str> for ErrorKind {
    fn from(e: &str) -> Self {
        ErrorKind::Msg(e.to_string())
    }
}

impl From<String> for Error {
    fn from(e: String) -> Self {
        ErrorKind::Msg(e).into()
    }
}

impl From<&str> for Error {
    fn from(e: &str) -> Self {
        ErrorKind::Msg(e.to_string()).into()
    }
}

impl_error_chain_kind! {
            /// The kind of an error.
            # [ derive ( Debug ) ]
            pub enum ErrorKind  {

                # [ doc = "Error decoding Base64 encoded value" ]
                    Base64 ( err : ::base64::DecodeError ) {
                        description ( call_to_deprecated_description ! ( err ) )
                        display ( "{}" , err )
                    } # [ doc = "Error interpreting a sequence of u8 as a UTF-8 encoded string." ]
                    InvalidUtf8 ( err : ::std::str::Utf8Error ) {
                        description ( call_to_deprecated_description ! ( err ) )
                        display ( "{}" , err )
                    } # [ doc = "Error during an I/O operation" ]
                    Io ( err : ::std::io::Error ) {
                        description ( call_to_deprecated_description ! ( err ) )
                        display ( "{}" , err )
                    } # [ doc = "Error returned from the `recv` function on an MPSC `Receiver`" ]
                    MpscRecv ( err : ::std::sync::mpsc::RecvError ) {
                        description ( call_to_deprecated_description ! ( err ) )
                        display ( "{}" , err )
                    } # [ doc = "Error parsing an IP or socket address" ]
                    ParseAddr ( err : ::std::net::AddrParseError ) {
                        description ( call_to_deprecated_description ! ( err ) )
                        display ( "{}" , err )
                    } # [ doc = "Error parsing an integer" ]
                    ParseInt ( err : ::std::num::ParseIntError ) {
                        description ( call_to_deprecated_description ! ( err ) )
                        display ( "{}" , err )
                    } # [ doc = "Error returned while hashing a password for user authentication" ]
                    PwHash ( err : ::pwhash::error::Error ) {
                        description ( call_to_deprecated_description ! ( err ) )
                        display ( "{}" , err )
                    }

                #[doc=" A convenient variant for String."]

                Msg ( s : String ) {
                    description ( & s )
                    display ( "{}" , s )
                }

                #[doc=" The client received a server response that it was not able to process."]

        BadResponse(details: String) {
            description("Bad Server Response")
            display("Bad Server Response: {}", details)
        }

#[doc=" The client was not able to communicate with the cluster due to some issue with the"]
#[doc=" network connection."]

        Connection(details: String) {
            description("Network Connection Issue")
            display("Unable to communicate with server cluster: {}", details)
        }

#[doc=" One or more of the arguments passed to the client are invalid."]

        InvalidArgument(details: String) {
            description("Invalid Argument")
            display("Invalid argument: {}", details)
        }

#[doc=" Cluster node is invalid."]

        InvalidNode(details: String) {
            description("Invalid cluster node")
            display("Invalid cluster node: {}", details)
        }

#[doc=" Exceeded max. number of connections per node."]

        NoMoreConnections {
            description("Too many connections")
            display("Too many connections")
        }

#[doc=" Server responded with a response code indicating an error condition."]

        ServerError(rc: ResultCode) {
            description("Server Error")
            display("Server error: {}", rc.into_string())
        }

#[doc=" Error returned when executing a User-Defined Function (UDF) resulted in an error."]

        UdfBadResponse(details: String) {
            description("UDF Bad Response")
            display("UDF Bad Response: {}", details)
        }

#[doc=" Error returned when a tasked timeed out before it could be completed."]

        Timeout(details: String) {
            description("Timeout")
            display("Timeout: {}", details)
        }

    }
}



impl From<Error> for ErrorKind {
    fn from(e: Error) -> Self {
        e.0
    }
}


/// Additional methods for `Result`, for easy interaction with this crate.
pub trait ResultExt<T> {
    /// If the `Result` is an `Err` then `chain_err` evaluates the closure,
    /// which returns *some type that can be converted to `ErrorKind`*, boxes
    /// the original error to store as the cause, then returns a new error
    /// containing the original error.
    fn chain_err<F, EK>(self, callback: F) -> ::std::result::Result<T, Error>
        where F: FnOnce() -> EK,
              EK: Into<ErrorKind>;
}

impl<T, E> ResultExt<T> for ::std::result::Result<T, E> where E: ::std::error::Error + Send + 'static {
    fn chain_err<F, EK>(self, callback: F) -> ::std::result::Result<T, Error>
        where F: FnOnce() -> EK,
              EK: Into<ErrorKind> {
        self.map_err(move |e| {
            let state = State::new::<Error>(Box::new(e));
            Error::new(callback().into(), state)
        })
    }
}

impl<T> ResultExt<T> for ::std::option::Option<T> {
    fn chain_err<F, EK>(self, callback: F) -> ::std::result::Result<T, Error>
        where F: FnOnce() -> EK,
              EK: Into<ErrorKind> {
        self.ok_or_else(move || {
            Error::from_kind(callback().into())
        })
    }
}

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct NoInternalBacktrace {}

#[derive(Debug)]
#[doc(hidden)]
#[allow(unknown_lints, bare_trait_objects)]
pub struct State {
    /// Next error in the error chain.
    pub next_error: Option<Box<dyn error::Error + Send>>,
    /// Backtrace for the current error.
    pub backtrace: NoInternalBacktrace,
}

impl Default for State {
    fn default() -> State {
        State {
            next_error: None,
            backtrace: NoInternalBacktrace {},
        }
    }
}

impl State {
    /// Creates a new State type
    #[allow(unknown_lints, bare_trait_objects)]
    pub fn new<CE: ChainedError>(e: Box<dyn error::Error + Send>) -> State {
        State {
            next_error: Some(e),
            backtrace: NoInternalBacktrace {},
        }
    }

    /// Returns the inner backtrace if present.
    pub fn backtrace(&self) -> Option<&Backtrace> {
        None
    }
}


macro_rules! log_error_chain {
    ($err:expr, $($arg:tt)*) => {
        error!($($arg)*);
        error!("Error: {}", $err);
        for e in $err.iter().skip(1) {
            error!("caused by: {}", e);
        }
        if let Some(backtrace) = $err.backtrace() {
            error!("backtrace: {:?}", backtrace);
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ad_lib() {
        fn result_check() -> Result<()> {
            bail!(ErrorKind::BadResponse("Missing replicas info".to_string()));
        }

        // test a few ways that errors are created
        let r = result_check();
        assert!(r.is_err());
        assert!(r.unwrap_err().backtrace().is_none());
        let r: Result<()> = Err(ErrorKind::ServerError(ResultCode::BatchDisabled).into());
        assert!(r.is_err());
        let r2 = r.chain_err(|| ErrorKind::InvalidArgument(format!("Invalid hosts list: '{}'", "boohoo")));
        assert!(r2.is_err());

        let e1 = Error::from(ErrorKind::InvalidArgument("this is not good".to_string()));
        let e2 = e1
            .chain_err(|| ErrorKind::Msg("chained msg".to_string()))
            .chain_err(|| ErrorKind::BadResponse("resp".to_string()));
        println!("as debug format {:?}", &e2);
        println!("as string {}", &e2);
        let sum = e2.iter().count();
        assert_eq!(sum, 3);

        // explicitly create the backtrace state
        let state = ::error_chain::State {
            next_error: Some(Box::new(Error::from(ErrorKind::Msg("World".to_string())))),
            backtrace: InternalBacktrace::new()
        };
        // explicitly use it, our options here are to panic or include an additional error in the chain
        let e1: Error = error_chain::ChainedError::new(ErrorKind::Msg("Hi".to_string()), state);
        println!("as debug format {:?}", &e1);
        let sum = e2.iter().count();
        assert_eq!(sum, 3);
    }
}
