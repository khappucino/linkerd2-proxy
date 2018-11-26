use http;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_timer::clock;
use tower_retry::budget::Budget;

use proxy::http::{metrics::classify::CanClassify, profiles, retry};
use {Addr, NameAddr};

use super::classify;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Direction {
    In,
    Out,
}

#[derive(Clone, Debug)]
pub struct Route {
    pub dst_addr: DstAddr,
    pub route: profiles::Route,
}

#[derive(Clone, Debug)]
pub struct Retry {
    budget: Arc<Budget>,
    response_classes: profiles::ResponseClasses,
    timeout: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DstAddr {
    addr: Addr,
    direction: Direction,
}

// === impl Route ===

impl CanClassify for Route {
    type Classify = classify::Request;

    fn classify(&self) -> classify::Request {
        self.route.response_classes().clone().into()
    }
}

impl retry::CanRetry for Route {
    type Retry = Retry;

    fn can_retry(&self) -> Option<Self::Retry> {
        if self.route.is_retryable() {
            let timeout = self.route.retry_timeout()?;
            self
                .route
                .retry_budget()
                .map(|budget| Retry {
                    budget: budget.clone(),
                    response_classes: self.route.response_classes().clone(),
                    timeout,
                })
        } else {
            None
        }
    }
}

// === impl Retry ===

impl retry::Retry for Retry {
    fn retry<B>(&self, started_at: Instant, res: &http::Response<B>) -> Result<(), retry::NoRetry> {
        if clock::now() - started_at > self.timeout {
            return Err(retry::NoRetry::Timeout);
        }

        for class in &*self.response_classes {
            if class.is_match(res) {
                if class.is_failure() {
                    // don't break through and deposit on a failure
                    return self
                        .budget
                        .withdraw()
                        .map_err(|_overdrawn| retry::NoRetry::Budget);
                }
                break;
            }
        }

        self.budget.deposit();
        Err(retry::NoRetry::Success)
    }
}

// === impl DstAddr ===

impl AsRef<Addr> for DstAddr {
    fn as_ref(&self) -> &Addr {
        &self.addr
    }
}

impl DstAddr {
    pub fn outbound(addr: Addr) -> Self {
        DstAddr { addr, direction: Direction::Out }
    }

    pub fn inbound(addr: Addr) -> Self {
        DstAddr { addr, direction: Direction::In }
    }

    pub fn direction(&self) -> Direction {
        self.direction
    }
}

impl<'t> From<&'t DstAddr> for http::header::HeaderValue {
    fn from(a: &'t DstAddr) -> Self {
        http::header::HeaderValue::from_str(&format!("{}", a))
            .expect("addr must be a valid header")
    }
}

impl fmt::Display for DstAddr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.addr.fmt(f)
    }
}

impl profiles::CanGetDestination for DstAddr {
    fn get_destination(&self) -> Option<&NameAddr> {
        self.addr.name_addr()
    }
}

impl profiles::WithRoute for DstAddr {
    type Output = Route;

    fn with_route(self, route: profiles::Route) -> Self::Output {
        Route {
            dst_addr: self,
            route,
        }
    }
}
