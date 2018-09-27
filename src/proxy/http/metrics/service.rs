use futures::{Async, Future, Poll};
use h2;
use http;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio_timer::clock;
use tower_h2;

use super::super::{Classify, ClassifyResponse};
use super::{ClassMetrics, Metrics, Registry};
use svc;

/// A stack module that wraps services to record metrics.
#[derive(Debug)]
pub struct Layer<T, M, C>
where
    T: Clone + Hash + Eq,
    M: svc::Make<T>,
    M::Value: svc::Service,
    C: Classify<Error = <M::Value as svc::Service>::Error>,
    C::Class: Hash + Eq,
{
    registry: Arc<Mutex<Registry<T, C::Class>>>,
    _p: PhantomData<fn() -> (M)>,
}

/// Wraps services to record metrics.
#[derive(Debug)]
pub struct Make<T, M, C>
where
    T: Clone + Hash + Eq,
    M: svc::Make<T>,
    M::Value: svc::Service,
    C: Classify<Error = <M::Value as svc::Service>::Error>,
    C::Class: Hash + Eq,
{
    registry: Arc<Mutex<Registry<T, C::Class>>>,
    inner: M,
}

/// A middleware that records HTTP metrics.
#[derive(Clone, Debug)]
pub struct Measure<S, C>
where
    S: svc::Service,
    C: Classify<Error = S::Error>,
    C::Class: Hash + Eq,
{
    metrics: Option<Arc<Mutex<Metrics<C::Class>>>>,
    inner: S,
}

pub struct ResponseFuture<S, C>
where
    S: svc::Service<Error = C::Error>,
    C: ClassifyResponse,
    C::Class: Hash + Eq,
{
    classify: Option<C>,
    metrics: Option<Arc<Mutex<Metrics<C::Class>>>>,
    stream_open_at: Instant,
    inner: S::Future,
}

#[derive(Debug)]
pub struct RequestBody<B, C>
where
    B: tower_h2::Body,
    C: Hash + Eq,
{
    metrics: Option<Arc<Mutex<Metrics<C>>>>,
    inner: B,
}

#[derive(Debug)]
pub struct ResponseBody<B, C>
where
    B: tower_h2::Body,
    C: ClassifyResponse<Error = h2::Error>,
    C::Class: Hash + Eq,
{
    class_at_first_byte: Option<C::Class>,
    classify: Option<C>,
    metrics: Option<Arc<Mutex<Metrics<C::Class>>>>,
    stream_open_at: Instant,
    first_byte_at: Option<Instant>,
    inner: B,
}

// ===== impl Make =====

impl<T, M, C, A, B> Layer<T, M, C>
where
    T: Clone + Hash + Eq,
    M: svc::Make<T>,
    M::Value: svc::Service<
        Request = http::Request<RequestBody<A, C::Class>>,
        Response = http::Response<B>,
        Error = h2::Error,
    >,
    A: tower_h2::Body,
    B: tower_h2::Body,
    C: Classify<Error = h2::Error>,
    C::Class: Hash + Eq,
    C::ClassifyResponse: Send + Sync + 'static,
{
    pub(super) fn new(registry: Arc<Mutex<Registry<T, C::Class>>>) -> Self {
        Self {
            registry,
            _p: PhantomData,
        }
    }
}

impl<T, M, A, B, C> svc::Layer<T, T, M> for Layer<T, M, C>
where
    T: Clone + Hash + Eq,
    M: svc::Make<T>,
    M::Value: svc::Service<
        Request = http::Request<RequestBody<A, C::Class>>,
        Response = http::Response<B>,
        Error = h2::Error,
    >,
    A: tower_h2::Body,
    B: tower_h2::Body,
    C: Classify<Error = h2::Error>,
    C::ClassifyResponse: Debug + Send + Sync + 'static,
    C::Class: Hash + Eq,
{
    type Value = <Make<T, M, C> as svc::Make<T>>::Value;
    type Error = M::Error;
    type Make = Make<T, M, C>;

    fn bind(&self, inner: M) -> Self::Make {
        Make {
            registry: self.registry.clone(),
            inner,
        }
    }
}

// ===== impl Make =====

impl<T, M, A, B, C> svc::Make<T> for Make<T, M, C>
where
    T: Clone + Hash + Eq,
    M: svc::Make<T>,
    M::Value: svc::Service<
        Request = http::Request<RequestBody<A, C::Class>>,
        Response = http::Response<B>,
        Error = C::Error,
    >,
    A: tower_h2::Body,
    B: tower_h2::Body,
    C: Classify<Error = h2::Error>,
    C::Class: Hash + Eq,
    C::ClassifyResponse: Debug + Send + Sync + 'static,
{
    type Value = Measure<M::Value, C>;
    type Error = M::Error;

    fn make(&self, target: &T) -> Result<Self::Value, Self::Error> {
        let inner = self.inner.make(target)?;

        let metrics = match self.registry.lock() {
            Ok(mut r) => Some(
                r.by_target
                    .entry(target.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(Metrics::default())))
                    .clone(),
            ),
            Err(_) => None,
        };

        Ok(Measure { metrics, inner })
    }
}

// ===== impl Measure =====

impl<C, S, A, B> svc::Service for Measure<S, C>
where
    S: svc::Service<
        Request = http::Request<RequestBody<A, C::Class>>,
        Response = http::Response<B>,
        Error = h2::Error,
    >,
    A: tower_h2::Body,
    B: tower_h2::Body,
    C: Classify<Error = h2::Error>,
    C::Class: Hash + Eq,
    C::ClassifyResponse: Debug + Send + Sync + 'static,
{
    type Request = http::Request<A>;
    type Response = http::Response<ResponseBody<B, C::ClassifyResponse>>;
    type Error = S::Error;
    type Future = ResponseFuture<S, C::ClassifyResponse>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.inner.poll_ready()
    }

    fn call(&mut self, req: Self::Request) -> Self::Future {
        let mut req_metrics = self.metrics.clone();

        if req.body().is_end_stream() {
            if let Some(lock) = req_metrics.take() {
                if let Ok(mut metrics) = lock.lock() {
                    (*metrics).total.incr();
                }
            }
        }

        let req = {
            let (head, inner) = req.into_parts();
            let body = RequestBody {
                metrics: req_metrics,
                inner,
            };
            http::Request::from_parts(head, body)
        };

        ResponseFuture {
            classify: req.extensions().get::<C::ClassifyResponse>().cloned(),
            metrics: self.metrics.clone(),
            stream_open_at: clock::now(),
            inner: self.inner.call(req),
        }
    }
}

impl<C, S, B> Future for ResponseFuture<S, C>
where
    S: svc::Service<Response = http::Response<B>, Error = h2::Error>,
    B: tower_h2::Body,
    C: ClassifyResponse<Error = h2::Error> + Debug + Send + Sync + 'static,
    C::Class: Hash + Eq,
{
    type Item = http::Response<ResponseBody<B, C>>;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let (head, inner) = try_ready!(self.inner.poll()).into_parts();

        let mut classify = self.classify.take();
        let class_at_first_byte = classify.as_mut().and_then(|c| c.start(&head));

        let body = ResponseBody {
            classify,
            class_at_first_byte,
            metrics: self.metrics.clone(),
            stream_open_at: self.stream_open_at,
            first_byte_at: None,
            inner,
        };
        let rsp = http::Response::from_parts(head, body);

        Ok(rsp.into())
    }
}

impl<B, C> tower_h2::Body for RequestBody<B, C>
where
    B: tower_h2::Body,
    C: Hash + Eq,
{
    type Data = B::Data;

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn poll_data(&mut self) -> Poll<Option<Self::Data>, h2::Error> {
        let frame = try_ready!(self.inner.poll_data());

        if let Some(lock) = self.metrics.take() {
            if let Ok(mut metrics) = lock.lock() {
                (*metrics).total.incr();
            }
        }

        Ok(Async::Ready(frame))
    }

    fn poll_trailers(&mut self) -> Poll<Option<http::HeaderMap>, h2::Error> {
        self.inner.poll_trailers()
    }
}

impl<B, C> Drop for RequestBody<B, C>
where
    B: tower_h2::Body,
    C: Hash + Eq,
{
    fn drop(&mut self) {}
}

impl<B, C> ResponseBody<B, C>
where
    B: tower_h2::Body,
    C: ClassifyResponse<Error = h2::Error>,
    C::Class: Hash + Eq,
{
    fn record_class(&mut self, class: Option<C::Class>) {
        let lock = match self.metrics.take() {
            Some(lock) => lock,
            None => return,
        };
        let mut metrics = match lock.lock() {
            Ok(m) => m,
            Err(_) => return,
        };

        let first_byte_at = self.first_byte_at.unwrap_or_else(|| clock::now());
        let class_metrics = match class {
            Some(c) => metrics
                .by_class
                .entry(c)
                .or_insert_with(|| ClassMetrics::default()),
            None => &mut metrics.unclassified,
        };
        class_metrics.total.incr();
        class_metrics
            .latency
            .add(first_byte_at - self.stream_open_at);
    }

    fn measure_err(&mut self, err: C::Error) -> C::Error {
        self.class_at_first_byte = None;
        let c = self.classify.take().map(|mut c| c.error(&err));
        self.record_class(c);
        err
    }
}

impl<B, C> tower_h2::Body for ResponseBody<B, C>
where
    B: tower_h2::Body,
    C: ClassifyResponse<Error = h2::Error>,
    C::Class: Hash + Eq,
{
    type Data = B::Data;

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn poll_data(&mut self) -> Poll<Option<Self::Data>, h2::Error> {
        let poll = self.inner.poll_data().map_err(|e| self.measure_err(e));
        let frame = try_ready!(poll);

        if self.first_byte_at.is_none() {
            self.first_byte_at = Some(clock::now());
        }

        if let c @ Some(_) = self.class_at_first_byte.take() {
            self.record_class(c);
        }

        Ok(Async::Ready(frame))
    }

    fn poll_trailers(&mut self) -> Poll<Option<http::HeaderMap>, h2::Error> {
        let trls = try_ready!(self.inner.poll_trailers().map_err(|e| self.measure_err(e)));

        let c = self.classify.take().map(|mut c| c.eos(trls.as_ref()));
        self.record_class(c);

        Ok(Async::Ready(trls))
    }
}

impl<B, C> Drop for ResponseBody<B, C>
where
    B: tower_h2::Body,
    C: ClassifyResponse<Error = h2::Error>,
    C::Class: Hash + Eq,
{
    fn drop(&mut self) {
        let c = self.classify.take().map(|mut c| c.eos(None));
        self.record_class(c);
    }
}