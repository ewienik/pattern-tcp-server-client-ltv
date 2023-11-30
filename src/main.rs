use {
    crate::proto::Packet,
    anyhow::anyhow,
    bytes::BytesMut,
    futures::{Sink, SinkExt, Stream, StreamExt},
    tokio::{
        io::{AsyncRead, AsyncWrite},
        net::{TcpListener, TcpStream},
    },
    tokio_util::codec::LengthDelimitedCodec,
};

mod proto {
    use {
        anyhow::bail,
        bytes::{Buf, BufMut, BytesMut},
    };

    const HEADER_SIZE: usize = 3;
    const PAYLOAD_OFFSET: usize = HEADER_SIZE;

    const REQUEST_SIZE: u16 = 7;
    const REQUEST_TYPE: u8 = 1;
    const REQUEST_OFFSET_LEFT: usize = PAYLOAD_OFFSET;
    const REQUEST_OFFSET_RIGHT: usize = REQUEST_OFFSET_LEFT + 2;

    const RESPONSE_SIZE: u16 = 5;
    const RESPONSE_TYPE: u8 = 2;
    const RESPONSE_OFFSET_VALUE: usize = PAYLOAD_OFFSET;

    fn header(mut buf: &[u8]) -> anyhow::Result<(u16, u8)> {
        let remaining = buf.len();
        if HEADER_SIZE > remaining {
            bail!(
                "too small buffer for Packet Header: expected {HEADER_SIZE}, provided {remaining}"
            );
        };
        let pkt_size = buf.get_u16_le();
        if pkt_size as usize > remaining {
            bail!("too small Packet size: expected {pkt_size}, provided {remaining}");
        };
        Ok((pkt_size, buf.get_u8()))
    }

    pub struct Request<T> {
        inner: T,
    }

    impl<T> Request<T> {
        pub fn into_inner(self) -> T {
            self.inner
        }
    }

    impl<T: AsRef<[u8]>> Request<T> {
        pub fn view(inner: T) -> anyhow::Result<Self> {
            let (pkt_size, pkt_type) = header(inner.as_ref())?;
            if REQUEST_SIZE != pkt_size {
                bail!("wrong packet size Request: expected {REQUEST_SIZE}, provided {pkt_size}");
            };
            if REQUEST_TYPE != pkt_type {
                bail!("wrong packet type Request: expected {REQUEST_TYPE}, provided {pkt_type}");
            };
            Ok(Self { inner })
        }

        pub fn pkt_size(&self) -> u16 {
            REQUEST_SIZE
        }

        pub fn left(&self) -> u16 {
            (&(self.inner.as_ref()[REQUEST_OFFSET_LEFT..])).get_u16_le()
        }

        pub fn right(&self) -> u16 {
            (&(self.inner.as_ref()[REQUEST_OFFSET_RIGHT..])).get_u16_le()
        }
    }

    impl Request<BytesMut> {
        pub fn new(inner: &mut BytesMut, left: u16, right: u16) -> Self {
            assert!(inner.is_empty());
            inner.put_request(left, right);
            Self {
                inner: inner.split(),
            }
        }
    }

    impl TryFrom<BytesMut> for Request<BytesMut> {
        type Error = anyhow::Error;

        fn try_from(mut inner: BytesMut) -> anyhow::Result<Self> {
            let view = Request::view(&inner)?;
            Ok(Self {
                inner: inner.split_to(view.pkt_size() as usize),
            })
        }
    }

    pub struct Response<T> {
        inner: T,
    }

    impl<T> Response<T> {
        pub fn into_inner(self) -> T {
            self.inner
        }
    }

    impl<T: AsRef<[u8]>> Response<T> {
        pub fn view(inner: T) -> anyhow::Result<Self> {
            let (pkt_size, pkt_type) = header(inner.as_ref())?;
            if RESPONSE_SIZE != pkt_size {
                bail!("wrong packet size Response: expected {RESPONSE_SIZE}, provided {pkt_size}");
            };
            if RESPONSE_TYPE != pkt_type {
                bail!("wrong packet type Response: expected {RESPONSE_TYPE}, provided {pkt_type}");
            };
            Ok(Self { inner })
        }

        pub fn pkt_size(&self) -> u16 {
            RESPONSE_SIZE
        }

        pub fn value(&self) -> u16 {
            (&(self.inner.as_ref()[RESPONSE_OFFSET_VALUE..])).get_u16_le()
        }
    }

    impl Response<BytesMut> {
        pub fn new(inner: &mut BytesMut, value: u16) -> Self {
            assert!(inner.is_empty());
            inner.put_response(value);
            Self {
                inner: inner.split(),
            }
        }
    }

    impl TryFrom<BytesMut> for Response<BytesMut> {
        type Error = anyhow::Error;

        fn try_from(mut inner: BytesMut) -> anyhow::Result<Self> {
            let view = Response::view(&inner)?;
            Ok(Self {
                inner: inner.split_to(view.pkt_size() as usize),
            })
        }
    }

    pub trait BufMutExt: BufMut + Sized {
        fn put_request(&mut self, left: u16, right: u16) {
            self.put_u16_le(REQUEST_SIZE);
            self.put_u8(REQUEST_TYPE);
            self.put_u16_le(left);
            self.put_u16_le(right);
        }

        fn put_response(&mut self, value: u16) {
            self.put_u16_le(RESPONSE_SIZE);
            self.put_u8(RESPONSE_TYPE);
            self.put_u16_le(value);
        }
    }

    impl BufMutExt for BytesMut {}

    pub enum Packet<T> {
        Request(Request<T>),
        Response(Response<T>),
    }

    impl<T> Packet<T> {
        pub fn into_inner(self) -> T {
            match self {
                Self::Request(request) => request.into_inner(),
                Self::Response(response) => response.into_inner(),
            }
        }
    }

    impl Packet<BytesMut> {
        pub fn new_request(inner: &mut BytesMut, left: u16, right: u16) -> Self {
            Self::Request(Request::new(inner, left, right))
        }
        pub fn new_response(inner: &mut BytesMut, value: u16) -> Self {
            Self::Response(Response::new(inner, value))
        }
    }

    impl TryFrom<BytesMut> for Packet<BytesMut> {
        type Error = anyhow::Error;

        fn try_from(inner: BytesMut) -> anyhow::Result<Self> {
            if let Ok(request) = Request::try_from(inner.clone()) {
                return Ok(Self::Request(request));
            };
            if let Ok(response) = Response::try_from(inner) {
                return Ok(Self::Response(response));
            };
            bail!("Unknown packet");
        }
    }
}

fn wrap_with_decoder<T: AsyncRead>(io: T) -> impl Stream<Item = anyhow::Result<Packet<BytesMut>>> {
    LengthDelimitedCodec::builder()
        .little_endian()
        .length_field_offset(0)
        .length_field_type::<u16>()
        .length_adjustment(0)
        .num_skip(0)
        .new_read(io)
        .map(|inner| Packet::try_from(inner?))
}

fn wrap_with_encoder<T: AsyncWrite>(io: T) -> impl Sink<Packet<BytesMut>> + Unpin {
    Box::pin(
        LengthDelimitedCodec::builder()
            .little_endian()
            .length_field_offset(0)
            .length_field_type::<u16>()
            .length_adjustment(-2)
            .new_write(io)
            .with(|pkt: Packet<BytesMut>| async move {
                anyhow::Ok(pkt.into_inner().split_off(2).freeze())
            }),
    )
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_join = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let (read, write) = socket.split();
        let mut sink = wrap_with_encoder(write);
        let mut stream = wrap_with_decoder(read);
        let Packet::Request(request) = stream.next().await.unwrap().unwrap() else {
            unreachable!();
        };
        let mut buf = BytesMut::new();
        sink.send(Packet::new_response(
            &mut buf,
            request.left() + request.right(),
        ))
        .await
        .map_err(|_| anyhow!("unable to send by server"))
        .unwrap();
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    let (read, write) = client.split();
    let mut sink = wrap_with_encoder(write);
    let mut stream = wrap_with_decoder(read);
    let mut buf = BytesMut::new();
    sink.send(Packet::new_request(&mut buf, 100, 200))
        .await
        .map_err(|_| anyhow!("unable to send by client"))
        .unwrap();
    let Packet::Response(response) = stream.next().await.unwrap().unwrap() else {
        unreachable!();
    };
    assert_eq!(response.value(), 300);
    server_join.await.unwrap();
}
