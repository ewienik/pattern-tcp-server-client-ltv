use {
    crate::proto::{Packet, ProtoDeserialize},
    anyhow::anyhow,
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
        bytes::{Buf, BufMut, Bytes, BytesMut},
    };

    #[derive(Debug)]
    pub enum Packet {
        Request { left: u16, right: u16 },
        Response { value: u16 },
    }

    pub trait ProtoSize {
        fn proto_size(&self) -> usize;
    }

    pub trait ProtoSerialize<T> {
        fn proto_serialize(&self, dst: T) -> anyhow::Result<()>;
    }

    pub trait ProtoDeserialize<T>: Sized {
        fn proto_deserialize(dst: T) -> anyhow::Result<Self>;
    }

    impl ProtoSize for Packet {
        fn proto_size(&self) -> usize {
            match self {
                Self::Request { .. } => Self::REQUEST_SIZE,
                Self::Response { .. } => Self::RESPONSE_SIZE,
            }
        }
    }

    impl<T: AsMut<[u8]>> ProtoSerialize<T> for Packet {
        fn proto_serialize(&self, mut dst: T) -> anyhow::Result<()> {
            let mut dst = dst.as_mut();
            let pkt_size = self.proto_size();
            if pkt_size > dst.len() {
                bail!(
                    "too small dst: required {pkt_size}, remaining {}",
                    dst.len()
                );
            }
            dst.put_u16_le(pkt_size as u16);
            match self {
                Packet::Request { left, right } => {
                    dst.put_u8(Self::REQUEST_TYPE);
                    dst.put_u16_le(*left);
                    dst.put_u16_le(*right);
                }
                Packet::Response { value } => {
                    dst.put_u8(Self::RESPONSE_TYPE);
                    dst.put_u16_le(*value);
                }
            }
            Ok(())
        }
    }

    impl<T: AsRef<[u8]>> ProtoDeserialize<T> for Packet {
        fn proto_deserialize(src: T) -> anyhow::Result<Self> {
            let mut src = src.as_ref();
            let remaining = src.len();
            if Packet::HEADER_SIZE > remaining {
                bail!(
                    "too small Buf for header: required {}, remaining {remaining}",
                    Packet::HEADER_SIZE,
                );
            };
            let pkt_size = src.get_u16_le();
            if pkt_size as usize > remaining {
                bail!("too small Buf for packet: required {pkt_size}, remaining {remaining}",);
            };
            Ok(match src.get_u8() {
                Self::REQUEST_TYPE => Packet::Request {
                    left: src.get_u16_le(),
                    right: src.get_u16_le(),
                },
                Self::RESPONSE_TYPE => Packet::Response {
                    value: src.get_u16_le(),
                },
                pkt_type => bail!("unknown packet type {pkt_type}"),
            })
        }
    }

    impl Packet {
        pub const HEADER_SIZE: usize = 3;

        pub const REQUEST_SIZE: usize = 7;
        pub const REQUEST_TYPE: u8 = 1;

        pub const RESPONSE_SIZE: usize = 5;
        pub const RESPONSE_TYPE: u8 = 2;

        pub fn to_bytes(&self) -> anyhow::Result<Bytes> {
            let mut buf = BytesMut::zeroed(self.proto_size());
            self.proto_serialize(&mut buf)?;
            Ok(buf.into())
        }
    }
}

fn wrap_with_decoder<T: AsyncRead>(io: T) -> impl Stream<Item = anyhow::Result<Packet>> {
    LengthDelimitedCodec::builder()
        .little_endian()
        .length_field_offset(0)
        .length_field_type::<u16>()
        .length_adjustment(0)
        .num_skip(0)
        .new_read(io)
        .map(|buf| Packet::proto_deserialize(buf?))
}

fn wrap_with_encoder<T: AsyncWrite>(io: T) -> impl Sink<Packet> + Unpin {
    Box::pin(
        LengthDelimitedCodec::builder()
            .little_endian()
            .length_field_offset(0)
            .length_field_type::<u16>()
            .length_adjustment(-2)
            .new_write(io)
            .with(|pkt: Packet| async move { pkt.to_bytes().map(|buf| buf.slice(2..)) }),
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
        let Packet::Request { left, right } = stream.next().await.unwrap().unwrap() else {
            unreachable!();
        };
        sink.send(Packet::Response {
            value: left + right,
        })
        .await
        .map_err(|_| anyhow!("unable to send by server"))
        .unwrap();
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    let (read, write) = client.split();
    let mut sink = wrap_with_encoder(write);
    let mut stream = wrap_with_decoder(read);
    sink.send(Packet::Request {
        left: 100,
        right: 200,
    })
    .await
    .map_err(|_| anyhow!("unable to send by client"))
    .unwrap();
    let Packet::Response { value } = stream.next().await.unwrap().unwrap() else {
        unreachable!();
    };
    assert_eq!(value, 300);
    server_join.await.unwrap();
}
