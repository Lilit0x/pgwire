use std::process::Command;
use std::str;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, StreamExt};
use tokio::net::TcpListener;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, Response, QueryResponse};
use pgwire::api::{ClientInfo, MakeHandler, StatelessMakeHandler, Type};
use pgwire::error::PgWireResult;
use pgwire::tokio::process_socket;

pub struct PGPProcessor;

#[async_trait]
impl SimpleQueryHandler for PGPProcessor {
    async fn do_query<'a, C>(&self, _client: &C, query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let output = Command::new("osqueryi")
            .arg(query)
            .output()
            .expect("failed to execute osquery query");

        let mut osquery_res = String::new();
        osquery_res.push_str(match str::from_utf8(&output.stdout) {
            Ok(val) => val,
            Err(_) => panic!("got non UTF-8 data from output"),
        });
        let raw_res: Vec<&str> = osquery_res.lines().collect();

        //format data to suit QueryResponse
        let mut field_infos: Vec<FieldInfo> = vec![];
        let mut data: Vec<(Option<i32>, Option<&str>)> = vec![];

        for n in 0..raw_res.len() {
            let text = raw_res[n];
            if text.starts_with('|') {
                //fields
                if n == 1 {
                    for field in text.split('|').collect::<Vec<&str>>().into_iter() {
                        field_infos.push(FieldInfo::new(
                            field.to_string(),
                            None,
                            None,
                            Type::VARCHAR,
                            FieldFormat::Text,
                        ))
                    }
                } else {
                    let mut count: i32 = 0;
                    for row in text.split('|').collect::<Vec<&str>>().into_iter() {
                        data.push((Some(count), Some(row)));
                        count += 1;
                    }
                }
            }
        }

        let schema = Arc::new(field_infos);
        let schema_ref = schema.clone();
        let data_row_stream = stream::iter(data.into_iter()).map(move |r| {
            let mut encoder = DataRowEncoder::new(schema_ref.clone());
            encoder.encode_field(&r.0)?;
            encoder.encode_field(&r.1)?;

            encoder.finish()
        });
        // io::stdout().write_all(&output.stdout).unwrap();
        // io::stderr().write_all(&output.stderr).unwrap();

        Ok(vec![Response::Query(QueryResponse::new(
            schema,
            data_row_stream,
        ))])

        // Ok(vec![Response::Execution(Tag::new_for_execution(
        //     "OK",
        //     Some(1),
        // ))])
    }
}
#[tokio::main]
pub async fn main() {
    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(PGPProcessor)));
    // We have not implemented extended query in this server, use placeholder instead
    let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(
        PlaceholderExtendedQueryHandler,
    )));
    let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));

    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
        let placeholder_ref = placeholder.make();
        tokio::spawn(async move {
            process_socket(
                incoming_socket.0,
                None,
                authenticator_ref,
                processor_ref,
                placeholder_ref,
            )
            .await
        });
    }
}
