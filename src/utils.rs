use std::cmp;
use std::time::Duration;
use std::future::Future;
use tokio::time::delay_for;

pub async fn with_retry<F, T, E, Fut>(
    retry_max: u32,
    wait_base: u32,
    wait_max: u32,
    mut f: F,
) -> Result<T, E>
where
    Fut: Future<Output = Result<T, E>>,
    F: FnMut() -> Fut,
{
    let mut retry: u32 = 0;
    loop {
        let e = match f().await {
            Ok(r) => { return Ok(r); },
            Err(e) => e,
        };
        retry += 1;
        if retry > retry_max {
            return Err(e);
        }
        let wait = cmp::min(wait_max, wait_base.pow(retry));
        //eprintln!("RETRY #{} waiting {}secs: {}", retry, wait, e);
        delay_for(Duration::from_secs(wait as u64)).await;
    }
}
