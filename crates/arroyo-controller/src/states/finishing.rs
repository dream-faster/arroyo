use crate::{JobMessage, states::StateError};

use super::{Finished, JobContext, State, Transition};

#[derive(Debug)]
pub struct Finishing {}

#[async_trait::async_trait]
impl State for Finishing {
    fn name(&self) -> &'static str {
        "Finishing"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        let mut final_checkpoint_started = false;

        loop {
            match ctx
                .job_controller
                .as_mut()
                .unwrap()
                .checkpoint_finished()
                .await
            {
                Ok(done) => {
                    if done
                        && ctx.job_controller.as_ref().unwrap().finished()
                        && final_checkpoint_started
                    {
                        return Ok(Transition::next(*self, Finished {}));
                    }
                }
                Err(e) => {
                    return Err(ctx.retryable(
                        self,
                        "failed while monitoring final checkpoint",
                        e,
                        10,
                    ));
                }
            }

            if !final_checkpoint_started {
                match ctx.job_controller.as_mut().unwrap().checkpoint(true).await {
                    Ok(started) => final_checkpoint_started = started,
                    Err(e) => {
                        return Err(ctx.retryable(
                            self,
                            "failed to initiate final checkpoint",
                            e,
                            10,
                        ));
                    }
                }
            }

            match ctx
                .rx
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("channel closed while receiving"))
            {
                Ok(JobMessage::RunningMessage(msg)) => {
                    if let Err(e) = ctx.job_controller.as_mut().unwrap().handle_message(msg).await {
                        return Err(ctx.retryable(
                            self,
                            "failed while waiting for job finish",
                            e,
                            10,
                        ));
                    }
                }
                Ok(msg) => {
                    ctx.handle(msg)?;
                }
                Err(e) => {
                    return Err(ctx.retryable(self, "failed while waiting for job to finish", e, 10));
                }
            }
        }
    }
}
