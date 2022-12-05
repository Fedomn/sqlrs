use super::BindError;

#[derive(thiserror::Error, Debug)]
pub enum PlannerError {
    #[error("bind error: {0}")]
    BindError(
        #[from]
        #[source]
        BindError,
    ),
}
