use std::path::Path;

use furica_core::{Error, Result};
use ort::{
    execution_providers::CPUExecutionProvider,
    metadata::ModelMetadata,
    session::{Session, run_options::RunOptions},
    tensor::TensorElementType,
    value::{TensorRef, ValueType},
};
use tokio_util::sync::CancellationToken;

use super::{check_cancel, decode, features::DIM};

pub(super) fn error(error: ort::Error) -> Error {
    Error::Protocol(format!("SenseVoice ONNX: {error}"))
}

pub(super) fn load(path: &Path) -> Result<Session> {
    Session::builder()
        .and_then(|b| b.with_execution_providers([CPUExecutionProvider::default().build()]))
        .and_then(|b| b.with_intra_threads(2))
        .and_then(|b| b.with_inter_threads(1))
        .and_then(|b| b.commit_from_file(path))
        .map_err(error)
}

pub(super) fn validate(session: &Session, vocab: usize) -> Result<()> {
    let inputs = [
        ("x", TensorElementType::Float32, &[-1, -1, DIM as i64][..]),
        ("x_length", TensorElementType::Int32, &[-1][..]),
        ("language", TensorElementType::Int32, &[-1][..]),
        ("text_norm", TensorElementType::Int32, &[-1][..]),
    ];
    if session.inputs.len() != 4
        || session.outputs.len() != 1
        || !(5..=65_536).contains(&vocab)
        || inputs.iter().any(|(name, ty, shape)| {
            !session
                .inputs
                .iter()
                .any(|x| x.name == *name && compatible(&x.input_type, *ty, shape))
        })
        || !session.outputs.iter().any(|x| {
            x.name == "logits"
                && compatible(
                    &x.output_type,
                    TensorElementType::Float32,
                    &[-1, -1, vocab as i64],
                )
        })
    {
        return Err(Error::Config(
            "unsupported SenseVoice ONNX tensor contract".into(),
        ));
    }
    let metadata = session.metadata().map_err(error)?;
    for (key, expected) in [
        ("model_type", "sense_voice_ctc"),
        ("lfr_window_size", "7"),
        ("lfr_window_shift", "6"),
        ("normalize_samples", "0"),
        ("lang_auto", "0"),
        ("with_itn", "14"),
    ] {
        if metadata.custom(key).map_err(error)?.as_deref() != Some(expected) {
            return Err(Error::Config(format!(
                "unsupported SenseVoice metadata: {key}"
            )));
        }
    }
    if metadata
        .custom("vocab_size")
        .map_err(error)?
        .and_then(|v| v.parse::<usize>().ok())
        != Some(vocab)
    {
        return Err(Error::Config(
            "SenseVoice vocabulary does not match tokens".into(),
        ));
    }
    if metadata
        .custom("blank_id")
        .map_err(error)?
        .is_some_and(|v| v != "0")
    {
        return Err(Error::Config("SenseVoice requires blank_id=0".into()));
    }
    Ok(())
}

fn compatible(value: &ValueType, ty: TensorElementType, dims: &[i64]) -> bool {
    matches!(value, ValueType::Tensor { ty: actual, shape, .. }
        if *actual == ty && shape.len() == dims.len()
            && shape.iter().zip(dims).all(|(&a, &b)| if b == -1 { a == -1 || a == 1 } else { a == b || a == -1 }))
}

pub(super) fn normalization(metadata: &ModelMetadata<'_>, key: &str) -> Result<[f32; DIM]> {
    let value = metadata
        .custom(key)
        .map_err(error)?
        .ok_or_else(|| Error::Config(format!("missing SenseVoice {key}")))?;
    let values = value
        .split(',')
        .map(str::parse::<f32>)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| Error::Config(format!("invalid SenseVoice {key}")))?;
    if values.iter().any(|x| !x.is_finite()) {
        return Err(Error::Config(format!("non-finite SenseVoice {key}")));
    }
    values
        .try_into()
        .map_err(|_| Error::Config(format!("SenseVoice {key} must have {DIM} values")))
}

pub(super) fn infer(
    session: &mut Session,
    features: &[f32],
    tokens: &[String],
    cancel: &CancellationToken,
    options: &RunOptions,
) -> Result<String> {
    let frames = features.len() / DIM;
    let length = [frames as i32];
    let language = [0_i32];
    let itn = [14_i32];
    check_cancel(cancel)?;
    let outputs = session
        .run_with_options(
            ort::inputs! {
                "x" => TensorRef::from_array_view(([1, frames, DIM], features)).map_err(error)?,
                "x_length" => TensorRef::from_array_view(([1], &length[..])).map_err(error)?,
                "language" => TensorRef::from_array_view(([1], &language[..])).map_err(error)?,
                "text_norm" => TensorRef::from_array_view(([1], &itn[..])).map_err(error)?,
            },
            options,
        )
        .map_err(error)?;
    check_cancel(cancel)?;
    let (shape, logits) = outputs["logits"]
        .try_extract_tensor::<f32>()
        .map_err(error)?;
    if shape[..] != [1, (frames + 4) as i64, tokens.len() as i64] {
        return Err(Error::Protocol("unexpected SenseVoice logits shape".into()));
    }
    decode::text(logits, tokens, cancel)
}
