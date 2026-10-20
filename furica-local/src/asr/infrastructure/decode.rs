use std::{fs::File, io::Read, path::Path};

use furica_core::{Error, Result};
use tokio_util::sync::CancellationToken;

use super::check_cancel;

pub(super) fn load_tokens(path: &Path) -> Result<Vec<String>> {
    let mut text = String::new();
    File::open(path)?
        .take(1024 * 1024 + 1)
        .read_to_string(&mut text)?;
    if text.len() > 1024 * 1024 {
        return Err(Error::Config("SenseVoice tokens exceed 1 MiB".into()));
    }
    let mut tokens = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let (token, id) = line
            .rsplit_once(' ')
            .ok_or_else(|| Error::Config("invalid SenseVoice token line".into()))?;
        if token.is_empty() || id.parse::<usize>().ok() != Some(index) {
            return Err(Error::Config(
                "SenseVoice tokens must have contiguous ordered IDs".into(),
            ));
        }
        tokens.push(token.to_owned());
    }
    if tokens.first().map(String::as_str) != Some("<unk>") {
        return Err(Error::Config("SenseVoice token 0 must be <unk>".into()));
    }
    Ok(tokens)
}

pub(super) fn text(
    logits: &[f32],
    tokens: &[String],
    cancel: &CancellationToken,
) -> Result<String> {
    let mut previous = None;
    let mut decoded = Vec::new();
    for frame in logits.chunks_exact(tokens.len()) {
        check_cancel(cancel)?;
        if frame.iter().any(|x| !x.is_finite()) {
            return Err(Error::Protocol(
                "SenseVoice returned non-finite logits".into(),
            ));
        }
        let mut id = 0;
        for i in 1..frame.len() {
            if frame[i] > frame[id] {
                id = i;
            }
        }
        if id != 0 && previous != Some(id) {
            decoded.push(id);
        }
        // Blank resets CTC repetitions, so a-a and a-blank-a differ.
        previous = Some(id);
    }
    // The first four decoded tokens are language, emotion, event and ITN.
    if decoded.len() < 4 || decoded[..4].iter().any(|&id| !is_tag(&tokens[id])) {
        return Err(Error::Protocol(
            "SenseVoice result is missing its four metadata tags".into(),
        ));
    }
    let mut result = String::new();
    for &id in &decoded[4..] {
        let token = &tokens[id];
        if !is_tag(token) && token != "<s>" && token != "</s>" {
            result.push_str(token);
        }
    }
    Ok(result.replace('▁', " ").trim().to_owned())
}

fn is_tag(token: &str) -> bool {
    token.starts_with("<|") && token.ends_with("|>")
}
