//! The data directory's TOML file is the sole source of system-prompt text.
use crate::{Error, Result, configuration::application::prompts::render_system};
use std::{io::Read, path::Path};

pub fn load_system(data_dir: &Path, workspace: &Path, max_bytes: usize) -> Result<String> {
    let invalid = || {
        Error::new(
            400,
            "invalid_system_prompt",
            "system-prompt.toml must contain exactly one string key named system",
        )
    };
    let mut bytes = Vec::new();
    std::fs::File::open(data_dir.join("system-prompt.toml"))
        .map_err(|_| {
            Error::new(
                400,
                "system_prompt_unavailable",
                "could not read system-prompt.toml from the host data directory",
            )
        })?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid())?;
    if bytes.len() > 1024 * 1024 {
        return Err(Error::new(
            413,
            "system_prompt_too_large",
            "system-prompt.toml exceeds 1 MiB",
        ));
    }
    let source = String::from_utf8(bytes).map_err(|_| invalid())?;
    let document = source
        .parse::<toml_edit::Document<String>>()
        .map_err(|_| invalid())?;
    if document.iter().count() != 1 {
        return Err(invalid());
    }
    let template = document
        .get("system")
        .and_then(|item| item.as_str())
        .ok_or_else(invalid)?;
    render_system(template, workspace, max_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_renders_and_reloading_reads_the_edited_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system-prompt.toml");
        std::fs::write(&path, include_str!("../../system-prompt.toml")).unwrap();
        let rendered = load_system(dir.path(), Path::new("/work"), 256 * 1024).unwrap();
        assert!(rendered.contains("Asia/Shanghai"));
        assert!(rendered.contains("/work"));
        assert!(!rendered.contains("{{"));
        std::fs::write(path, "system = 'edited'").unwrap();
        assert_eq!(
            load_system(dir.path(), Path::new("/work"), 1024).unwrap(),
            "edited"
        );
    }

    #[test]
    fn missing_malformed_and_non_string_configuration_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            load_system(dir.path(), Path::new("/work"), 1024)
                .unwrap_err()
                .code,
            "system_prompt_unavailable"
        );
        for source in [
            "system = '",
            "other = 'text'",
            "system = 42",
            "system = 'text'\nextra = 'unused'",
            "system = 'one'\nsystem = 'two'",
        ] {
            std::fs::write(dir.path().join("system-prompt.toml"), source).unwrap();
            assert_eq!(
                load_system(dir.path(), Path::new("/work"), 1024)
                    .unwrap_err()
                    .code,
                "invalid_system_prompt"
            );
        }
    }
}
