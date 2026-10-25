//! System-prompt variables and substitution live here so they can be changed together.
use crate::{Error, Result};
use std::path::Path;

pub fn render_system(template: &str, workspace: &Path, max_bytes: usize) -> Result<String> {
    let workspace = workspace.to_string_lossy();
    // Add or change runtime variables in this one place. Values are never rendered recursively.
    let variables = [
        ("language", "简体中文"),
        ("timezone", "Asia/Shanghai"),
        ("workspace", workspace.as_ref()),
    ];
    let mut rendered = String::new();
    let mut remaining = template;
    while let Some((text, variable)) = remaining.split_once("{{") {
        let (name, rest) = variable.split_once("}}").ok_or_else(invalid_variable)?;
        let value = variables
            .iter()
            .find_map(|(key, value)| (*key == name.trim()).then_some(*value))
            .ok_or_else(invalid_variable)?;
        rendered.push_str(text);
        rendered.push_str(value);
        remaining = rest;
        if rendered.len() > max_bytes {
            return Err(too_large());
        }
    }
    rendered.push_str(remaining);
    if rendered.len() > max_bytes {
        return Err(too_large());
    }
    if rendered.trim().is_empty() {
        return Err(Error::new(
            400,
            "invalid_system_prompt",
            "system prompt must not be empty",
        ));
    }
    Ok(rendered)
}

fn invalid_variable() -> Error {
    Error::new(
        400,
        "invalid_system_prompt",
        "system prompt contains an unknown or unclosed variable; supported variables: language, timezone, workspace",
    )
}

fn too_large() -> Error {
    Error::new(
        413,
        "system_prompt_too_large",
        "system prompt exceeds max_context_bytes",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replacement_preserves_multiline_text_and_does_not_expand_values() {
        let workspace = Path::new("/projects/{{language}}/项目");
        assert_eq!(
            render_system(
                "语言：{{ language }}\n时区：{{timezone}}\n目录：{{workspace}}\nJSON: {\"ok\": true}",
                workspace,
                1024,
            ).unwrap(),
            "语言：简体中文\n时区：Asia/Shanghai\n目录：/projects/{{language}}/项目\nJSON: {\"ok\": true}"
        );
    }

    #[test]
    fn invalid_templates_and_rendered_byte_overflow_are_rejected() {
        for template in ["{{unknown}}", "{{language", "  \n"] {
            assert_eq!(
                render_system(template, Path::new("/work"), 1024)
                    .unwrap_err()
                    .code,
                "invalid_system_prompt"
            );
        }
        assert_eq!(
            render_system("{{language}}", Path::new("/work"), 5)
                .unwrap_err()
                .code,
            "system_prompt_too_large"
        );
    }
}
