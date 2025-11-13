use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if let Err(err) = copy_plugin() {
        eprintln!("cargo:warning={err}");
    }
}

fn copy_plugin() -> Result<(), String> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").map_err(|e| e.to_string())?);
    let workspace_root = manifest_dir
        .parent()
        .ok_or_else(|| "unable to determine workspace root".to_string())?
        .to_path_buf();

    let target_dir = env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| workspace_root.join("target"));
    let profile = env::var("PROFILE").map_err(|e| e.to_string())?;

    let file_name = format!(
        "{}stdfunc{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    );
    let source = target_dir.join(&profile).join(&file_name);
    if !source.exists() {
        return Err(format!(
            "stdfunc plugin artifact not found at {}",
            source.display()
        ));
    }

    let destination_dir = workspace_root.join("lib");
    fs::create_dir_all(&destination_dir).map_err(|e| e.to_string())?;
    let destination = destination_dir.join(file_name);
    fs::copy(&source, &destination)
        .map_err(|e| format!("failed to copy plugin to {}: {e}", destination.display()))?;
    println!(
        "cargo:warning=Copied stdfunc plugin to {}",
        destination.display()
    );
    Ok(())
}
