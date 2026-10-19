//! macOS LaunchAgent plist generation and install/uninstall (file only; never
//! calls launchctl).

use std::path::{Path, PathBuf};

pub const LABEL: &str = "ai.rever.boss.scheduler";

fn esc(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

/// Render the plist. `home` is passed to the daemon only when it is not the default.
pub fn plist(bin: &Path, home: Option<&Path>, log: &Path, path_env: &str) -> String {
    let mut args = format!("    <string>{}</string>\n", esc(&bin.display().to_string()));
    if let Some(h) = home {
        args += &format!(
            "    <string>--home</string>\n    <string>{}</string>\n",
            esc(&h.display().to_string())
        );
    }
    args += "    <string>daemon</string>\n";
    let log = esc(&log.display().to_string());
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{LABEL}</string>
  <key>ProgramArguments</key>
  <array>
{args}  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>10</integer>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>{}</string>
  </dict>
  <key>StandardOutPath</key>
  <string>{log}</string>
  <key>StandardErrorPath</key>
  <string>{log}</string>
</dict>
</plist>
"#,
        esc(path_env)
    )
}

pub fn plist_path() -> Result<PathBuf, String> {
    let home = std::env::var("HOME").map_err(|_| "HOME is not set")?;
    Ok(PathBuf::from(home)
        .join("Library/LaunchAgents")
        .join(format!("{LABEL}.plist")))
}

pub fn install(sched_home: &Path, custom_home: bool) -> Result<String, String> {
    if !cfg!(target_os = "macos") {
        return Err("install-agent is macOS only; see README for a systemd user unit".into());
    }
    let bin = std::env::current_exe()
        .and_then(|p| p.canonicalize())
        .map_err(|e| format!("locate binary: {e}"))?;
    let path_env = std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin:/usr/sbin:/sbin".into());
    let body = plist(
        &bin,
        custom_home.then_some(sched_home),
        &sched_home.join("daemon.log"),
        &path_env,
    );
    let dest = plist_path()?;
    std::fs::create_dir_all(sched_home).map_err(|e| e.to_string())?;
    std::fs::create_dir_all(dest.parent().unwrap()).map_err(|e| e.to_string())?;
    std::fs::write(&dest, body).map_err(|e| format!("write {}: {e}", dest.display()))?;
    Ok(format!(
        "wrote {}\nnot loaded. To start it:\n  launchctl bootstrap gui/$(id -u) {}\n\
                After swapping the binary:\n  launchctl kickstart -k gui/$(id -u)/{LABEL}",
        dest.display(),
        dest.display()
    ))
}

pub fn uninstall() -> Result<String, String> {
    let dest = plist_path()?;
    match std::fs::remove_file(&dest) {
        Ok(()) => Ok(format!(
            "removed {}\nif it is loaded, also run:\n  launchctl bootout gui/$(id -u)/{LABEL}",
            dest.display()
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(format!("{} not present", dest.display())),
        Err(e) => Err(format!("remove {}: {e}", dest.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plist_has_absolute_binary_and_keepalive() {
        let p = plist(
            Path::new("/opt/boss/boss-sched"),
            None,
            Path::new("/u/.boss/scheduler/daemon.log"),
            "/usr/bin:/bin",
        );
        assert!(p.contains("<string>/opt/boss/boss-sched</string>\n    <string>daemon</string>"));
        assert!(p.contains("<key>KeepAlive</key>\n  <true/>"));
        assert!(p.contains("<key>RunAtLoad</key>\n  <true/>"));
        assert!(p.contains(&format!("<string>{LABEL}</string>")));
        assert!(!p.contains("--home"));
        let p = plist(Path::new("/b"), Some(Path::new("/h&x")), Path::new("/l"), "/p");
        assert!(p.contains("<string>--home</string>\n    <string>/h&amp;x</string>"));
    }
}
