//! Login-service files that keep the daemon running: a LaunchAgent plist on
//! macOS, a systemd user unit on Linux. Install/uninstall only write or remove
//! the file; they never call launchctl or systemctl.
//!
//! Both restart the daemon only when it fails. A clean exit (SIGTERM, or a
//! second copy finding the lock held) stays down, so a duplicate never loops.

use std::path::{Path, PathBuf};

pub const LABEL: &str = "ai.rever.boss.scheduler";
pub const UNIT: &str = "boss-scheduler.service";

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
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
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

/// Quote one systemd unit-file value: `"` and `\` escaped, `%` (specifiers)
/// and `$` (variable expansion) doubled so paths are taken literally.
fn sd_quote(s: &str) -> String {
    let s = s
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('%', "%%")
        .replace('$', "$$");
    format!("\"{s}\"")
}

/// Render the systemd user unit. `KillMode=mixed`: stop sends SIGTERM to the
/// daemon alone, which terminates its job groups itself and records them as
/// interrupted; anything still left after that is SIGKILLed by systemd.
pub fn systemd_unit(bin: &Path, home: Option<&Path>, log: &Path, path_env: &str) -> String {
    let mut exec = sd_quote(&bin.display().to_string());
    if let Some(h) = home {
        exec += &format!(" --home {}", sd_quote(&h.display().to_string()));
    }
    exec += " daemon";
    let log = log.display().to_string().replace('%', "%%");
    format!(
        "[Unit]
Description=Boss Scheduler

[Service]
ExecStart={exec}
Restart=on-failure
RestartSec=10
KillMode=mixed
TimeoutStopSec=30
Environment={}
StandardOutput=append:{log}
StandardError=append:{log}

[Install]
WantedBy=default.target
",
        sd_quote(&format!("PATH={path_env}"))
    )
}

/// Renders a service file from (binary, custom home, log, PATH).
type Render = fn(&Path, Option<&Path>, &Path, &str) -> String;

/// The service file for this OS and where it goes.
fn target() -> Result<(PathBuf, Render), String> {
    let home = PathBuf::from(std::env::var("HOME").map_err(|_| "HOME is not set")?);
    if cfg!(target_os = "macos") {
        Ok((home.join("Library/LaunchAgents").join(format!("{LABEL}.plist")), plist))
    } else if cfg!(target_os = "linux") {
        let config = std::env::var("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| home.join(".config"));
        Ok((config.join("systemd/user").join(UNIT), systemd_unit))
    } else {
        Err("install-agent supports macOS (launchd) and Linux (systemd) only".into())
    }
}

/// What install-agent would write, for this OS.
pub fn render(sched_home: &Path, custom_home: bool) -> Result<String, String> {
    let (_, render) = target()?;
    let (bin, home, path_env) = inputs(sched_home)?;
    Ok(render(
        &bin,
        custom_home.then_some(home.as_path()),
        &home.join("daemon.log"),
        &path_env,
    ))
}

/// The service runs from `/` (launchd) or the user's home (systemd), so every
/// path in it must be absolute.
fn inputs(sched_home: &Path) -> Result<(PathBuf, PathBuf, String), String> {
    let bin = std::env::current_exe()
        .and_then(|p| p.canonicalize())
        .map_err(|e| format!("locate binary: {e}"))?;
    let home = std::path::absolute(sched_home).map_err(|e| format!("resolve {}: {e}", sched_home.display()))?;
    let path_env = std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin:/usr/sbin:/sbin".into());
    Ok((bin, home, path_env))
}

pub fn install(sched_home: &Path, custom_home: bool) -> Result<String, String> {
    let (dest, _) = target()?;
    let body = render(sched_home, custom_home)?;
    std::fs::create_dir_all(sched_home).map_err(|e| e.to_string())?;
    std::fs::create_dir_all(dest.parent().unwrap()).map_err(|e| e.to_string())?;
    std::fs::write(&dest, body).map_err(|e| format!("write {}: {e}", dest.display()))?;
    let next = if cfg!(target_os = "macos") {
        format!(
            "not loaded. To start it:\n  launchctl bootstrap gui/$(id -u) {}\n\
             After swapping the binary:\n  launchctl kickstart -k gui/$(id -u)/{LABEL}",
            dest.display()
        )
    } else {
        format!(
            "not enabled. To start it:\n  systemctl --user daemon-reload\n  systemctl --user enable --now {UNIT}\n\
             After swapping the binary:\n  systemctl --user restart {UNIT}\n\
             To keep it running while you are logged out:\n  loginctl enable-linger $USER"
        )
    };
    Ok(format!("wrote {}\n{next}", dest.display()))
}

pub fn uninstall() -> Result<String, String> {
    let (dest, _) = target()?;
    let stop = if cfg!(target_os = "macos") {
        format!("launchctl bootout gui/$(id -u)/{LABEL}")
    } else {
        format!("systemctl --user disable --now {UNIT}")
    };
    match std::fs::remove_file(&dest) {
        Ok(()) => Ok(format!(
            "removed {}\nif it is loaded, also run:\n  {stop}",
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
    fn plist_restarts_only_on_failure() {
        let p = plist(
            Path::new("/opt/boss/boss-sched"),
            None,
            Path::new("/u/.boss/scheduler/daemon.log"),
            "/usr/bin:/bin",
        );
        assert!(p.contains("<string>/opt/boss/boss-sched</string>\n    <string>daemon</string>"));
        assert!(p.contains("<key>KeepAlive</key>\n  <dict>\n    <key>SuccessfulExit</key>\n    <false/>"));
        assert!(!p.contains("<key>KeepAlive</key>\n  <true/>"));
        assert!(p.contains("<key>RunAtLoad</key>\n  <true/>"));
        assert!(p.contains(&format!("<string>{LABEL}</string>")));
        assert!(!p.contains("--home"));
        let p = plist(Path::new("/b"), Some(Path::new("/h&x")), Path::new("/l"), "/p");
        assert!(p.contains("<string>--home</string>\n    <string>/h&amp;x</string>"));
    }

    #[test]
    fn systemd_unit_quotes_paths_and_restarts_only_on_failure() {
        let u = systemd_unit(
            Path::new("/opt/boss sched/boss-sched"),
            Some(Path::new("/h/50%$x")),
            Path::new("/h/50%$x/daemon.log"),
            "/usr/bin:/bin",
        );
        assert!(
            u.contains("ExecStart=\"/opt/boss sched/boss-sched\" --home \"/h/50%%$$x\" daemon\n"),
            "{u}"
        );
        assert!(u.contains("Restart=on-failure\n"));
        assert!(u.contains("KillMode=mixed\n"));
        assert!(u.contains("Environment=\"PATH=/usr/bin:/bin\"\n"));
        assert!(u.contains("StandardOutput=append:/h/50%%$x/daemon.log\n"));
        assert!(u.contains("WantedBy=default.target"));
        let u = systemd_unit(Path::new("/b"), None, Path::new("/l"), "/p");
        assert!(u.contains("ExecStart=\"/b\" daemon\n"));
    }

    #[test]
    fn relative_home_is_made_absolute() {
        let (_, home, _) = inputs(Path::new("rel/sched")).unwrap();
        assert!(home.is_absolute());
        assert!(home.ends_with("rel/sched"));
    }
}
