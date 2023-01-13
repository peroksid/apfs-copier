use clap::Parser;
use std::{collections::HashSet, fs, path::Path, path::PathBuf, sync::Mutex, thread, time};

#[macro_use]
extern crate lazy_static;

#[derive(Parser)]
#[command(name= "APFS Copier")]
#[command(about = "Copy a directory tree from a mounted APFS volume to a destination directory on ExFAT volume in Linux")]
#[command(author = "Alexander Pugachev")]
struct Cli {
    device: String,
    mount_point: String,
    source: PathBuf,
    dest: PathBuf,
}

lazy_static! {
    static ref FAILED_CONNECTION_ABORTS: Mutex<HashSet<String>> = { Mutex::new(HashSet::new()) };
}

fn main() {
    let args = Cli::parse();
    initial_mount_check(&args);
    copy_tree(&args);
    println!("done!");
}

fn initial_mount_check(args: &Cli) {
    match fs::read_dir(args.source.as_path()) {
        Ok(dir_content) => {
            println!("{:#?}", dir_content);
        }
        Err(e) => match e.raw_os_error() {
            Some(107) => {
                // Transport endpoint is not connected
                println!("Transport endpoint is not connected, mounting at start");
                mount(args.device.as_str(), args.mount_point.as_str());
            }
            _ => panic!("Error: {}", e),
        },
    };
    println!("passed initial mount check");
}

fn copy_tree(args: &Cli) {
    let mut stack = vec![];
    stack.push(PathBuf::from(&args.source));
    while let Some(path) = stack.pop() {
        if is_failure(&path) {
            continue;
        }
        // every component of dest path must be escaped properly because it's created underscored at the destination
        let dest_path: PathBuf = args
            .dest
            .join(path.strip_prefix(args.source.as_path()).unwrap()).iter()
            .map(|p| underscore_non_windows_chars(p.to_str().unwrap().to_string()))
            .collect();
        if path.is_dir() {

            match fs::create_dir_all(&dest_path) {
                Ok(_) => (),
                Err(e) => match e.raw_os_error() {
                    Some(22) => {
                        fs::create_dir_all(replace_forbidden_characters(&dest_path)).unwrap();
                    }
                    _ => panic!("Error: {:#?} From: '{:#?}' To: '{:#?}'", e, &path, &dest_path),
                },
            }
            let mut need_remount = false;

            for entry in fs::read_dir(&path).unwrap() {
                match entry {
                    Ok(entry) => stack.push(entry.path()),
                    Err(e) => match e.raw_os_error() {
                        Some(103) => {
                            // can't remount here because the file we failed to open is still in use preventing umount
                            need_remount = true;
                            break;
                        } // Software caused connection abort -- this is we're here, need to remount, remember not to try this path again, and continue
                        _ => panic!("Error: {}", e),
                    },
                };
            }

            if need_remount {
                handle_software_caused_connection_abort(args, &path).unwrap();
            }
        } else {
            copy_file(args, path.as_path(), dest_path.as_path()).unwrap();
        }
    }
}

fn copy_file(args: &Cli, from: &Path, to: &Path) -> Result<(), std::io::Error> {
    if to.exists() {
        return Ok(());
    }
    match fs::copy(from, to) {
        Ok(_) => Ok(()),
        Err(e) => match e.raw_os_error() {
            Some(5) => Ok(()), //  input-output error, can't get source data, just continue
            Some(103) => handle_software_caused_connection_abort(args, from), // Software caused connection abort -- this is we're here, need to remount, remember not to try this path again, and continue
            Some(22) => copy_file(args, from, replace_forbidden_characters(to).as_path()),
            _ => panic!("Error: {:#?} From: '{:#?}' To: '{:#?}'", e, from, to),
        },
    }
}

fn handle_software_caused_connection_abort(args: &Cli, path: &Path) -> Result<(), std::io::Error> {
    println!(
        "Software caused connection abort, remounting and continuing: {}",
        &path.to_str().unwrap().to_string()
    );
    remember_failure(path);
    remount(args);
    println!("remounted, continuing");
    Ok(())
}

fn umount(mount_point: &str) {
    let output = std::process::Command::new("sudo")
        .arg("umount")
        .arg(mount_point)
        .output()
        .expect("failed to execute umount");
    println!("status: {}", output.status);
    println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
    println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
    if output.status.success() {
        println!("umounted");
    } else {
        println!("failed to umount");
    }
    thread::sleep(time::Duration::from_secs(10));
}

fn mount(device: &str, mount_point: &str) {
    let output = std::process::Command::new("sudo")
        .arg("apfs-fuse")
        .arg(device)
        .arg(mount_point)
        .output()
        .expect("failed to execute mount");
    println!("status: {}", output.status);
    println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
    println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
    if output.status.success() {
        println!("mounted");
    } else {
        umount(mount_point);
        println!("failed to mount, retrying");
        mount(device, mount_point);
    }
    thread::sleep(time::Duration::from_secs(10));
}

fn remount(args: &Cli) {
    println!("remounting");
    umount(args.mount_point.as_str());
    mount(args.device.as_str(), args.mount_point.as_str());
}

fn remember_failure(path: &Path) {
    let mut set = FAILED_CONNECTION_ABORTS.lock().unwrap();
    set.insert(path.to_str().unwrap().to_string());
}

fn is_failure(path: &Path) -> bool {
    FAILED_CONNECTION_ABORTS
        .lock()
        .unwrap()
        .contains(&path.to_str().unwrap().to_string())
}

fn replace_forbidden_characters(path: &Path) -> PathBuf {
    let mut new_path = PathBuf::from(path);
    new_path.set_file_name(underscore_non_windows_chars(
        path.file_name().unwrap().to_str().unwrap().to_string(),
    ));
    new_path
}

fn underscore_non_windows_chars(filename: String) -> String {
    // " * / : < > ? \ |
    filename
        .replace("\"", "_")
        .replace("*", "_")
        .replace("/", "_")
        .replace(":", "_")
        .replace("<", "_")
        .replace(">", "_")
        .replace("?", "_")
        .replace("\\", "_")
        .replace("|", "_")
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_underscore_non_windows_chars() {
        assert_eq!(
            super::underscore_non_windows_chars("foo\"bar".to_string()),
            "foo_bar".to_string()
        );
        assert_eq!(
            super::underscore_non_windows_chars("foo*bar".to_string()),
            "foo_bar".to_string()
        );
        assert_eq!(
            super::underscore_non_windows_chars("foo/bar".to_string()),
            "foo_bar".to_string()
        );
        assert_eq!(
            super::underscore_non_windows_chars("foo:bar".to_string()),
            "foo_bar".to_string()
        );
        assert_eq!(
            super::underscore_non_windows_chars("foo<bar".to_string()),
            "foo_bar".to_string()
        );
        assert_eq!(
            super::underscore_non_windows_chars("foo>bar".to_string()),
            "foo_bar".to_string()
        );
        assert_eq!(
            super::underscore_non_windows_chars("foo\\bar".to_string()),
            "foo_bar".to_string()
        );
        assert_eq!(
            super::underscore_non_windows_chars("foo|bar".to_string()),
            "foo_bar".to_string()
        );
    }
}
