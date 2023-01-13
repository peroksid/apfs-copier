use std::{path::Path, path::PathBuf, fs, thread, time, collections::HashSet, sync::Mutex};

#[macro_use]
extern crate lazy_static;

const MOUNT_POINT: &str = "/mnt/media";
const SOURCE_DIR: &str = "/mnt/media/root";
const DEST_DIR: &str = "/media/ubuntu/My Passport/white";

lazy_static! {
    static ref  FAILED_CONNECTION_ABORTS: Mutex<HashSet<String>> = {
        Mutex::new(HashSet::new())
    };
}

fn main() {
    initial_mount_check();
    let source = Path::new(SOURCE_DIR);
    let dest = Path::new(DEST_DIR);
    copy_tree(source, dest);
    println!("done!");
}

fn initial_mount_check(){
    let source = Path::new(SOURCE_DIR);
    match fs::read_dir(&source) {
        Ok(dir_content) => {
            println!("{:#?}", dir_content);

        },
        Err(e) => match e.raw_os_error() {
            Some(107) => {
                // Transport endpoint is not connected
                println!("Transport endpoint is not connected, mounting at start");
                mount();
            },
            _ => panic!("Error: {}", e),
        }
    };
    println!("passed initial mount check");
}

fn copy_tree(source: &Path, dest: &Path) {
    let mut stack = vec![];
    stack.push(PathBuf::from(source));
    while let Some(path) = stack.pop() {
        if is_failure(&path) {
            continue;
        }
        let dest_path = dest.join(path.strip_prefix(source).unwrap());
        if path.is_dir() {
            fs::create_dir_all(&dest_path).unwrap();
            let mut need_remount = false;

            for entry in fs::read_dir(&path).unwrap() {
                match entry {
                    Ok(entry) => stack.push(entry.path()),
                    Err(e) => match e.raw_os_error() {
                        Some(103) => {
                            // can't remount here because the file we failed to open is still in use preventing umount
                            need_remount = true;
                            break;
                        }, // Software caused connection abort -- this is we're here, need to remount, remember not to try this path again, and continue
                        _ => panic!("Error: {}", e),
                    },
                };


            }

            if need_remount {
                handle_software_caused_connection_abort(&path).unwrap();
            }
        } else {
            copy_file(path.as_path(), dest_path.as_path()).unwrap();
        }
    }
}

fn copy_file(from: &Path, to: &Path) -> Result<(), std::io::Error> {
    if to.exists() {
        return Ok(());
    }
    match fs::copy(from, to) {
        Ok(_) => Ok(()),
        Err(e) => match e.raw_os_error() {
            Some(5) => Ok(()), //  input-output error, can't get source data, just continue
            Some(103) => handle_software_caused_connection_abort(from), // Software caused connection abort -- this is we're here, need to remount, remember not to try this path again, and continue
            _ => Err(e),
        },
    }
}


fn handle_software_caused_connection_abort(path: &Path) -> Result<(), std::io::Error>{
    println!("Software caused connection abort, remounting and continuing: {}", &path.to_str().unwrap().to_string());
    remember_failure(path);
    remount();
    println!("remounted, continuing");
    Ok(())
}

fn umount() {
    let output = std::process::Command::new("sudo")
        .arg("umount")
        .arg(MOUNT_POINT)
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

fn mount(){
    let output = std::process::Command::new("sudo")
        .arg("apfs-fuse")
        .arg("/dev/sdc2")
        .arg(MOUNT_POINT)
        .output()
        .expect("failed to execute mount");
    println!("status: {}", output.status);
    println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
    println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
    if output.status.success() {
        println!("mounted");
    } else {
        umount();
        println!("failed to mount, retrying");
        mount();
    }
    thread::sleep(time::Duration::from_secs(10));
}

fn remount() {
    println!("remounting");
    umount();
    mount();
}

fn remember_failure(path: &Path){
    let mut set = FAILED_CONNECTION_ABORTS.lock().unwrap();
    set.insert(path.to_str().unwrap().to_string());
}

fn is_failure(path: &Path) -> bool {
    FAILED_CONNECTION_ABORTS.lock().unwrap().contains(&path.to_str().unwrap().to_string())
}