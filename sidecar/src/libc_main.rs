use std::{ffi::{self, CString, CStr}, fs::{File, self, OpenOptions}, path::Path, time::SystemTime, process, thread, time::Duration};
use std::io::Write;

use ddcommon::cstr;
use nix::libc;

use spawn_worker::utils::{raw_env, ExecVec, CListMutPtr};

use sysinfo::{ProcessExt, System, SystemExt};

use chrono;

use crate::sidecar::maybe_start;


type StartMainFn = extern "C" fn(
    main: MainFn,
    argc: ffi::c_int,
    argv: *const *const ffi::c_char,
    init: InitFn,
    fini: FiniFn,
    rtld_fini: FiniFn,
    stack_end: *const ffi::c_void,
);
type MainFn = unsafe extern "C" fn(
    ffi::c_int,
    *const *const ffi::c_char,
    *const *const ffi::c_char,
) -> ffi::c_int;
type InitFn = extern "C" fn(ffi::c_int, *const *const ffi::c_char, *const *const ffi::c_char);
type FiniFn = extern "C" fn();

#[no_mangle]
pub unsafe extern "C" fn start_mini_agent() {
    maybe_start().unwrap();
}

#[cfg(feature = "build_for_node")]
#[no_mangle]
#[napi]
unsafe extern "C" fn napi_start_mini_agent() {
    maybe_start().unwrap();
}

#[allow(dead_code)]
unsafe extern "C" fn new_main(
    argc: ffi::c_int,
    argv: *const *const ffi::c_char,
    _envp: *const *const ffi::c_char,
) -> ffi::c_int {
    let path = maybe_start().unwrap();

    let mut env = raw_env::as_clist();
    env.remove_entry(|e| e.starts_with("LD_PRELOAD=".as_bytes()));

    let mut env: ExecVec<10> = env.into_exec_vec();

    env.push_cstring(
        CString::new(format!(
            "DD_TRACE_AGENT_URL=unix://{}",
            path.to_string_lossy()
        ))
        .expect("extra null found in in new env variable"),
    );

    println!("{}", format!(
        "DD_TRACE_AGENT_URL=unix://{}",
        path.to_string_lossy()
    ));

    let old_environ = raw_env::swap(env.as_ptr());

    let rv = match unsafe { ORIGINAL_MAIN } {
        Some(main) => main(argc, argv, env.as_ptr()),
        None => 0,
    };

    // setting back before exiting as env will be garbage collected and all of its references will become invalid
    raw_env::swap(old_environ);
    rv
}

/// # Safety
///
/// This method is meant to only be called by the default elf entrypoing once the symbol is replaced by LD_PRELOAD
#[no_mangle]
pub unsafe extern "C" fn __libc_start_main(
    main: MainFn,
    argc: ffi::c_int,
    argv: *const *const ffi::c_char,
    init: InitFn,
    fini: FiniFn,
    rtld_fini: FiniFn,
    stack_end: *const ffi::c_void,
) { 
    let libc_start_main =
        spawn_worker::utils::dlsym::<StartMainFn>(libc::RTLD_NEXT, cstr!("__libc_start_main"))
            .unwrap();
    ORIGINAL_MAIN = Some(main);
    // #[cfg(not(test))]
    // println!("starting new_main");
    // #[cfg(not(test))]
    // libc_start_main(new_main, argc, argv, init, fini, rtld_fini, stack_end);
    // #[cfg(test)]
    // libc_start_main(
    //     unsafe { ORIGINAL_MAIN.unwrap() },
    //     argc,
    //     argv,
    //     init,
    //     fini,
    //     rtld_fini,
    //     stack_end,
    // );

    // the pointer to envp is the next integer after argv
    // it's a null-terminated array of strings
    // Note: for some reason setting a new env in new_main didn't work,
    // as the subprocesses spawned by this process still contain LD_PRELOAD,
    // but removing it here does indeed work

    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open("/tmp/mini-agent-logs.txt")
        .unwrap();

    let current_process: String = std::env::current_exe()
        .expect("Can't get the exec path")
        .to_string_lossy()
        .into_owned();

    let time = chrono::offset::Utc::now();

    writeln!(f, "| ld_preload at timestamp: {:?}, for process named: {} and with pid: {} |\n", time, current_process, process::id()).unwrap();

    // libc_start_main(
    //     ORIGINAL_MAIN.unwrap(),
    //     argc,
    //     argv,
    //     init,
    //     fini,
    //     rtld_fini,
    //     stack_end,
    // )

    let envp_ptr = argv.offset(argc as isize + 1) as *mut *const ffi::c_char;
    let mut env_vec = CListMutPtr::from_raw_parts(envp_ptr);
    match env_vec.remove_entry(|e| e.starts_with("LD_PRELOAD=".as_bytes())) {
        Some(preload_lib) => {
            println!(
                "Found {} in process {}, starting bootstrap process",
                CStr::from_ptr(preload_lib as *const ffi::c_char)
                    .to_str()
                    .expect("Couldn't convert LD_PRELOAD lib to string"),
                std::process::id(),
            );

            libc_start_main(new_main, argc, argv, init, fini, rtld_fini, stack_end)
        }
        None => {
            println!(
                "No LD_PRELOAD found in env of process {}",
                std::process::id()
            );
            libc_start_main(
                ORIGINAL_MAIN.unwrap(),
                argc,
                argv,
                init,
                fini,
                rtld_fini,
                stack_end,
            )
        }
    }
}

static mut ORIGINAL_MAIN: Option<MainFn> = None;
