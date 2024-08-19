// Copyright 2021-Present Datadog, Inc. https://www.datadoghq.com/
// SPDX-License-Identifier: Apache-2.0

use std::{env, path::PathBuf};

use crate::primary_sidecar_identifier;

pub fn crashtracker_unix_socket_path() -> PathBuf {
    env::temp_dir().join(format!(
        concat!("libdatadog.ct.", crate::sidecar_version!(), "@{}.sock"),
        primary_sidecar_identifier()
    ))
}
