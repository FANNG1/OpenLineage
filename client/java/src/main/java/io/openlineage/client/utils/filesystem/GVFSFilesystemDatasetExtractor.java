/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.filesystem;

import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URI;

public class GVFSFilesystemDatasetExtractor implements FilesystemDatasetExtractor {
  private static final String SCHEME = "gvfs";

  @Override
  public boolean isDefinedAt(URI location) {
    return SCHEME.equalsIgnoreCase(location.getScheme());
  }

  @Override
  public DatasetIdentifier extract(URI location) {
    String path = location.getPath();
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    String[] parts = path.split("/");
    String result = "";
    if (parts.length >= 3) {
      result = parts[0] + "." + parts[1] + "." + parts[2];
      System.out.println(result);
    } else {
      result = path;
      System.out.println("bad gvfs format:" + path);
    }
    return new DatasetIdentifier(result, "test");
  }

  @Override
  public DatasetIdentifier extract(URI location, String rawName) {
    System.out.println("location:" + location.toString() + ", raw name:" + rawName);
    return extract(location);
  }
}
