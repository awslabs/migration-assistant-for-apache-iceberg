# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""AWS Lambda handler for XTable conversion.

Packages Apache XTable in a container image with Java 11.
Uses subprocess to invoke XTable's RunSync via java -cp.

Input event:
{
    "source_format": "HUDI" | "DELTA",
    "target_formats": ["ICEBERG"],
    "table_base_path": "s3://bucket/path/to/table/",
    "table_name": "my_table",
    "database_name": "my_db",
    "sync_mode": "FULL" | "INCREMENTAL"
}

Output:
{
    "statusCode": 200,
    "body": {
        "message": "...",
        "table_base_path": "...",
        "metadata_path": "..."
    }
}
"""

import os
import subprocess
import tempfile

import yaml

# Writable scratch directory. In AWS Lambda this resolves to /tmp (the only
# writable location); using gettempdir() avoids hardcoding the literal path.
_TMP_DIR = tempfile.gettempdir()

# Required for Spark local mode inside Lambda
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-11")

# Path to the pre-built XTable bundled JAR
XTABLE_JAR = "/opt/xtable/xtable-utilities-bundled.jar"

JAVA_BIN = "/usr/lib/jvm/java-11/bin/java"


def _build_classpath():
    """Build classpath from bundled jar + all dependency jars in lib/."""
    cp = [XTABLE_JAR]
    lib_dir = "/opt/xtable/lib"
    if os.path.isdir(lib_dir):
        for f in os.listdir(lib_dir):
            if f.endswith(".jar"):
                cp.append(os.path.join(lib_dir, f))
    return ":".join(cp)


def handler(event, context):
    """Lambda entry point."""
    source_format = event["source_format"].upper()
    target_formats = event.get("target_formats", ["ICEBERG"])
    table_base_path = event["table_base_path"].rstrip("/") + "/"
    table_name = event["table_name"]
    database_name = event.get("database_name", "default")
    sync_mode = event.get("sync_mode", "FULL")

    print(f"XTable conversion: {source_format} → {target_formats}")
    print(f"  Table: {database_name}.{table_name}")
    print(f"  Path: {table_base_path}")
    print(f"  Mode: {sync_mode}")

    # Build dataset config YAML
    # Format expected by XTable main branch:
    # sourceFormat: HUDI
    # targetFormats:
    #   - ICEBERG
    # datasets:
    #   - tableBasePath: s3://...
    #     tableName: my_table
    dataset_config = {
        "sourceFormat": source_format,
        "targetFormats": target_formats,
        "datasets": [{
            "tableBasePath": table_base_path,
            "tableName": table_name,
        }]
    }

    with tempfile.NamedTemporaryFile(
            mode='w', suffix='.yaml', delete=False, dir=_TMP_DIR) as f:
        yaml.dump(dataset_config, f)
        f.flush()
        dataset_config_path = f.name

    # Write Hadoop config to handle S3 rename limitations
    hadoop_conf_dir = os.path.join(_TMP_DIR, "hadoop-conf")
    os.makedirs(hadoop_conf_dir, exist_ok=True)
    with open(f"{hadoop_conf_dir}/core-site.xml", "w", encoding="utf-8") as f:
        f.write("""<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.s3a.aws.credentials.provider</name>
    <value>com.amazonaws.auth.DefaultAWSCredentialsProviderChain</value>
  </property>
  <property>
    <name>fs.s3a.rename.raises.exceptions</name>
    <value>false</value>
  </property>
  <property>
    <name>fs.s3a.change.detection.mode</name>
    <value>none</value>
  </property>
</configuration>""")

    # Invoke XTable via java subprocess
    # Build minimal environment for Java subprocess (only known-safe values)
    java_env = {
        "JAVA_HOME": "/usr/lib/jvm/java-11",
        "PATH": "/usr/lib/jvm/java-11/bin:/usr/local/bin:/usr/bin:/bin",
        "HADOOP_CONF_DIR": hadoop_conf_dir,
        "SPARK_LOCAL_IP": "127.0.0.1",
        "HOME": _TMP_DIR,
    }

    try:
        # Validate paths are within expected directories (defense-in-depth)
        if not dataset_config_path.startswith(_TMP_DIR + os.sep):
            raise ValueError(f"Unexpected config path: {dataset_config_path}")

        classpath = _build_classpath()
        print(f"Running: {JAVA_BIN} -cp {classpath} org.apache.xtable.utilities.RunSync --datasetConfig {dataset_config_path}")

        result = subprocess.run(  # nosemgrep: dangerous-subprocess-use-audit
            [
                JAVA_BIN,
                "-cp", classpath,
                "org.apache.xtable.utilities.RunSync",
                "--datasetConfig", dataset_config_path,
            ],
            capture_output=True,
            text=True,
            timeout=280,  # Lambda timeout is 300s, leave headroom
            env=java_env,
        )

        print(f"XTable stdout: {result.stdout[-2000:]}" if result.stdout else "No stdout")
        if result.stderr:
            print(f"XTable stderr: {result.stderr[-2000:]}")

        if result.returncode != 0:
            error_msg = result.stderr[-500:] or result.stdout[-500:] or "Unknown error"
            return {
                "statusCode": 500,
                "body": {
                    "message": f"Conversion failed (exit code {result.returncode}): {error_msg}",
                    "table_base_path": table_base_path,
                    "error": error_msg,
                }
            }

        # XTable may exit 0 but log "Sync failed" — detect this silent failure
        combined_output = (result.stdout or "") + (result.stderr or "")
        if "Sync failed" in combined_output and "Sync is successful" not in combined_output:
            error_msg = "XTable exited 0 but sync failed. " + (result.stdout[-500:] or result.stderr[-500:])
            print(f"Detected silent XTable failure: {error_msg}")
            return {
                "statusCode": 500,
                "body": {
                    "message": f"Conversion failed (silent): {error_msg}",
                    "table_base_path": table_base_path,
                    "error": error_msg,
                }
            }

        print("XTable conversion completed successfully")
        return {
            "statusCode": 200,
            "body": {
                "message": (f"Converted {source_format} table "
                            f"{database_name}.{table_name} to "
                            f"{', '.join(target_formats)}"),
                "table_base_path": table_base_path,
                "metadata_path": f"{table_base_path}metadata/",
                "source_format": source_format,
                "target_formats": target_formats,
                "sync_mode": sync_mode,
            }
        }

    except subprocess.TimeoutExpired:
        return {
            "statusCode": 500,
            "body": {
                "message": "Conversion timed out (>280s)",
                "table_base_path": table_base_path,
                "error": "timeout",
            }
        }
    except Exception as e:
        print(f"XTable conversion failed: {e}")
        return {
            "statusCode": 500,
            "body": {
                "message": f"Conversion failed: {str(e)[:500]}",
                "table_base_path": table_base_path,
                "error": str(e)[:500],
            }
        }
