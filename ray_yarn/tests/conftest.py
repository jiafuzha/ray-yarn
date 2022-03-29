import pytest
import skein
import os
import sys
import time
import subprocess


@pytest.fixture(scope="session")
def conda_env():
    envpath = "dask-yarn-py%d%d.tar.gz" % sys.version_info[:2]
    if not os.path.exists(envpath):
        conda_pack = pytest.importorskip("conda_pack")
        conda_pack.pack(output=envpath, verbose=True)
    return envpath


@pytest.fixture(scope="session")
def skein_client():
    with skein.Client() as client:
        yield client


def check_is_shutdown(client, app_id, status="SUCCEEDED"):
    timeleft = 10
    report = client.application_report(app_id)
    while report.state not in ("FINISHED", "FAILED", "KILLED"):
        time.sleep(0.1)
        timeleft -= 0.1
        report = client.application_report(app_id)
        if timeleft < 0:
            client.kill_application(app_id)
            logs = get_logs(app_id)
            print(
                "Application wasn't properly terminated, killed by test fixture.\n"
                "\n"
                "Application Logs\n"
                "----------------\n"
                "%s" % logs
            )
            assert False, "Application wasn't properly terminated"

    if report.final_status != status:
        logs = get_logs(app_id)
        print(
            "Expected application to terminate with status==%s, got status==%s\n"
            "\n"
            "Application Logs\n"
            "----------------\n"
            "%s" % (status, report.final_status, logs)
        )
        assert report.final_status == status


def get_logs(app_id, tries=3):
    command = ["yarn", "logs", "-applicationId", app_id]
    for i in range(tries - 1):
        try:
            return subprocess.check_output(command).decode()
        except Exception:
            pass
        time.sleep(1)
    return subprocess.check_output(command).decode()