import os
import json
import tempfile
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import datetime
import tarfile

# Control flag
runGcloud = False

# Configuration
PROJECT_ID = os.environ.get("PROJECT_ID")
REGION = "europe-west2"
REPO = "imperago-metrics"
IMAGE_NAME = "daily_audit_v3"
TAG = f"{REGION}-docker.pkg.dev/{PROJECT_ID}/{REPO}/{IMAGE_NAME}:latest"
JOB_NAME = "daily-audit-v3"
BUCKET_NAME = f"gcf-artifacts-{PROJECT_ID}"  # Must exist
SOURCE_TAR = "source.tar.gz"

# Step 1: Get secret
GOOGLE_CLOUD_SECRET = os.environ.get("GOOGLE_CLOUD_SECRET")
assert GOOGLE_CLOUD_SECRET, "GOOGLE_CLOUD_SECRET is not set"

with tempfile.NamedTemporaryFile(mode="w+", delete=False) as temp_file:
    temp_file.write(GOOGLE_CLOUD_SECRET)
    temp_file.flush()
    SERVICE_ACCOUNT_PATH = temp_file.name

# Step 2: Package source code
def create_tarball():
    print("📦 Creating tarball with Python tarfile module...")
    with tarfile.open(SOURCE_TAR, "w:gz") as tar:
        for root, dirs, files in os.walk("."):
            if "__pycache__" in root or ".git" in root:
                continue
            for file in files:
                if file.endswith(".tar.gz"):
                    continue
                filepath = os.path.join(root, file)
                arcname = os.path.relpath(filepath, ".")
                tar.add(filepath, arcname=arcname)
    print(f"✅ Created {SOURCE_TAR}")

# Step 3: Upload to GCS
def upload_source(credentials):
    print("📤 Uploading tarball to GCS...")

    storage = build("storage", "v1", credentials=credentials)
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d-%H%M%S")
    object_name = f"cloudbuild/source-{timestamp}.tar.gz"

    media = MediaFileUpload(SOURCE_TAR, resumable=True)
    request = storage.objects().insert(bucket=BUCKET_NAME, name=object_name, media_body=media)
    response = request.execute()
    print("✅ Tarball uploaded to gcs bucket")
    return object_name

# Step 4: Trigger Cloud Build
def trigger_cloud_build(credentials, object_name):
    print("🔨 Triggering Cloud Build...")
    cloudbuild = build("cloudbuild", "v1", credentials=credentials)

    build_request = {
        "source": {
            "storageSource": {
                "bucket": BUCKET_NAME,
                "object": object_name,
            }
        },
        "steps": [
            {
                "name": "gcr.io/cloud-builders/docker",
                "args": ["build", "-t", TAG, "."]
            },
            {
                "name": "gcr.io/cloud-builders/docker",
                "args": ["push", TAG]
            }
        ],
        "images": [TAG]
    }

    build_op = cloudbuild.projects().builds().create(projectId=PROJECT_ID, body=build_request).execute()
    print("✅ Cloud Build started. Build ID:", build_op["metadata"]["build"]["id"])

# Step 5: Update Job (without running)
def update_job_only(credentials):
    run_client = build("run", "v2", credentials=credentials)
    name = f"projects/{PROJECT_ID}/locations/{REGION}/jobs/{JOB_NAME}"
    
    try:
        job = run_client.projects().locations().jobs().get(name=name).execute()
        updated = False

        if "spec" in job:
            try:
                if "template" in job["spec"]:
                    template = job["spec"]["template"]
                    if "spec" in template and "template" in template["spec"]:
                        containers = template["spec"]["template"]["spec"]["containers"]
                        containers[0]["image"] = TAG

                        # ✅ Inject PROJECT_ID env var
                        containers[0].setdefault("env", [])
                        env_vars = {env["name"]: env for env in containers[0]["env"]}
                        env_vars["PROJECT_ID"] = {"name": "PROJECT_ID", "value": PROJECT_ID}
                        containers[0]["env"] = list(env_vars.values())

                        updated = True
                        print("✅ Updated using v2 spec path")
                    elif "template" in template:
                        containers = template["template"]["spec"]["containers"]
                        containers[0]["image"] = TAG

                        # ✅ Inject PROJECT_ID env var
                        containers[0].setdefault("env", [])
                        env_vars = {env["name"]: env for env in containers[0]["env"]}
                        env_vars["PROJECT_ID"] = {"name": "PROJECT_ID", "value": PROJECT_ID}
                        containers[0]["env"] = list(env_vars.values())

                        updated = True
                        print("✅ Updated using alternative v2 spec path")
            except KeyError as e:
                print(f"Failed v2 spec path: {e}")

        if not updated and "template" in job:
            try:
                template = job["template"]
                if "template" in template and "containers" in template["template"]:
                    containers = template["template"]["containers"]
                    containers[0]["image"] = TAG

                    # ✅ Inject PROJECT_ID env var
                    containers[0].setdefault("env", [])
                    env_vars = {env["name"]: env for env in containers[0]["env"]}
                    env_vars["PROJECT_ID"] = {"name": "PROJECT_ID", "value": PROJECT_ID}
                    containers[0]["env"] = list(env_vars.values())

                    updated = True
                    print("✅ Updated using v1 template path")
            except KeyError as e:
                print(f"Failed v1 template path: {e}")

        if not updated:
            print("❌ Could not find container image path in job structure")
            print("Full job structure:")
            print(json.dumps(job, indent=2))
            return

        run_client.projects().locations().jobs().patch(
            name=name,
            body=job
        ).execute()
        print("✅ Cloud Run job updated (not executed)")
        
    except Exception as e:
        print(f"❌ Error updating job: {e}")
        raise

# Step 6: Update and Run Job
def update_and_run_job(credentials):
    run_client = build("run", "v2", credentials=credentials)
    name = f"projects/{PROJECT_ID}/locations/{REGION}/jobs/{JOB_NAME}"
    
    try:
        job = run_client.projects().locations().jobs().get(name=name).execute()
        print("🔍 Job structure keys:", list(job.keys()))
        updated = False

        if "spec" in job:
            try:
                if "template" in job["spec"]:
                    template = job["spec"]["template"]
                    if "spec" in template and "template" in template["spec"]:
                        containers = template["spec"]["template"]["spec"]["containers"]
                        containers[0]["image"] = TAG

                        # ✅ Inject PROJECT_ID env var
                        containers[0].setdefault("env", [])
                        env_vars = {env["name"]: env for env in containers[0]["env"]}
                        env_vars["PROJECT_ID"] = {"name": "PROJECT_ID", "value": PROJECT_ID}
                        containers[0]["env"] = list(env_vars.values())

                        updated = True
                        print("✅ Updated using v2 spec path")
                    elif "template" in template:
                        containers = template["template"]["spec"]["containers"]
                        containers[0]["image"] = TAG

                        # ✅ Inject PROJECT_ID env var
                        containers[0].setdefault("env", [])
                        env_vars = {env["name"]: env for env in containers[0]["env"]}
                        env_vars["PROJECT_ID"] = {"name": "PROJECT_ID", "value": PROJECT_ID}
                        containers[0]["env"] = list(env_vars.values())

                        updated = True
                        print("✅ Updated using alternative v2 spec path")
            except KeyError as e:
                print(f"Failed v2 spec path: {e}")

        if not updated and "template" in job:
            try:
                template = job["template"]
                if "template" in template and "containers" in template["template"]:
                    containers = template["template"]["containers"]
                    containers[0]["image"] = TAG

                    # ✅ Inject PROJECT_ID env var
                    containers[0].setdefault("env", [])
                    env_vars = {env["name"]: env for env in containers[0]["env"]}
                    env_vars["PROJECT_ID"] = {"name": "PROJECT_ID", "value": PROJECT_ID}
                    containers[0]["env"] = list(env_vars.values())

                    updated = True
                    print("✅ Updated using v1 template path")
            except KeyError as e:
                print(f"Failed v1 template path: {e}")

        if not updated:
            print("❌ Could not find container image path in job structure")
            print("Full job structure:")
            print(json.dumps(job, indent=2))
            return

        run_client.projects().locations().jobs().patch(
            name=name,
            body=job
        ).execute()
        print("✅ Cloud Run job updated")

        response = run_client.projects().locations().jobs().run(name=name, body={}).execute()
        print("🚀 Job execution started.")
        execution_name = response.get("metadata", {}).get("name", "")
        if execution_name:
            print(f"📋 Execution name: {execution_name.split('/')[-1]}")
            print(f"🔗 Logs: {response.get('metadata', {}).get('logUri', 'N/A')}")
        print(json.dumps(response, indent=2))
        
    except Exception as e:
        print(f"❌ Error updating/running job: {e}")
        raise


if __name__ == "__main__":
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    try:
        create_tarball()
        object_name = upload_source(credentials)
        trigger_cloud_build(credentials, object_name)
        
        if runGcloud:
            print("🚀 runGcloud is TRUE - Updating and running Cloud Run job...")
            update_and_run_job(credentials)
        else:
            print("🔧 runGcloud is FALSE - Updating job but not running...")
            update_job_only(credentials)
            
    finally:
        # Clean up: Delete the tarball
        if os.path.exists(SOURCE_TAR):
            os.remove(SOURCE_TAR)
            print(f"🗑️  Cleaned up: Deleted {SOURCE_TAR}")
        
        # Clean up: Delete the temporary service account file
        if os.path.exists(SERVICE_ACCOUNT_PATH):
            os.remove(SERVICE_ACCOUNT_PATH)
            print(f"🗑️  Cleaned up: Deleted temporary service account file")