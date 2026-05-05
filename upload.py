import os
import sys
import json
import tempfile
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
import datetime
import tarfile

# Control flag
runGcloud = True

# Step 1: Set up credentials
SERVICE_ACCOUNT_PATH = None  # Initialize for cleanup
service_account_info = None  # Will hold the parsed JSON
PROJECT_ID = None  # Will be extracted from credentials

if os.getenv('GCP'):
    # Running locally - use GCP environment variable (as file path)
    print("🔑 Using local GCP credentials from file")
    gcp_file_path = os.getenv('GCP')
    
    with open(gcp_file_path, 'r') as f:
        service_account_info = json.load(f)
    
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    
    # Extract PROJECT_ID from service account JSON
    PROJECT_ID = service_account_info.get("project_id")
    
elif os.environ.get("GOOGLE_CLOUD_SECRET"):
    # Running in Codespaces - use GOOGLE_CLOUD_SECRET
    print("🔑 Using Codespaces credentials from GOOGLE_CLOUD_SECRET")
    GOOGLE_CLOUD_SECRET = os.environ.get("GOOGLE_CLOUD_SECRET")
    
    # Parse the JSON to extract service account info
    try:
        service_account_info = json.loads(GOOGLE_CLOUD_SECRET)
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse GOOGLE_CLOUD_SECRET as JSON")
        raise
    
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as temp_file:
        temp_file.write(GOOGLE_CLOUD_SECRET)
        temp_file.flush()
        SERVICE_ACCOUNT_PATH = temp_file.name
    
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    
    # Extract PROJECT_ID from service account JSON
    PROJECT_ID = service_account_info.get("project_id")
    
else:
    # Try Application Default Credentials (gcloud auth application-default login)
    print("🔑 Attempting to use Application Default Credentials")
    try:
        credentials, PROJECT_ID = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        print(f"✅ Using ADC with project: {PROJECT_ID}")
            
    except DefaultCredentialsError as e:
        print(f"❌ No credentials found. Please either:")
        print(f"   1. Set GCP environment variable: export GCP='/path/to/service-account.json'")
        print(f"   2. Set GOOGLE_CLOUD_SECRET for Codespaces")
        print(f"   3. Run: gcloud auth application-default login")
        raise

# Validate PROJECT_ID was extracted
if not PROJECT_ID:
    raise ValueError(
        "PROJECT_ID could not be extracted from credentials. "
        "Ensure your service account JSON contains 'project_id' field."
    )

print(f"✅ Using PROJECT_ID: {PROJECT_ID}")

# Configuration
REGION = "europe-west2"
REPO = "integration"
IMAGE_NAME = "integration"
TAG = f"{REGION}-docker.pkg.dev/{PROJECT_ID}/{REPO}/{IMAGE_NAME}:latest"
JOB_NAME = "adp-integrations"
JOB_NAME_2 = "event-driven-absences"
BUCKET_NAME = f"gcf-artifacts-{PROJECT_ID}"  # Must exist
SOURCE_TAR = "source.tar.gz"

# Step 2: Ensure all files are saved and synced before packaging
def flush_all_files():
    """Force the system to flush file buffers to disk before archiving."""
    print("💾 Flushing all file buffers before archiving...")
    try:
        # Flush Python-level buffers
        for obj in list(globals().values()):
            if hasattr(obj, "flush") and callable(obj.flush):
                try:
                    obj.flush()
                except Exception:
                    pass

        # Sync filesystem buffers
        if hasattr(os, "sync"):
            os.sync()
        print("✅ All files flushed and synced.")
    except Exception as e:
        print(f"⚠️ Warning: could not flush all files — {e}")

# Step 3: Package source code
def create_tarball():
    print("📦 Creating tarball with Python tarfile module...")
    flush_all_files()  # <-- always flush before archiving
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

# Step 4: Upload to GCS
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

# Step 5: Trigger Cloud Build
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

# Step 6: Update Job (without running)
def update_job_only(credentials,job_name):
    run_client = build("run", "v2", credentials=credentials)
    name = f"projects/{PROJECT_ID}/locations/{REGION}/jobs/{job_name}"
    
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

# Step 7: Update and Run Job
def update_and_run_job(credentials, job_name):
    run_client = build("run", "v2", credentials=credentials)
    name = f"projects/{PROJECT_ID}/locations/{REGION}/jobs/{job_name}"
    
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
    try:
        create_tarball()
        object_name = upload_source(credentials)
        trigger_cloud_build(credentials, object_name)
        
        for job in [JOB_NAME, JOB_NAME_2]:
            if runGcloud:
                print(f"🚀 Updating and running job: {job}")
                update_and_run_job(credentials, job)
            else:
                print(f"🔧 Updating job (not running): {job}")
                update_job_only(credentials, job)
    finally:
        # 🧹 Clean up artifacts
        if os.path.exists(SOURCE_TAR):
            os.remove(SOURCE_TAR)
            print(f"🗑️  Cleaned up: Deleted {SOURCE_TAR}")
        
        # Only clean up temp file if it was created (Codespaces mode)
        if SERVICE_ACCOUNT_PATH and os.path.exists(SERVICE_ACCOUNT_PATH):
            os.remove(SERVICE_ACCOUNT_PATH)
            print(f"🗑️  Cleaned up: Deleted temporary service account file")