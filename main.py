import settings
from storage_services import StorageServices
from secret_services import SecretServices
from redivis_services import RedivisServices

from flask import jsonify
import functions_framework
import json


@functions_framework.http
def data_validator(request):
    print(f"running version {settings.version}...")
    sec = SecretServices()
    assessment_cred = json.loads(
        sec.access_secret_version(secret_id=settings.assessment_service_account_secret_id, version_id=1))
    admin_api_key = sec.access_secret_version(secret_id=settings.admin_firebase_api_key_secret_id, version_id=1)

    api_key = request.headers.get('API-Key')
    if api_key != admin_api_key:
        return 'Invalid API Key', 403

    if request.method == 'POST':
        request_json = request.get_json(silent=True)
        if request_json:
            if 'lab_id' in request_json:
                lab_id = request_json['lab_id']
            else:
                return jsonify({"error": "Missing parameter lab_id"}), 400
            if 'source' in request_json:
                source = request_json['source']
            else:
                return jsonify({"error": "Missing parameter source"}), 400
            rs = RedivisServices(lab_id)
            if rs.dataset_version == 'next':
                return jsonify({"error": "This lab's current dataset has not been released, please release it then get next version."}), 400
            if source == "firestore":
                try:
                    storage = StorageServices()
                    storage.firestore_to_storage(lab_id=lab_id, assessment_cred=assessment_cred, source=source)
                    print("Function executed successfully!")
                    return f'Function executed successfully!', 200
                except Exception as e:
                    print(f"Function failed! {e}.")
                    return f'Function failed! {e}', 500
            elif source == "redivis":
                pass
    else:
        return 'Function needs to receive POST request', 500


# if __name__ == "__main__":
#     data_validator("asf")
