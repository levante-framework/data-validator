import settings
from storage_services import StorageServices
from secret_services import SecretServices
from redivis_services import RedivisServices
import functions_framework
import os


@functions_framework.http
def data_validator(request):
    print(f"running version {settings.version}...")
    sec = SecretServices()
    os.environ['assessment_cred'] = sec.access_secret_version(secret_id=settings.assessment_service_account_secret_id, version_id="latest")
    admin_api_key = sec.access_secret_version(secret_id=settings.admin_firebase_api_key_secret_id, version_id="latest")

    api_key = request.headers.get('API-Key')
    if api_key != admin_api_key:
        return 'Invalid API Key', 403

    if request.method == 'POST':
        request_json = request.get_json(silent=True)
        if request_json:
            lab_id = request_json.get('lab_id', None)
            is_from_firestore = request_json.get('is_from_firestore', False)
            is_save_to_storage = request_json.get('is_save_to_storage', False)
            is_upload_to_redivis = request_json.get('is_upload_to_redivis', False)
            is_release_on_redivis = request_json.get('is_release_to_redivis', False)
            prefix_name = request_json.get('prefix_name', None)
            dataset_version = request_json.get('dataset_version', None)
            if params_check(lab_id, is_from_firestore, is_save_to_storage, prefix_name, is_upload_to_redivis, dataset_version, is_release_on_redivis):
                storage = StorageServices(lab_id=lab_id, is_from_firestore=is_from_firestore)
                if is_save_to_storage:
                    storage.process(dataset_version=dataset_version)
                else:
                    storage.storage_prefix = prefix_name

                if is_upload_to_redivis:
                    rs = RedivisServices(lab_id=lab_id, dataset_version=dataset_version, is_from_firestore=is_from_firestore)
                    rs.create_dateset_version()
                    file_names = storage.list_blobs_with_prefix()
                    for file_name in file_names:
                        rs.save_to_redivis_table(file_name=file_name)
                    print(f"Current DS has {rs.count_tables()} tables.")
                    if is_release_on_redivis:
                        rs.release_dataset()
                    return f'Function executed successfully!', 200
                else:
                    return f'Function executed successfully! Data not ship to redivis', 200
            else:
                return 'Missing significant parameter in the request body', 400
        else:
            return 'Request body is not received properly', 500
    else:
        return 'Function needs to receive POST request', 500


def params_check(lab_id, is_from_firestore, is_save_to_storage, prefix_name, is_upload_to_redivis, dataset_version, is_release_on_redivis):
    #return "Parameter 'source' has to be either firestore or redivis.", 400
    return True


# if __name__ == "__main__":
#     data_validator("asf")
