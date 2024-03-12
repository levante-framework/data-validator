import settings
from storage_services import StorageServices
from secret_services import SecretServices
from redivis_services import RedivisServices
from entity_controller import EntityController
import functions_framework
import os
import logging

# Configure logging to show debug messages
# logging.basicConfig(level=logging.DEBUG)


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
            if params_check(lab_id, is_from_firestore, is_save_to_storage, is_upload_to_redivis, is_release_on_redivis, prefix_name):
                ec = EntityController(is_from_firestore=is_from_firestore)
                if not prefix_name:
                    if is_from_firestore:
                        ec.set_values_from_firestore(lab_id=lab_id)
                    elif lab_id != 'all':
                        ec.set_values_for_consolidate()
                    else:
                        ec.set_values_from_redivis(lab_id=lab_id, is_consolidate=False)
                    print(f"validation_log_list: {ec.validation_log}")

                storage = StorageServices(lab_id=lab_id, is_from_firestore=is_from_firestore)
                if prefix_name:
                    storage.storage_prefix = prefix_name
                elif is_save_to_storage:
                    storage.process(valid_data=ec.get_valid_data(), invalid_data=ec.get_invalid_data())
                    print(f"upload_to_GCP_log_list: {storage.upload_to_GCP_log}")
                else:
                    output = {'title': f'Function executed successfully! Here is the invalid data columns.',
                              'logs': ec.validation_log,
                              'data': ec.get_invalid_data()}
                    return output, 200

                if is_upload_to_redivis:
                    rs = RedivisServices(is_from_firestore=is_from_firestore)
                    rs.set_dataset(lab_id=lab_id)
                    rs.create_dateset_version()
                    # rs.save_to_redivis_table(file_name="lab_guests_firestore_2024-03-11-19-23-32/trials.json")
                    file_names = storage.list_blobs_with_prefix()
                    for file_name in file_names:
                        rs.save_to_redivis_table(file_name=file_name)
                    if is_release_on_redivis:
                        rs.release_dataset()
                    print(f"upload_to_redivis_log_list: {rs.upload_to_redivis_log}")
                    output = {'title': f'Function executed successfully! Current DS has {rs.count_tables()} tables.',
                              'logs': rs.upload_to_redivis_log}
                    return output, 200
                else:
                    output = {'title': f'Function executed successfully! Data has not been shipped to redivis.',
                              'logs': storage.upload_to_GCP_log}
                    return output, 200
            else:
                return 'Missing significant parameter in the request body', 400
        else:
            return 'Request body is not received properly', 500
    else:
        return 'Function needs to receive POST request', 500


def params_check(lab_id, is_from_firestore, is_save_to_storage, is_upload_to_redivis, is_release_on_redivis, prefix_name):
    if not lab_id:
        return "Parameter 'lab_id' needs to be specified.", 400
    elif not isinstance(lab_id, str):
        return "Parameter 'lab_id' needs to be a valid string.", 400
    if not isinstance(is_from_firestore, bool):
        return "Parameter 'is_from_firestore' has to be a bool value.", 400
    if not isinstance(is_save_to_storage, bool):
        return "Parameter 'is_save_to_storage' has to be a bool value.", 400
    if not isinstance(is_upload_to_redivis, bool):
        return "Parameter 'is_upload_to_redivis' has to be a bool value.", 400
    if not isinstance(is_release_on_redivis, bool):
        return "Parameter 'is_release_on_redivis' has to be a bool value.", 400

    return True


# if __name__ == "__main__":
#     data_validator("asf")
