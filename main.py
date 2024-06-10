import os
import logging
import settings
import requests
import functions_framework
from flask import Flask, request
from storage_services import StorageServices
from secret_services import SecretServices
from redivis_services import RedivisServices
from entity_controller import EntityController
from utils import *

logging.basicConfig(level=logging.INFO)


@functions_framework.http
def data_validator(request):
    logging.info(f"running version {settings.version}, ENV: {settings.ENV}, project_id: {os.environ.get('project_id', None)}")
    sec = SecretServices()
    os.environ['assessment_cred'] = sec.access_secret_version(secret_id=settings.assessment_service_account_secret_id,
                                                              version_id="latest")

    admin_api_key = sec.access_secret_version(secret_id=settings.admin_firebase_api_key_secret_id, version_id="latest")
    admin_api_key = admin_api_key.strip().lower()

    # Sanitize API Keys
    api_key = request.headers.get('API-Key')
    api_key = api_key.strip().lower()

    if api_key != admin_api_key:
        return 'Invalid API Key', 403

    if request.method == 'POST':
        request_json = request.get_json(silent=True)
        if request_json:
            lab_ids = request_json.get('lab_ids', [])
            is_from_guest = request_json.get('is_from_guest', False)
            if is_from_guest:
                os.environ['guest_mode'] = "True"

            is_from_firestore = request_json.get('is_from_firestore', False)
            is_save_to_storage = request_json.get('is_save_to_storage', False)
            is_upload_to_redivis = request_json.get('is_upload_to_redivis', False)
            is_release_on_redivis = request_json.get('is_release_to_redivis', False)
            prefix_name = request_json.get('prefix_name', None)
            # start_date = request_json.get('start_date', None) # '04/01/2024'
            # end_date = request_json.get('end_date', None)  # '04/01/2024'

            results = []
            job = 1
            for lab_id in lab_ids:
                logging.info(f'Syncing data from Firestore to Redivis for lab_id: {lab_id}; job {job} of {len(lab_ids)}.')
                if params_check(lab_id, is_from_firestore, is_save_to_storage,
                                is_upload_to_redivis, is_release_on_redivis,
                                prefix_name):
                    storage = StorageServices(lab_id=lab_id, is_from_firestore=is_from_firestore)
                    ec = EntityController(is_from_firestore=is_from_firestore)
                    if prefix_name:  # if prefix_name specified, go to uploading_to_redivis process.
                        storage.storage_prefix = prefix_name
                    else:  # if no prefix_name specified, start validation process.
                        if is_from_firestore:
                            logging.info(f'Getting data from Firestore for lab_id: {lab_id}.')
                            ec.set_values_from_firestore(lab_id=lab_id)
                        elif lab_id != 'all':
                            ec.set_values_for_consolidate()
                        else:
                            ec.set_values_from_redivis(lab_id=lab_id, is_consolidate=False)
                        # logging.info(f"validation_log_list: {ec.validation_log}")

                        # GCP storage service
                        if is_save_to_storage:
                            logging.info(f"Saving data to GCP storage for lab_id: {lab_id}.")
                            storage.process(valid_data=ec.get_valid_data(), invalid_data=ec.get_invalid_data())
                            # logging.info(f"upload_to_GCP_log_list: {storage.upload_to_GCP_log}")
                        else:
                            output = {'title': f'Function executed successfully!',
                                      'valid_users_count': len(ec.valid_users),
                                      'valid_runs_count': len(ec.valid_runs),
                                      'valid_trials_count': len(ec.valid_trials),
                                      'logs': ec.validation_log,
                                      'invalid_results': ec.get_invalid_data()}
                            results.append(output)
                    # redivis service
                    if is_upload_to_redivis:
                        logging.info(f"Uploading data to Redivis for lab_id: {lab_id}.")
                        rs = RedivisServices(is_from_firestore=is_from_firestore)
                        rs.set_dataset(lab_id=lab_id)
                        rs.create_dateset_version()
                        # rs.save_to_redivis_table(file_name="lab_guests_firestore_2024-03-11-19-23-32/trials.json")
                        file_names = storage.list_blobs_with_prefix()
                        for file_name in file_names:
                            rs.save_to_redivis_table(file_name=file_name)
                        if not ec.get_invalid_data():
                            rs.delete_table(table_name='validation_results')
                        if is_release_on_redivis:
                            rs.release_dataset()
                        logging.info(f"upload_to_redivis_log_list: {rs.upload_to_redivis_log}")
                        output = {'title': f'Function executed successfully! Current DS has {rs.count_tables()} tables.',
                                  'logs': rs.upload_to_redivis_log}
                        results.append(output)
                    elif is_save_to_storage and not prefix_name:
                        output = {'title': f'Function executed successfully! Data uploaded to GCP storage only.',
                                  'logs': storage.upload_to_GCP_log}
                        results.append(output)
                    else:
                        return 'Error in parameters setup in the request body', 400
                else:
                    return 'Error in parameters setup in the request body', 400
                logging.info(f'Finished job {job} of {len(lab_ids)}.')
                job += 1
            logging.info(f'Finished all jobs.\n{results}')
            return f'Finished all jobs.\n{results}', 200
        else:
            return 'Request body is not received properly', 500
    else:
        return 'Function needs to receive POST request', 500


# This cloud function is responsible for triggering the data_validator cloud function
# It can be triggered by a POST request with a list of lab_ids for testing
# Or it can be triggered by a scheduled job to run on a regular basis
# Using Firestore to get the list of lab_ids to pass to the data_validator
def data_validator_trigger(http_request=None):
    logging.info(f"running version {settings.version}, ENV: {settings.ENV}, project_id: "
                 f"{os.environ.get('project_id', None)}")

    client = get_secret_manager_client()
    admin_service_account = get_secret(settings.admin_service_account_secret_id, client)
    admin_public_key = get_secret(settings.admin_public_key_secret_id, client)
    data_validator_url = get_secret(settings.data_validator_url_secret_id, client)
    admin_app = initialize_firebase(admin_service_account)

    lab_ids = get_lab_ids(_request=http_request, app=admin_app)

    # This is the payload that will be sent to the data_validator
    payload = {
        "lab_ids": lab_ids,
        "is_from_guest": False,
        "is_from_firestore": True,
        "is_save_to_storage": True,
        "is_upload_to_redivis": True,
        "is_release_to_redivis": True,
    }

    headers = {
        'Content-Type': 'application/json',
        'API-Key': admin_public_key.strip()
    }

    response = requests.post(data_validator_url.strip(), json=payload, headers=headers)

    if response.status_code == 200:
        logging.info('data_validator triggered successfully.')
        return 'data_validator triggered successfully.', 200
    else:
        logging.error(f'Error triggering data_validator: {response.text}')
        return f'Error triggering data_validator: {response.text}', response.status_code


# Local testing with Flask and functions-framework
app = Flask(__name__)


# This is the payload that will be sent to the data_validator_trigger for testing
# This payload determines the behavior of the data_validator by looking for the presence
# Of is_test key in the payload
@app.route('/', methods=['POST'])
def main(data):
    return data_validator_trigger(data)


if __name__ == "__main__":
    app.run(debug=True)
