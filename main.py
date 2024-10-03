import os
import logging
import settings
import requests
import functions_framework
from flask import Flask, request

import utils
from utils import *

logging.basicConfig(level=logging.INFO)


@functions_framework.http
def data_validator(request):
    settings.initialize_env_securities()

    from secret_services import secret_services
    from storage_services import StorageServices
    from redivis_services import RedivisServices
    from entity_controller import EntityController
    logging.info(
        f"running version {settings.config['VERSION']}, project_id: {os.environ.get('project_id', None)}, instance: {settings.config['INSTANCE']}")
    os.environ["REDIVIS_API_TOKEN"] = secret_services.access_secret_version(
        secret_id=settings.config['REDIVIS_API_TOKEN_SECRET_ID'],
        version_id="latest")

    admin_api_key = secret_services.access_secret_version(secret_id=settings.config['VALIDATOR_API_SECRET_ID'],
                                                          version_id="latest")
    admin_api_key = admin_api_key.strip().lower()

    # Sanitize API Keys
    api_key = request.headers.get('API-Key')
    api_key = api_key.strip().lower()
    #
    if api_key != admin_api_key:
        return 'Invalid API Key', 403

    if request.method == 'POST':
        request_json = request.get_json(silent=True)
        if request_json:

            try:
                dataset_parameters = utils.DatasetParameters(**request_json)
            except Exception as e:
                return str(e), 400

            results = []

            if dataset_parameters:
                storage = StorageServices(dataset_id=dataset_parameters.dataset_id)

                valid_data = {}
                invalid_data = []
                validation_logs = []
                if dataset_parameters.prefix:  # if prefix_name specified, go to uploading_to_redivis process.
                    storage.storage_prefix = dataset_parameters.prefix
                else:  # if no prefix_name specified, start validation process.
                    logging.info(
                        f'Syncing data from Firestore to Redivis for orgs: {dataset_parameters.orgs}.')
                    for org in dataset_parameters.orgs:
                        logging.info(f'Getting data from Firestore for org_id: {org.org_id}.')
                        ec = EntityController(org=org)

                        ec.set_values_from_firestore()
                        logging.info(f"validation_log_list: {ec.validation_log}")
                        valid_data = merge_dictionaries(valid_data, ec.get_valid_data())
                        invalid_data = invalid_data + ec.get_invalid_data()
                        validation_logs.append(ec.validation_log)

                    # GCP storage service
                    if dataset_parameters.is_save_to_storage:
                        logging.info(f"Saving data to GCP storage for dataset_id: {dataset_parameters.dataset_id}.")
                        storage.process(valid_data=valid_data, invalid_data=invalid_data)
                        logging.info(f"upload_to_GCP_log_list: {storage.upload_to_GCP_log}")
                    else:
                        output = {'title': f'Function executed successfully!',
                                  'valid_users_count': len(valid_data.get('users', [])),
                                  'valid_runs_count': len(valid_data.get('runs', [])),
                                  'valid_trials_count': len(valid_data.get('trials', [])),
                                  'validation_logs': validation_logs,
                                  'invalid_results': invalid_data}
                        results.append(output)

                # redivis service
                if dataset_parameters.is_upload_to_redivis:
                    logging.info(f"Uploading data to Redivis for dataset_id: {dataset_parameters.dataset_id}.")
                    rs = RedivisServices()
                    rs.set_dataset(dataset_id=dataset_parameters.dataset_id)
                    rs.create_dateset_version(params=dataset_parameters.orgs)
                    file_names = storage.list_blobs_with_prefix()
                    for file_name in file_names:
                        rs.save_to_redivis_table(file_name=file_name)

                    if dataset_parameters.is_release_to_redivis:
                        rs.release_dataset(params=dataset_parameters.orgs)
                    logging.info(f"upload_to_redivis_log_list: {rs.upload_to_redivis_log}")
                    output = {
                        'title': f'Function executed successfully! Current DS has {rs.count_tables()} tables.',
                        'redivis_logs': rs.upload_to_redivis_log,
                        'validation_logs': validation_logs,
                    }
                    results.append(output)
                elif dataset_parameters.is_save_to_storage and not dataset_parameters.prefix:
                    output = {'title': f'Function executed successfully! Data uploaded to GCP storage only.',
                              'gcp_logs': storage.upload_to_GCP_log,
                              'validation_logs': validation_logs,
                              }
                    results.append(output)
                else:
                    pass
            response = {'status': 'success', 'logs': results}
            logging.info(json.dumps(response))
            return json.dumps(response), 200
        else:
            return 'Request body is not received properly', 500
    else:
        return 'Function needs to receive POST request', 500


# This cloud function is responsible for triggering the data_validator cloud function
# It can be triggered by a POST request with a list of lab_ids for testing
# Or it can be triggered by a scheduled job to run on a regular basis
# Using Firestore to get the list of lab_ids to pass to the data_validator
def data_validator_trigger(http_request=None):
    logging.info(f"running version {settings.config['VERSION']}, project_id: {os.environ.get('project_id', None)}")

    client = get_secret_manager_client()
    admin_service_account = get_secret(settings.config['ADMIN_SERVICE_ACCOUNT_SECRET_ID'], client)
    admin_public_key = get_secret(settings.config['ADMIN_PUBLIC_KEY_SECRET_ID'], client)
    data_validator_url = get_secret(settings.config['DATA_VALIDATOR_URL_SECRET_ID'], client)
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
