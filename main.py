import os
import logging
import settings
import requests
import functions_framework
from flask import Flask, request

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
            lab_ids = request_json.get('lab_ids', [])
            is_from_guest = request_json.get('is_from_guest', False)
            is_save_to_storage = request_json.get('is_save_to_storage', False)
            is_upload_to_redivis = request_json.get('is_upload_to_redivis', False)
            is_release_on_redivis = request_json.get('is_release_to_redivis', False)

            filter_by = None if not request_json.get('filter_by', None) else request_json.get('filter_by')
            filter_list = None if not request_json.get('filter_list', None) else request_json.get('filter_list')
            start_date = None if not request_json.get('start_date', None) else request_json.get('start_date')
            end_date = None if not request_json.get('end_date', None) else request_json.get('end_date')
            prefix_name = None if not request_json.get('prefix_name', None) else request_json.get('prefix_name')

            results = []
            job = 1
            valid, error_message = params_check(lab_ids=lab_ids,
                                                is_from_guest=is_from_guest,
                                                is_save_to_storage=is_save_to_storage,
                                                is_upload_to_redivis=is_upload_to_redivis,
                                                is_release_on_redivis=is_release_on_redivis,
                                                filter_by=filter_by,
                                                filter_list=filter_list,
                                                start_date=start_date,
                                                end_date=end_date)
            if not valid:
                return error_message, 400
            else:
                for lab_id in lab_ids:
                    logging.info(
                        f'Syncing data from Firestore to Redivis for lab_id: {lab_id}; job {job} of {len(lab_ids)}.')
                    if is_from_guest:
                        os.environ['guest_mode'] = "True"

                    storage = StorageServices(lab_id=lab_id)
                    ec = EntityController()
                    if prefix_name:  # if prefix_name specified, go to uploading_to_redivis process.
                        storage.storage_prefix = prefix_name
                    else:  # if no prefix_name specified, start validation process.
                        logging.info(f'Getting data from Firestore for lab_id: {lab_id}.')
                        ec.set_values_from_firestore(lab_id=lab_id,
                                                     start_date=start_date,
                                                     end_date=end_date,
                                                     filter_by=filter_by,
                                                     filter_list=filter_list)
                        logging.info(f"validation_log_list: {ec.validation_log}")

                        # GCP storage service
                        if is_save_to_storage:
                            logging.info(f"Saving data to GCP storage for lab_id: {lab_id}.")
                            storage.process(valid_data=ec.get_valid_data(), invalid_data=ec.get_invalid_data())
                            logging.info(f"upload_to_GCP_log_list: {storage.upload_to_GCP_log}")
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
                        rs = RedivisServices()
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
                        output = {
                            'title': f'Function executed successfully! Current DS has {rs.count_tables()} tables.',
                            'redivis_logs': rs.upload_to_redivis_log,
                            'validation_logs': ec.validation_log,
                        }
                        results.append(output)
                    elif is_save_to_storage and not prefix_name:
                        output = {'title': f'Function executed successfully! Data uploaded to GCP storage only.',
                                  'gcp_logs': storage.upload_to_GCP_log,
                                  'validation_logs': ec.validation_log,
                                  }
                        results.append(output)
                    else:
                        pass
                logging.info(f'Finished job {job} of {len(lab_ids)}.')
                job += 1
            response = {'status': 'success', 'logs': results}
            logging.info(response)
            return response, 200
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
