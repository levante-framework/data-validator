import os
import logging
import settings
import requests
import functions_framework
from flask import Flask, request
import time

import utils
from utils import *

logging.basicConfig(level=logging.INFO)


@functions_framework.http
def data_validator(request):
    start_time = time.time()
    settings.initialize_env_securities()

    from firestore_services import FirestoreServices
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
            if not dataset_parameters:
                return 'No dataset params are given.', 400
            else:
                validated_data = {}
                total_validation_stats = {
                    'groups': 0,
                    'administrations': 0,
                    'users': {
                        'total': 0,
                        'valid_users': 0
                    },
                    'runs': {
                        'total': 0,
                        'valid_runs': 0
                    },
                    'trials': {
                        'total': 0,
                        'valid_trials': 0
                    },
                    'survey_responses': {
                        'student': 0,
                        'teacher': 0,
                        'caregiver': 0,
                    },
                    'invalid_data_count': 0,
                    'orgs': {}
                }
                logging.info(
                    f'Syncing data from Firestore to Redivis for orgs: {dataset_parameters.orgs}.')
                # Processing validation
                for org in dataset_parameters.orgs:
                    logging.info(f'Getting data from Firestore for org_id: {org.org_id}.')
                    ec = EntityController(org=org)
                    ec.validate_data_from_firestore()
                    org_validated_data = ec.get_validated_data()

                    org_validation_stats = {
                        'groups': len(ec.valid_groups) + len(ec.invalid_groups),
                        'administrations': len(ec.valid_administrations) + len(ec.invalid_administrations),
                        'users': {
                            'total': len(ec.valid_users) + len(ec.invalid_users),
                            'valid_users': sum(1 for user in ec.valid_users if user.valid_user)
                        },
                        'runs': {
                            'total': len(ec.valid_runs) + len(ec.invalid_runs),
                            'valid_runs': sum(1 for run in ec.valid_runs if run.valid_run)
                        },
                        'trials': {
                            'total': len(ec.valid_trials) + len(ec.invalid_trials),
                            'valid_trials': sum(1 for trial in ec.valid_trials if trial.valid_trial)
                        },
                        'survey_responses': ec.survey_responses_stats,
                        'invalid_data_count': len(org_validated_data.get('invalid_data', [])),
                    }
                    total_validation_stats['orgs'][org.org_id] = org_validation_stats

                    total_validation_stats['groups'] += org_validation_stats['groups']
                    total_validation_stats['administrations'] += org_validation_stats['administrations']
                    total_validation_stats['users']['total'] += org_validation_stats['users']['total']
                    total_validation_stats['users']['valid_users'] += org_validation_stats['users'][
                        'valid_users']
                    total_validation_stats['runs']['total'] += org_validation_stats['runs']['total']
                    total_validation_stats['runs']['valid_runs'] += org_validation_stats['runs']['valid_runs']
                    total_validation_stats['trials']['total'] += org_validation_stats['trials']['total']
                    total_validation_stats['trials']['valid_trials'] += org_validation_stats['trials'][
                        'valid_trials']
                    total_validation_stats['survey_responses']['student'] += org_validation_stats['survey_responses']['student']
                    total_validation_stats['survey_responses']['teacher'] += org_validation_stats['survey_responses']['teacher']
                    total_validation_stats['survey_responses']['caregiver'] += org_validation_stats['survey_responses']['caregiver']
                    total_validation_stats['invalid_data_count'] += org_validation_stats['invalid_data_count']

                    validated_data = merge_dictionaries(validated_data, org_validated_data)

                validated_data = reduce_duplication_by_keys(data=validated_data,
                                                            keys={'tasks': 'task_id', 'variants': 'variant_id'})
                # GCP storage service
                if not dataset_parameters.is_save_to_storage:
                    elapsed_time = time.time() - start_time
                    output = {'title': 'Function executed successfully! Nothing uploaded to GCP or Redivis.',
                              'elapsed_time': elapsed_time,
                              'is_save_to_storage': dataset_parameters.is_save_to_storage,
                              'is_force_uploading_to_redivis': dataset_parameters.is_force_uploading_to_redivis,
                              'total_validation_stats': total_validation_stats,
                              }
                    logging.info(json.dumps(output))
                    return json.dumps(output), 200
                else:  # Start to process on GCP and redivis
                    logging.info(f"Saving data to GCP storage for dataset_id: {dataset_parameters.dataset_id}.")
                    storage = StorageServices(dataset_id=dataset_parameters.dataset_id,
                                              is_forced_uploading_redivis=dataset_parameters.is_force_uploading_to_redivis)
                    storage.process(validated_data=validated_data)

                    # redivis service
                    if storage.is_new_version_needed:
                        logging.info(f"Uploading data to Redivis for dataset_id: {dataset_parameters.dataset_id}.")
                        rs = RedivisServices()
                        rs.set_dataset(dataset_id=dataset_parameters.dataset_id)
                        rs.create_dateset_version(params=dataset_parameters.to_dict()['orgs'])
                        if rs.upload_to_redivis_log['dataset_fails']:
                            logging.info("Process stops at create_dateset_version.")
                        else:
                            file_names = storage.list_blobs_with_prefix()
                            logging.info(f"GCP bucket {dataset_parameters.dataset_id} has files {file_names}.")

                            for file_name in file_names:
                                rs.save_to_redivis_table(file_name=file_name)

                            # Remove archive or deleted tables from Redivis
                            table_names_in_redivis = [table.name for table in rs.dataset.list_tables()]
                            table_names_in_gcp_bucket = [name.split('/')[-1].split('.')[0] for name in file_names]

                            for name in table_names_in_redivis:
                                if name not in table_names_in_gcp_bucket:
                                    rs.delete_table(table_name=name)

                            rs.release_dataset(params=dataset_parameters.to_dict())

                        rs.upload_to_redivis_log['table_counts'] = rs.count_tables()
                        output = {
                            'validation_logs': total_validation_stats,
                            'gcp_logs': storage.upload_to_GCP_log,
                            'redivis_logs': rs.upload_to_redivis_log,
                        }
                    else:
                        output = {
                            'validation_logs': total_validation_stats,
                            'gcp_logs': storage.upload_to_GCP_log,
                        }
                elapsed_time = time.time() - start_time
                response = {'dataset_parameters': dataset_parameters.to_dict(),
                            'logs': output,
                            'elapsed_time': elapsed_time,
                            'api_version': settings.config['VERSION']}
                logging.info(json.dumps(response))
                fs_admin = FirestoreServices(app_name='admin_site')
                fs_admin.set_logs_to_firebase(response=response, dataset_id=dataset_parameters.dataset_id)
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
