import logging
import time
import json
import settings
from flask import Flask, request, jsonify
import utils

utils.setup_project_environment()

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)


def process(req):
    start_time = time.time()
    from secret_services import secret_service
    from entity_controller import EntityController
    from firestore_services import firestore_services
    from storage_services import StorageServices
    from redivis_services import RedivisServices

    admin_api_key = secret_service.get_secret_payload(
        secret_id=settings.config['VALIDATOR_API_SECRET_ID'],
        version_id="latest").strip().lower()

    # Sanitize API Keys
    api_key = req.headers.get('API-Key')
    api_key = api_key.strip().lower()
    #
    if api_key != admin_api_key:
        return 'Invalid API Key', 403

    if req.method == 'POST':
        request_json = req.get_json(silent=True)
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
                new_version_release = False
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
                    'new_schemas': {"runs": [], "trials": [], "surveys": []},
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
                    total_validation_stats['survey_responses']['student'] += org_validation_stats['survey_responses'][
                        'student']
                    total_validation_stats['survey_responses']['teacher'] += org_validation_stats['survey_responses'][
                        'teacher']
                    total_validation_stats['survey_responses']['caregiver'] += org_validation_stats['survey_responses'][
                        'caregiver']
                    total_validation_stats['invalid_data_count'] += org_validation_stats['invalid_data_count']
                    total_validation_stats['new_schemas']['runs'].extend(ec.new_schemas['runs'])
                    total_validation_stats['new_schemas']['trials'].extend(ec.new_schemas['trials'])
                    total_validation_stats['new_schemas']['surveys'].extend(ec.new_schemas['surveys'])
                    validated_data = utils.merge_dictionaries(validated_data, org_validated_data)

                validated_data = utils.reduce_duplication_by_keys(data=validated_data,
                                                                  keys={'administrations': 'administration_id',
                                                                        'groups': 'group_id',
                                                                        'tasks': 'task_id',
                                                                        'variants': 'variant_id',
                                                                        'users': 'user_id',
                                                                        'runs': 'run_id',
                                                                        'trials': 'trial_id',
                                                                        })
                # GCP storage service
                if not dataset_parameters.is_save_to_storage:
                    elapsed_time = time.time() - start_time
                    output = {'title': 'Function executed successfully! Nothing uploaded to GCP or Redivis.',
                              'elapsed_time': elapsed_time,
                              'is_save_to_storage': dataset_parameters.is_save_to_storage,
                              'is_force_uploading_to_redivis': dataset_parameters.is_force_uploading_to_redivis,
                              'total_validation_stats': total_validation_stats,
                              }
                    logging.info(json.dumps(output, cls=utils.CustomJSONEncoder))
                    return json.dumps(output, cls=utils.CustomJSONEncoder), 200
                else:  # Start to process on GCP and redivis
                    logging.info(f"Saving data to GCP storage for dataset_id: {dataset_parameters.dataset_id}.")

                    storage = StorageServices(cred=firestore_services.admin_credentials,
                                              dataset_id=dataset_parameters.dataset_id,
                                              is_forced_uploading_redivis=dataset_parameters.is_force_uploading_to_redivis)
                    storage.process(validated_data=validated_data)

                    # redivis service
                    if storage.is_new_version_needed:
                        new_version_release = storage.is_new_version_needed
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

                            exception_tables = ['invalid_data']
                            for table_name in exception_tables:
                                if table_name in table_names_in_redivis and table_name not in table_names_in_gcp_bucket:
                                    rs.delete_table(table_name=table_name)

                            rs.release_dataset(params=dataset_parameters.to_dict())

                        rs.upload_to_redivis_log['table_counts'] = rs.count_tables()
                        output = {
                            'total_validation_stats': total_validation_stats,
                            'gcp_logs': storage.upload_to_GCP_log,
                            'redivis_logs': rs.upload_to_redivis_log,
                        }
                    else:
                        output = {
                            'total_validation_stats': total_validation_stats,
                            'gcp_logs': storage.upload_to_GCP_log,
                        }
                elapsed_time = time.time() - start_time
                response = {
                    'dataset_parameters': dataset_parameters.to_dict(),
                    'logs': output,
                    'elapsed_time': elapsed_time,
                    'api_version': settings.config['VERSION'],
                    'new_version_release': new_version_release,
                }
                logging.info(json.dumps(response))
                firestore_services.set_logs_to_firebase(response=response, dataset_id=dataset_parameters.dataset_id)

                notification_mode = dataset_parameters.slack_notification_mode.lower()

                if notification_mode != 'none':
                    if new_version_release:
                        utils.notify_slack(message=json.dumps(response))
                    elif any(total_validation_stats['new_schemas'].values()):
                        utils.notify_slack(
                            message=json.dumps(
                                response if notification_mode == 'full' else total_validation_stats['new_schemas'])
                        )

                return json.dumps(response), 200
        else:
            return 'Request body is not received properly', 500
    else:
        return 'Function needs to receive POST request', 500


# Cloud Functions entry point
def data_validator(request):  # GCP will call this directly
    return process(request)


# Flask route for local testing
@app.route('/', methods=['POST'])
def local_run():
    return process(request)


if __name__ == "__main__":
    app.run(port=8080)
