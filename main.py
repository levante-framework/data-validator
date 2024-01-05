import settings
from firestore_services import EntityController, upload_blob_from_memory
from datetime import datetime
from google.cloud import firestore
import pandas as pd
import json


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


def save_data(ec: EntityController, lab_id: str):
    valid_dict = {'districts': [obj.model_dump() for obj in ec.valid_districts],
                  'schools': [obj.model_dump() for obj in ec.valid_schools],
                  'classes': [obj.model_dump() for obj in ec.valid_classes],
                  'users': [obj.model_dump() for obj in ec.valid_users],
                  'runs': [obj.model_dump() for obj in ec.valid_runs],
                  'trials': [obj.model_dump() for obj in ec.valid_trials],
                  'assignments': [obj.model_dump() for obj in ec.valid_assignments],
                  'tasks': [obj.model_dump() for obj in ec.valid_tasks],
                  'variants': [obj.model_dump() for obj in ec.valid_variants],
                  'variants_params': [obj.model_dump() for obj in ec.valid_variants_params],
                  'user_classes': [obj.model_dump() for obj in ec.valid_user_class],
                  'user_assignments': [obj.model_dump() for obj in ec.valid_user_assignment],
                  'assignment_tasks': [obj.model_dump() for obj in ec.valid_assignment_task]}

    invalid_dict = {'districts': [obj for obj in ec.invalid_districts],
                    'schools': [obj for obj in ec.invalid_schools],
                    'classes': [obj for obj in ec.invalid_classes],
                    'users': [obj for obj in ec.invalid_users],
                    'runs': [obj for obj in ec.invalid_runs],
                    'trials': [obj for obj in ec.invalid_trials],
                    'assignments': [obj for obj in ec.invalid_assignments],
                    'tasks': [obj for obj in ec.invalid_tasks],
                    'variants': [obj for obj in ec.invalid_variants],
                    'variants_params': [obj for obj in ec.invalid_variants_params],
                    'user_classes': [obj for obj in ec.invalid_user_class],
                    'user_assignments': [obj for obj in ec.invalid_user_assignment],
                    'assignment_tasks': [obj for obj in ec.invalid_assignment_task]
                    }
    valid_data = json.dumps(valid_dict, cls=CustomJSONEncoder)
    invalid_data = json.dumps(invalid_dict, cls=CustomJSONEncoder)
    if settings.SAVE_TO_STORAGE:
        try:
            upload_blob_from_memory(bucket_name=settings.BUCKET_NAME, data=valid_data,
                                    destination_blob_name=f"District_{lab_id}_valid_data_{datetime.now()}.json",
                                    content_type='application/json')
        except Exception as e:
            print(f"Failed to save valid roar data to cloud, {lab_id}, {e}")

        try:
            upload_blob_from_memory(bucket_name=settings.BUCKET_NAME, data=invalid_data,
                                    destination_blob_name=f"District_{lab_id}_invalid_data_{datetime.now()}.json",
                                    content_type='application/json')
        except Exception as e:
            print(f"Failed to save invalid roar data to cloud, {lab_id}, {e}")
    else:
        with open('valid_data.json', 'w') as file:
            file.write(valid_data)
        with open('invalid_data.json', 'w') as file:
            file.write(invalid_data)

def main():
    lab_id = '61e8aee84cf0e71b14295d45'
    ec = EntityController(lab_id=lab_id)
    save_data(ec, lab_id)
    # runs_dict = [obj.model_dump() for obj in ec.valid_runs]
    # runs_df = pd.DataFrame(runs_dict)
    # runs_df.to_csv('output.csv')
    # print(len(ec.valid_users))
    # print(len(ec.invalid_users))
    # print(len(ec.valid_runs))
    # print(len(ec.invalid_runs))
    # print(len(ec.valid_trials))
    # print(len(ec.invalid_trials))
    # print(len(ec.invalid_user_class))


if __name__ == "__main__":
    main()
