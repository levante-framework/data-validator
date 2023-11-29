from firestore_services import FirestoreServices
from core_models import School, Task, Variant
from pydantic import ValidationError
import json


def main():
    fs = FirestoreServices()
    tasks = fs.get_tasks
    tasks_list = []
    for task in tasks:
        try:
            variants = fs.get_variants(task_id=task['id'])
            variants_list = [Variant(**variant) for variant in variants]
            tasks_list.append(Task(**task, variants=variants_list))
        except ValidationError as e:
            # Print the error
            print(f"Validation error on task_id-{task.get("id", None)}: {e}")
    print(tasks_list)


if __name__ == "__main__":
    main()
