from firestore_services import FirestoreServices
from core_models import School
from pydantic import ValidationError


def main():
    schools = FirestoreServices().get_schools()
    schools_names = []
    for school in schools:
        try:
            # Create an instance of your Pydantic model
            schools_name = School(**school).name
            schools_names.append(schools_name)
        except ValidationError as e:
            # Print the error
            print(f"Validation error on school_id-{school.get("id", None)}: {e}")
    print(schools_names)


if __name__ == "__main__":
    main()
