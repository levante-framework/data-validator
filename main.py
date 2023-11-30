from firestore_services import EntityController
from pydantic import ValidationError
import json

def main():
    ec = EntityController()
    # print(len(nec.valid_variants_params))
    # print(len(sec.invalid_classes))
    print(len(ec.valid_user_class))
    print(len(ec.invalid_user_class))

if __name__ == "__main__":
    main()
