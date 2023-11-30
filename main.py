from firestore_services import NestedEntityController, SimpleEntityController
from pydantic import ValidationError
import json

def main():
    nec = NestedEntityController()
    sec = SimpleEntityController()
    print(len(nec.valid_variants_params))
    print(len(sec.invalid_classes))


if __name__ == "__main__":
    main()
