from firestore_services import EntityController
import pandas as pd
import json


def main():
    ec = EntityController()
    runs_dict = [obj.model_dump() for obj in ec.valid_runs]
    runs_df = pd.DataFrame(runs_dict)
    runs_df.to_csv('output.csv')
    # print(len(nec.valid_variants_params))
    # print(len(sec.invalid_classes))
    # print(len(ec.valid_runs))
    # print(len(ec.valid_trials))
    # print(len(ec.invalid_trials))
    # print(len(ec.invalid_user_class))


if __name__ == "__main__":
    main()
