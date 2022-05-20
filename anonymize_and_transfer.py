import os
import pydicom
from argparse import ArgumentParser, Namespace
from typing import Any, Iterable, Optional, Sequence, Text, Tuple, Union
import sys
import logging
import subprocess
import glob
from dicognito.anonymizer import Anonymizer
from dicognito.burnedinannotationguard import BurnedInAnnotationGuard


parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "sources",
    metavar="source",
    type=str,
    nargs="+",
    help="The directories or file globs (e.g. *.dcm) to anonymize. Directories "
    "will be recursed, and all files found within will be anonymized.",
)
parser.add_argument(
    "--output-directory",
    "-o",
    action="store",
    type=str,
    help="Write anonymized files to " "OUTPUT_DIRECTORY, which will be created if necessary",
)
parser.add_argument(
    "--anonymization-map",
    "-m",
    action="store",
    type=str,
    default=None,
    help="Save a mapping between the original (unanonymized) and anonymized PatientIDs to "
    "ANONYMIZATION_MAP. Map will be saved in CSV format. Use with caution: this "
    "file will contain sensitive data!",
)
parser.add_argument(
    "--seed",  # currently only intended to make testing easier
    help="The seed to use when generating random attribute values. Primarily "
    "intended to make testing easier. Best anonymization practice is to omit "
    "this value and let dicognito generate its own random seed.",
)

args = parser.parse_args()
logging.basicConfig()

anonymization_map = "anonymization_map.csv"
if os.path.isfile(args.anonymization_map):
    logging.critical(
        "Refusing to overwrite existing map file: %s. Rename existing file or specify alternate path.",
        anonymization_map,
    )
    sys.exit(1)
with open(args.anonymization_map, "w") as f:
    print("original patient ID, anonymized patient ID", file=f)

    anonymizer = Anonymizer(id_prefix=args.id_prefix, id_suffix=args.id_suffix, seed=args.seed)
    burned_in_annotation_guard = BurnedInAnnotationGuard(args.assume_burned_in_annotation, args.on_burned_in_annotation)

    def get_files_from_source(source: str) -> Iterable[str]:
        if os.path.isfile(source):
            yield source
        elif os.path.isdir(source):
            for (dirpath, dirnames, filenames) in os.walk(source):
                for filename in filenames:
                    yield os.path.join(dirpath, filename)
        else:
            for expanded_source in glob.glob(source):
                for file in get_files_from_source(expanded_source):
                    yield file

    def ensure_output_directory_exists(args: Namespace) -> None:
        if args.output_directory and not os.path.isdir(args.output_directory):
            os.makedirs(args.output_directory)

    def calculate_output_filename(file: str, args: Namespace, dataset: pydicom.dataset.Dataset) -> str:
        output_file = file
        if args.output_directory:
            output_file = os.path.join(args.output_directory, dataset.SOPInstanceUID + ".dcm")
        return output_file

    ensure_output_directory_exists(args)

    mapped_ids = []
    for source in args.sources:
        for file in get_files_from_source(source):
            try:
                with pydicom.dcmread(file, force=False) as dataset:
                    burned_in_annotation_guard.guard(dataset, file)
                    original_patient_id = dataset.get("PatientID", "")
                    anonymizer.anonymize(dataset)

                    if original_patient_id not in mapped_ids:
                        with open(anonymization_map, "a") as f:
                            print(original_patient_id + "," + dataset.get("PatientID", ""), file=f)
                        mapped_ids += [original_patient_id]

                    output_file = calculate_output_filename(file, args, dataset)
                    dataset.save_as(output_file, write_like_original=False)
            except pydicom.errors.InvalidDicomError:
                logging.info("File %s appears not to be DICOM. Skipping.", file)
                continue
            except Exception:
                logging.error("Error occurred while converting %s. Aborting.\nError was:", file, exc_info=True)
                sys.exit(1)

    logging.warning("Saved anonymization map to %s", args.anonymization_map)
