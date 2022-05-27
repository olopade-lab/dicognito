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
    "intermediate_output_directory",
    type=str,
    help="Write anonymized files to OUTPUT_DIRECTORY, which will be created if necessary; warning: do not write large amounts of temporary data to SSD, they have limited lifetime writes!",
)
parser.add_argument(
    "remote_address",
    action="store",
    type=str,
    help="Remote address for rsync command including username, example: annawoodard@gardner.cri.uchicago.edu:/scratch/annawoodard",
)
parser.add_argument(
    "--seed",  # currently only intended to make testing easier
    help="The seed to use when generating random attribute values. Primarily "
    "intended to make testing easier. Best anonymization practice is to omit "
    "this value and let dicognito generate its own random seed.",
)
parser.add_argument(
    "--assume-burned-in-annotation",
    action="store",
    type=str,
    default=BurnedInAnnotationGuard.ASSUME_IF_CHOICES[0],
    choices=BurnedInAnnotationGuard.ASSUME_IF_CHOICES,
    help="How to assume the presence of burned-in annotations, considering "
    "the value of the Burned In Annotation attribute",
)
parser.add_argument(
    "--on-burned-in-annotation",
    action="store",
    type=str,
    default=BurnedInAnnotationGuard.IF_FOUND_CHOICES[0],
    choices=BurnedInAnnotationGuard.IF_FOUND_CHOICES,
    help="What to do when an object with assumed burned-in annotations is found",
)

args = parser.parse_args()
args.sources = [os.path.abspath(x) for x in args.sources]
args.intermediate_output_directory = os.path.abspath(args.intermediate_output_directory)
logging.basicConfig()

anonymization_map = "anonymization_map.csv"
if not os.path.isfile(anonymization_map):
    with open(anonymization_map, "w") as f:
        print("original patient ID, anonymized patient ID", file=f)

    anonymizer = Anonymizer(seed=args.seed)
    burned_in_annotation_guard = BurnedInAnnotationGuard(args.assume_burned_in_annotation, args.on_burned_in_annotation)

    def get_files_from_source(source: str) -> Iterable[str]:
        if os.path.isfile(source):
            yield source
        elif os.path.isdir(source):
            for (dirpath, dirnames, filenames) in os.walk(source):
                for filename in filenames:
                    yield os.path.join(dirpath, filename)
        else:
            result = {}
            for expanded_source in glob.glob(source):
                result.update(get_files_from_source(expanded_source))

    def ensure_output_directory_exists(args: Namespace) -> None:
        if args.intermediate_output_directory and not os.path.isdir(args.intermediate_output_directory):
            os.makedirs(args.intermediate_output_directory)

    def calculate_output_filename(file: str, args: Namespace, dataset: pydicom.dataset.Dataset, source: str) -> str:
        output_file = file
        out_dir = "."
        if args.intermediate_output_directory:
            tail = file.replace(source, "")
            out_dir = os.path.dirname(args.intermediate_output_directory + "/." + tail)
            os.makedirs(out_dir, exist_ok=True)
        output_file = os.path.join(out_dir, dataset.SOPInstanceUID + ".dcm")
        return output_file

    ensure_output_directory_exists(args)
    transferred_files = []
    if os.path.isfile("transfer_checkpoint.txt"):
        with open("transfer_checkpoint.txt", "r") as f:
            transferred_files = f.readlines()

    mapped_ids = []
    for source in args.sources:
        for file in get_files_from_source(source):
            if file in transferred_files:
                print(f"encountered already-transferred file {file}; will be skipped")
                continue
            try:
                with pydicom.dcmread(file, force=False) as dataset:
                    burned_in_annotation_guard.guard(dataset, file)
                    original_patient_id = dataset.get("PatientID", "")
                    anonymizer.anonymize(dataset)

                    if original_patient_id not in mapped_ids:
                        with open(anonymization_map, "a+") as f:
                            print(original_patient_id + "," + dataset.get("PatientID", ""), file=f)
                        mapped_ids += [original_patient_id]

                    output_file = calculate_output_filename(file, args, dataset, source)
                    dataset.save_as(output_file, write_like_original=False)
                    logging.info(f"starting transfer of anonymized file {output_file}")
                    remote_dir = os.path.split(file)[0].replace(os.path.split(source)[0], "")
                    command = f"rsync -avPz --recursive --relative --partial --remove-source-files {output_file} {args.remote_address}"
                    logging.info(f"calling {command}")
                    subprocess.check_call(command, shell=True)
                    logging.info(f"completed transfer of anonymized file {output_file}")
                    with open("transfer_checkpoint.txt", "a+") as f:
                        f.write(file + "\n")

            except pydicom.errors.InvalidDicomError:
                logging.info("File %s appears not to be DICOM. Skipping.", file)
                continue
            except Exception:
                logging.error("Error occurred while converting %s. Aborting.\nError was:", file, exc_info=True)
                sys.exit(1)

    logging.warning("Saved anonymization map to %s", args.anonymization_map)
