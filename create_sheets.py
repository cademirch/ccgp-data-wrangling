import os
from db import get_mongo_client
import pymongo
import datetime
from pprint import pprint
import pandas as pd
import re
import copy
from thefuzz import process
from pathlib import Path
from datetime import datetime
import argparse


def preprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Processes dataframe from MongoDB to be turned into workflow sheet or SRA sheet."""
    list_of_dicts = []
    for _, row in df.iterrows():
        record = row.to_dict()
        files = sorted(record["files"])
        if len(files) == 2:
            record["filename"] = files[0]
            record["filename2"] = files[1]
            record["library_ID"] = record["filename"].split("_R1")[0]
            record["title"] = f"Whole genome sequencing of {record['*organism']}"
            list_of_dicts.append(record)

        elif len(files) >= 4:

            prefixes = set()
            for file in files:
                prefixes.add(
                    re.split("_R\d[._]", file)[0]
                )  # this splits on the _R1/R2 of the filename

            # Using fuzzy matching here for the following example case:
            # sample has two sets of reads: samp_a1_R1.fq.gz and samp_a1_L001_R1.fq.gz
            # Splitting on R1 gives two prefixes, samp_a1_R1 and samp_a1_L001, the former is a substring of the latter,
            # So something like [f for f in files if p in f for p in prefixes] doesn't work to find pairs of reads.
            pairs = []
            for p in prefixes:
                matches = process.extract(p, files, limit=2)
                pairs.append([matches[0][0], matches[1][0]])

            for pair in pairs:
                record_copy = copy.deepcopy(record)
                record_copy["filename"] = pair[0]
                record_copy["filename2"] = pair[1]
                record_copy["library_ID"] = record_copy["filename"].split("_R1")[0]
                record_copy[
                    "title"
                ] = f"Whole genome sequencing of {record_copy['*organism']}"
                list_of_dicts.append(record_copy)
    out_df = pd.DataFrame(list_of_dicts)
    return out_df


def create_workflow_sheet(
    project_id: str,
    db_client: pymongo.MongoClient,
) -> None:
    """Creates workflow csv for given project_id. If project does not have ref genome accession, a placeholder will be printed."""
    db = db_client["ccgp"]
    ccgp_samples = db["ccgp-samples"]
    ccgp_workflow_progress = db["ccgp_workflow_progress"]
    df = preprocess_dataframe(
        pd.DataFrame(list(ccgp_samples.find({"ccgp-project-id": project_id})))
    )
    workflow_df = df[
        [
            "*sample_name",
            "library_ID",
            "ref_genome_accession",
            "*organism",
            "filename",
            "filename2",
            "lat",
            "long",
            "lat_lon",
            "ccgp-project-id",
        ]
    ]

    if workflow_df["lat_lon"].isna().all():
        workflow_df = workflow_df.drop(columns=["lat_lon"])
    else:
        workflow_df["lat"] = workflow_df["lat_lon"].apply(lambda x: x.split(",")[0])
        workflow_df["long"] = workflow_df["lat_lon"].apply(lambda x: x.split(",")[1])
        workflow_df = workflow_df.drop(
            columns=["lat_lon"],
        )
    workflow_df["ref_genome_accession"] = workflow_df["ref_genome_accession"].replace(
        "NaN",
        "refGenomePlaceholder",
    )

    workflow_df["lat"] = workflow_df["lat"].astype(str).str.replace('"', "")
    workflow_df["long"] = workflow_df["long"].astype(str).str.replace('"', "")
    workflow_df["lat"] = workflow_df["lat"].astype(str).str.strip()
    workflow_df["long"] = workflow_df["long"].astype(str).str.strip()
    rename_col_dict = {
        "*sample_name": "BioSample",
        "library_ID": "LibraryName",
        "ref_genome_accession": "refGenome",
        "filename": "fq1",
        "filename2": "fq2",
        "ccgp-project-id": "Organism",  # using project-id here to coerce organism to be the same for all samps in a project.
        "lat": "lat",
        "long": "long",
    }
    workflow_df = workflow_df.rename(columns=rename_col_dict).drop(
        columns=["*organism"]
    )
    workflow_df["Run"] = workflow_df["LibraryName"]
    workflow_df["BioProject"] = workflow_df["Organism"]
    workflow_df = workflow_df.drop_duplicates()
    filename = f"{project_id}_workflow.csv"
    filepath = os.path.join("..", "workflow_sheets", filename)
    workflow_df.to_csv(filepath, index=False)
    ccgp_workflow_progress.update_one(
        filter={"project_id": project_id},
        update={"$set": {"workflow_sheet_created": datetime.utcnow()}},
        upsert=True,
    )


def create_sra_sheet(
    project_id: str,
    db_client: pymongo.MongoClient,
) -> None:
    db = db_client["ccgp"]
    ccgp_samples = db["ccgp-samples"]
    ccgp_workflow_progress = db["ccgp_workflow_progress"]
    sra_df = preprocess_dataframe(
        pd.DataFrame(list(ccgp_samples.find({"ccgp-project-id": project_id})))
    )
    sra_df["library_strategy"] = "WGS"
    sra_df["library_source"] = "GENOMIC"
    sra_df["library_selection"] = "RANDOM"
    sra_df["library_layout"] = "PAIRED"
    sra_df["platform"] = "Illumina"
    sra_df["instrument_model"] = "Illumina NovaSeq 6000"
    sra_df["filetype"] = "fastq"
    sra_df = sra_df.rename(columns={"library_prep_method": "design_description"})

    sra_cols = [
        "biosample_accession",
        "library_ID",
        "title",
        "library_strategy",
        "library_source",
        "library_selection",
        "library_layout",
        "platform",
        "instrument_model",
        "design_description",
        "filetype",
        "filename",
        "filename2",
    ]
    sra_df = sra_df[sra_cols]
    filename = f"{project_id}_sra.tsv"
    filepath = os.path.join("..", "sra_sheets", filename)
    sra_df.to_csv(filepath, index=False, sep="\t")
    ccgp_workflow_progress.update_one(
        filter={"project_id": project_id},
        update={"$set": {"sra_sheet_created": datetime.utcnow()}},
        upsert=True,
    )


def main():
    db = get_mongo_client()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        dest="project_id",
        required=True,
        help="Project id to generate sheets for.",
    )
    subparser = parser.add_subparsers(
        dest="command", title="subcommands", description="valid subcommands"
    )
    metadata = subparser.add_parser("workflow", description="Create workflow sheet")
    attributes = subparser.add_parser("sra", description="Create SRA submission sheet")
    both = subparser.add_parser("both", description="Create both")
    args = parser.parse_args()

    if args.command == "workflow":
        create_workflow_sheet(args.project_id, db)
    elif args.command == "sra":
        create_sra_sheet(args.project_id, db)
    elif args.command == "both":
        create_workflow_sheet(args.project_id, db)
        create_sra_sheet(args.project_id, db)


if __name__ == "__main__":
    main()
