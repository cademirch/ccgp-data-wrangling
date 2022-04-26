"""This module handles parsing of the minicore sheets and biosample metadata sheets that are submitted to the me."""
import pandas as pd
from pathlib import Path
from collections import defaultdict
from gsheets import WGSTracking
import sys
from glob import glob


def get_already_read() -> set:
    with open("parsed_submissions.txt", "r") as f:
        already_read = set([line.rstrip() for line in f.readlines()])
    return already_read


def get_project_id(series: pd.Series) -> dict[str:str]:
    """Takes series of species and looks up what project-id it belongs to and returns that id"""

    def create_dict() -> dict:
        project_ids = defaultdict(str)
        genuses = defaultdict(str)
        with open("project_ids_species.csv", "r") as f:
            next(f)
            for line in f:
                line = line.strip().split(",")
                project_id, genus, subspecies = line[0], line[1], line[2]
                project_ids[subspecies] = project_id
                genuses[genus] = project_id
        return project_ids, genuses

    spp_lookup, genus_lookup = create_dict()
    out = [[], []]
    for s in series:
        if len(s.strip().split()) >= 3:
            s = " ".join(s.strip().split()[:2])  # Only care about genus species.
        if s in spp_lookup:
            out[0].append(spp_lookup[s])
            out[1].append(1)
        else:  # S was not in subspecies so lets compare genus of S to genus of K
            g = s.strip().split()[0]
            if g in genus_lookup:
                out[0].append(genus_lookup[g])
                out[1].append(0)
            else:
                out[0].append("Unknown project-id")
                out[1].append(0)

    return out


def find_header_line_num(file: Path) -> int:
    """Find the line number of the header in a file."""
    with open(file, "r", errors="ignore") as f:
        for i, line in enumerate(f):
            if line.split("\t")[0] == ("*sample_name"):
                return i
    raise (ValueError(f"Could not find header in {file}"))


# Read all minicore sheets
def read_minicore_sheets() -> pd.DataFrame:
    df_list = []
    w = WGSTracking()
    already_read = get_already_read()

    files = set(
        glob("../metadata_submissions/minicore/*.xlsx")
        + glob("../metadata_submissions/minicore/*.xls")
    )
    files = files - already_read
    if len(files) == 0:
        print("No minicore files to process")
        return None
    files = files - already_read
    for file in files:
        print(f"Processing file: {file}")
        if file.suffix in [".xlsx", ".xls"]:
            df = pd.read_excel(file)
            df.drop(
                index=df.index[[0, 1]], axis=0, inplace=True
            )  # Drop the first two rows (info and example)
            df.drop(
                df.columns[[0]], axis=1, inplace=True
            )  # Drop the first column (sample number)
            df.dropna(how="all", inplace=True)
            df["ccgp-project-id"], df["expected-species"] = get_project_id(
                df["Genus species*"]
            )
            df["ref_genome_accession"] = w.reference_accession(
                df["ccgp-project-id"].unique().tolist()[0]
            )
            df_list.append(df)
            with open("parsed_submissions.txt", "a") as f:
                print(file, file=f)
    out_df = pd.concat(df_list)
    rename_dict = {
        "SampleID*": "*sample_name",
        "Genus species*": "*organism",
        "decimal latitude*": "lat",
        "decimal longitude*": "long",
        "sample collection date*": "*collection_date",
        "Locality Name": "geo_loc_name",
    }
    out_df.rename(columns=rename_dict, inplace=True)
    cols_to_keep = [
        "*sample_name",
        "*organism",
        "Preferred Sequence ID",
        "subspecies",
        "gDNA extraction method*",
        "long",
        "lat",
        "sample collection date*",
        "geo_loc_name",
        "Locality Description",
        "library_prep_method",
        "ccgp-project-id",
        "expected-species",
        "ref_genome_accession",
    ]
    out_df.drop(columns=out_df.columns.difference(cols_to_keep), inplace=True)

    out_df[
        "library_prep_method"
    ] = """Automated DNA extractions from tissues were performed using a bead-based and taxa-specific series of kits from Macherey-Nagel or Omega Bio-tek on an epMotion 5075 liquid handling robot. 
    Following extraction, DNA was assayed for purity, quality, and quantity using a NanoDrop 1000, agarose gel, and Qubit fluorescence quantification, respectively. 
    Individuals were sequenced in whole-genome libraries using SeqWell PlexWell kits, which is a modification of Illumina's Nextera/tagmentation technology. 
    Samples were first normalized and then tagmented, in which one of 24 sample-specific i7 indexes were ligated. 
    Individuals were then pooled into a single library in sets of 24 and ligated with one pool-specific i5 index. 
    Individuals are therefore dual indexed using 8bp indexes. Libraries were then amplified for 6 PCR cycles using the Kapa HiFi HotStart ReadyMix and then size-selected using SeqWell MAGwise Paramagnetic Beads. 
    Libraries were pooled equimolarly according to concentration and average fragment size for sequencing on a NovaSeq S4 6000 with paired-end 150 base pair reads at QB3 Genomics Vincent J. Coates Genomics Sequencing Lab."""
    return out_df


def read_biosample_sheets() -> pd.DataFrame:
    df_list = []
    w = WGSTracking()
    already_read = get_already_read()
    files = set(
        glob("../metadata_submissions/not_minicore/*.xlsx")
        + glob("../metadata_submissions/not_minicore/*.xls")
        + glob("../metadata_submissions/not_minicore/*.tsv")
    )
    files = files - already_read
    if len(files) == 0:
        print("No not-minicore files to process")
        return None

    for file in files:
        print(f"Processing file: {file}")
        if file.suffix in [".xlsx", ".xls"]:
            df = pd.read_excel(file, header=12)

        elif file.suffix in [".tsv"]:
            df = pd.read_csv(
                file,
                header=find_header_line_num(file),
                sep="\t",
                encoding_errors="ignore",
            )
        else:
            continue
        df.dropna(how="all", inplace=True)
        df["ccgp-project-id"], df["expected-species"] = get_project_id(df["*organism"])
        df["ref_genome_accession"] = w.reference_accession(
            df["ccgp-project-id"].unique().tolist()[0]
        )
        df_list.append(df)
        with open("parsed_submissions.txt", "a") as f:
            print(file, file=f)
    out_df = pd.concat(df_list)
    return out_df


def get_big_df() -> pd.DataFrame:
    minicore_sheets = read_minicore_sheets()
    biosample_sheets = read_biosample_sheets()

    if minicore_sheets is None and biosample_sheets is None:
        print("Nothing to do.")
        sys.exit()
    elif minicore_sheets is not None:
        df = minicore_sheets
    elif biosample_sheets is not None:
        df = biosample_sheets
    df = pd.concat([read_minicore_sheets(), read_biosample_sheets()])
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.loc[:, ~df.columns.str.contains("^Unnamed", na=False)]
    df["*sample_name"] = df["*sample_name"].astype(str)
    # Some sample names have a period in them, it seems UCB replaces with dash so
    df["*sample_name"] = df["*sample_name"].str.replace(".", "-")
    # Same for spaces but underscores instead
    df["*sample_name"] = df["*sample_name"].str.replace(" ", "_")
    df.reset_index(inplace=True)
    return df


def get_summary_df(df) -> pd.DataFrame:
    wgs_sheet = WGSTracking()

    subset = df[
        [
            "ccgp-project-id",
            "files",
            "filesize_sum",
            "*sample_name",
            "*organism",
            "expected-species",
        ]
    ]
    grouped = subset.groupby("ccgp-project-id")
    total_counts = grouped.size()
    has_reads = grouped["files"].count()
    expected_species = grouped["expected-species"].sum()
    filesize_sum = grouped["filesize_sum"].sum() / 1e12
    reference_prog = wgs_sheet.reference_progress()
    expected_count = wgs_sheet.expected_count()
    percent_seq = has_reads / expected_count

    df = pd.concat(
        {
            "Metadata recieved": total_counts,
            "# Samples has reads": has_reads,
            "# Unxpected Species recieved": expected_species,
            "Sum of filesizes (Tb)": filesize_sum,
            "Reference Stage": reference_prog,
            "Expected # of samples": expected_count,
            "% Done": percent_seq,
        },
        axis=1,
    )
    df.reset_index(inplace=True)
    df.sort_values(by=["% Done"], inplace=True, ascending=False)
    return df
