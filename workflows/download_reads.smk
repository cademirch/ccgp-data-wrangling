from pathlib import Path
import os
from snakemake.exceptions import WorkflowError
import sys
sys.path.append(
    ".."
)  # Don't like this b/c hardcodes this file structure.  but no one else will probably use this so doesn't matter
from gdrive import CCGPDrive
from pathlib import Path
import shutil
from datetime import datetime

"""
To run this:

snakemake --nolock --use-conda --cores N --configfile <config>'
"""



def get_multiqc_input(wildcards):
    checkpoint_output = checkpoints.checksums.get(**wildcards).output[0]
    reads = list(Path(f"downloads/{wildcards.name}/").glob("*.fastq.gz"))
    reads.extend(list(Path(f"downloads/{wildcards.name}/").glob("*.fq.gz")))
    reads = [x.name.replace(".fastq.gz", "").replace(".fq.gz", "") for x in reads]
    return expand("downloads/qc/{name}/{sample}{ext}", **wildcards, sample=reads, ext=['_fastqc.zip', '_screen.txt'])

def get_read(wc):
    if Path(wc.sample + "fastq.gz").exists():
        return Path(wc.sample + "fastq.gz")
    elif Path(wc.sample + "fq.gz").exists():
        return Path(wc.sample + "fq.gz")

rule all:
    input:
        ancient(expand("downloads/done_files/{name}_synced_done.txt", name=config['name'])),
        expand("downloads/qc/{name}_uploaded.txt", name=config['name'])
    
    shell: "echo {config[name]}, {config[cmd]}"


rule download:
    """Makes dir config[name] and runs the download command in that directory."""
    output:
        touch("downloads/done_files/download_{name}_done.txt")
    params:
        cmd = config['cmd'],
        dl_dir = "downloads/{name}"
    shell:
        "mkdir -p {params.dl_dir} && cd {params.dl_dir} && {params.cmd}"

checkpoint checksums:
    """
    Goes to config[name] and checks for md5sum.txt prints original checksums to name_md5sums.txt so its included in aws sync. Then checks the sums.
    This rule is a checkpoint so that if the md5sum fails then the workflow stops!
    """
    input:
        ancient("downloads/done_files/download_{name}_done.txt")
    output:
        touch("downloads/{name}/checksums_done")
    run:
        if Path("downloads", wildcards.name, "md5sum.txt").exists():
            with open(Path("downloads", wildcards.name, "md5sum.txt"), "r") as f:
                outfile = Path("downloads", wildcards.name, f"{wildcards.name}_md5sums.txt")
                with open(outfile, "w") as o:
                    for line in f:
                        line = line.strip().split()
                        sha = line[0]
                        filename = Path("downloads", wildcards.name, Path(line[1]).name) # Path to file in md5sum txt relative to snakefile
                        print(f"{sha}  {filename}", file=o)
            checksums = Path("downloads", wildcards.name, f"{wildcards.name}_checkedsums.txt")
            shell(f"md5sum -c {outfile} > {checksums}") # This will exit with code 1 if a checksum doesnt match
            undetermined = Path("downloads", wildcards.name).glob("Undetermined*")
            for u in undetermined:  # Rename undetermined files for saving with sync.
                dest = Path(u.parent, f"test_{datetime.now().strftime('%Y-%m-%d')}_{u.name}")
                shutil.move(u, dest)
        else:
            raise WorkflowError(f"No md5sum for {wildcards.name}, to work around this you can touch this file: {output[0]}")
        return True
        
rule sync:
    """Syncs to AWS s3 bucket """
    input:
        ancient("downloads/{name}/checksums_done")
    output:
        touch("downloads/done_files/{name}_synced_done.txt")
    params:
        dl_dir = "downloads/{name}"
    shell:
        "aws s3 sync {params.dl_dir}/ s3://ccgp --endpoint=http://10.50.1.41:7480/"


rule fastqc:
    input:
        ancient(get_read),
        
    output:
        html=temp("downloads/qc/{name}/{sample}.html"),
        zip="downloads/qc/{name}/{sample}_fastqc.zip" 
    params: "--quiet"
    log:
        "logs/{name}/{sample}.log"
    threads: 1
    wrapper:
        "v1.3.2/bio/fastqc"

rule fastq_screen:
    input:
        ancient(get_read),
        
    output:
        txt=("downloads/qc/{name}/{sample}_screen.txt"),
        html=temp("downloads/qc/{name}/{sample}_screen.html"),
        png=temp("downloads/qc/{name}/{sample}_screen.png")
    params:
        fastq_screen_config="FastQ_Screen_Genomes/fastq_screen.conf",
        subset=100000,
        outdir="downloads/qc/{name}"
    conda: "fastq_screen.yaml"
    threads: 1
    shell:
        """
        fastq_screen --outdir {params.outdir} \
        --force --conf {params.fastq_screen_config} \
        --subset 100000 --threads 8 {input}
        """

rule multiqc:
    input:
        ancient(get_multiqc_input)
    output:
        "downloads/qc/{name}_multiqc.html"
    params:
        ""  # Optional: extra parameters for multiqc.
    log:
        "logs/{name}_multiqc.log"
    wrapper:
        "v1.3.2/bio/multiqc"

rule upload_multiqc:
    input:
        "downloads/qc/{name}_multiqc.html"
    output:
        touch("downloads/qc/{name}_uploaded.txt")
    run:
        drive = CCGPDrive()
        drive.upload_file(Path(input[0]), "MultiQC Files")