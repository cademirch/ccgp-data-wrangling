from pathlib import Path
import os

"""
To run this:

snakemake --nolock --use-conda --cores N --config cmd='<download command>' --config name='<name of the sequencing run>'

"""

def get_reads(wildcards):
    checkpoint_output = checkpoints.checksums.get(**wildcards).output[0]
    reads = Path(f"{wildcards.name}/").glob("*.fastq.gz")
    reads = [x.name for x in reads]
    reads = [str(x).strip(".fastq.gz") for x in reads]
    return expand("downloads/qc/{name}/{sample}{ext}", **wildcards, sample=reads, ext=['_fastqc.zip', '_screen.txt'])

rule all:
    input:
        ancient(expand("downloads/done_files/{name}_synced_done.txt", name=config['name'])),
        expand("downloads/qc/{name}_multiqc.html", name=config['name'])
    
    shell: "rm -rf {config[name]}"


rule download:
    """Makes dir config[name] and runs the download command in that directory."""
    output:
        touch("downloads/done_files/download_{name}_done.txt")
    params:
        cmd = config['cmd']
    shell:
        "mkdir -p {wildcards.name} && cd {wildcards.name} && {params.cmd}"

checkpoint checksums:
    """Goes to config[name] and checks for md5sum.txt prints original checksums to name_md5sums.txt so its included in aws sync. Then checks the sums."""
    input:
        ancient("downloads/done_files/download_{name}_done.txt")
    output:
        touch("downloads/{name}/checksums_done.txt")
    run:
        os.chdir(os.path.join("downloads", wildcards.name))
        if Path("md5sum.txt").exists():
            with open(Path("md5sum.txt"), "r") as f:
                outfile = Path(f"{wildcards.name}_md5sums.txt")
                with open(outfile, "w") as o:
                    for line in f:
                        line = line.strip().split()
                        sha = line[0]
                        filename = Path(line[1]).name
                        print(f"{sha}  {filename}", file=o)
            checksums = Path(f"{wildcards.name}_checkedsums.txt")
            shell(f"md5sum -c {outfile} > {checksums}") # This will exit with code 1 if a checksum doesnt match
        return True
        
rule sync:
    """Syncs to AWS s3 bucket """
    input:
        ancient("downloads/{name}/checksums_done.txt")
    output:
        touch("downloads/done_files/{name}_synced_done.txt")
    shell:
        "aws s3 sync {wildcards.name}/ s3://ccgp --endpoint=http://10.50.1.41:7480/"


rule fastqc:
    input:
        ancient("downloads/{name}/{sample}.fastq.gz"),
        ancient("downloads/{name}/checksums_done.txt")
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
        ancient("downloads/{name}/{sample}.fastq.gz"),
        ancient("downloads/{name}/checksums_done.txt")
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
        ancient(get_reads)
    output:
        "downloads/qc/{name}_multiqc.html"
    params:
        ""  # Optional: extra parameters for multiqc.
    log:
        "logs/{name}_multiqc.log"
    wrapper:
        "v1.3.2/bio/multiqc"