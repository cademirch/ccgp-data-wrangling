# ccgp-data-wrangling
![New Flowchart](https://user-images.githubusercontent.com/44762354/166742653-6ab34688-33cf-4cc3-9b62-090595c89f50.jpg)

This is a collection of python scripts and snakemake workflows for managing data and metadata for the CCGP. 

# How to run download workflow
## With download link
The workflow is designed to ONLY work with the link from QB3.
1. Copy config template. Name can be anything.
 ``` 
 cp configs/example_config.yaml configs/<name>_config.yaml
 ```
2. Replace `cmd` field with download link (copy and paste). Replace `name` field with name.
3. Activate conda env and run the workflow in dry-run mode:
  ```
  conda activate data_wrangler
  snakemake -s download_reads.smk --configfile configs/<name>_config.yaml -n
  ```
  Job summary should look like this:
  ```
  Job stats:
job               count    min threads    max threads
--------------  -------  -------------  -------------
all                   1              1              1
checksums             1              1              1
download              1              1              1
multiqc               1              1              1
sync                  1              1              1
upload_multiqc        1              1              1
total                 6              1              1
  ```
4. Actually run the workflow in a screen
  ```
  screen -S <name>
  conda activate data_wrangler
  snakemake -s download_reads.smk --configfile configs/<name>_config.yaml --nolock --use-conda --cores 10
  ```
## Without download link
Sometimes submitters sequenced outside of QB3. Regardless the workflow should still be used.
1. Create a directory in `downloads/` where you will put the reads.
  ```
  mkdir downloads/<name>
  ```
2. Copy all the reads to the new directory. Make sure they are not in any subdirectory. Touch the download done file:
  ```
  touch downloads/done_files/download_<name>_done.txt
  ```
3. If there is an md5sum file, make sure it is named `md5sum.txt`. If there is none, touch the checksum done file like so:
  ```
  touch downloads/<name>/checksums_done
  ```
4. Setup the config file like above. You must put a value for `cmd` but it can be anything.
5. Run the workflow in a screen like above.


# Running the transfer reads workflow:
1. Check that the project you are transferring has the expected number of reads/samples before running.
2. Execute the workflow:
  ```
  snakemake -s transfer_reads.smk --config pid='<project-id>' --nolock --use-conda --cores 10
  ```
