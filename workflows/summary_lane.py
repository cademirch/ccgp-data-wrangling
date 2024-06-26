import pandas as pd
import argparse

def calculate_yields(input_file, output_file1, output_file2):
    df = pd.read_csv(input_file)

    grouped_df = df.groupby(['Lane'], group_keys=False)
    total_reads_by_lane = grouped_df['Num.Read.PF'].sum()
    undetermined_reads_by_lane = grouped_df.apply(lambda x: x[x['SampleID'] == 'Undetermined']['Num.Read.PF'].sum())
    percent_undetermined_reads_by_lane = (undetermined_reads_by_lane / total_reads_by_lane) * 100
    df_lane_sum = pd.DataFrame({'Total_Reads_by_Lane': total_reads_by_lane, 'Percent_Undetermined_Reads_by_Lane': percent_undetermined_reads_by_lane})
    # Calculate the proportion of each 'Num.Read.PF' value relative to the sum of 'Num.Read.PF' for each 'Lane'
    proportion_of_total_reads = grouped_df['Num.Read.PF'].apply(lambda x: x / x.sum())
    df_samp_prop = pd.DataFrame({'SampleID': df['SampleID'], 'Lane': df['Lane'],'Num.Read.PF': df['Num.Read.PF'], 'Proportion_of_Total_Reads': proportion_of_total_reads})

    df_lane_sum.to_csv(output_file1, index=False)
    df_samp_prop.to_csv(output_file2, index=False)


def main() -> None:

    parser = argparse.ArgumentParser(description='Write sample files.')
    parser.add_argument('-d', '--dfile', dest='samp', required=True, help="Specify path to sample list")
    parser.add_argument('-a', '--out1', dest='out1', required=True, help="Specify path to sample list")
    parser.add_argument('-b', '--out2', dest='out2', required=True, help="Specify path to sample list")


    args = parser.parse_args()

    sample_list = args.samp
    output_file1 = args.out1
    output_file2 = args.out2


    calculate_yields(sample_list, output_file1, output_file2)

main()