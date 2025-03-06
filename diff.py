from collections import Counter

def count_line_differences(file1, file2, output_file):
    # Read and count occurrences of each line in both files
    with open(file1, 'r') as f1, open(file2, 'r') as f2:
        file1_lines = Counter(line.strip() for line in f1)
        file2_lines = Counter(line.strip() for line in f2)

    # Open the output file for writing the results
    with open(output_file, 'w') as f_out:
        # Get all unique lines across both files
        all_lines = set(file1_lines.keys()).union(file2_lines.keys())

        # Write the differences in occurrence counts
        for line in all_lines:
            count1 = file1_lines.get(line, 0)
            count2 = file2_lines.get(line, 0)
            if count1 != count2:
                f_out.write(f"Line: {line}\n")
                f_out.write(f" - Appears {count1} times in File 1\n")
                f_out.write(f" - Appears {count2} times in File 2\n\n")

    print(f"Line difference results saved to {output_file}")


# Usage example
count_line_differences('C:/Users/valentinosko/Downloads/collateral_20241107.txt', 'C:/Users/valentinosko/Downloads/collateral_20241107_cappitech.txt', 'C:/Users/valentinosko/Downloads/line_difference_results.txt')


