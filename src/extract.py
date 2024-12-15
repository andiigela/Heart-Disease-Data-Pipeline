import os
import shutil


def load_log(log_file):
    processed_files = {}
    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
            for line in f:
                file, mod_time = line.strip().split(',')
                processed_files[file] = float(mod_time)
    return processed_files
def save_log(log_file, processed_files):
    """Save the processed files with their modification times to the log file."""
    with open(log_file, 'w') as f:
        for file, mod_time in processed_files.items():
            f.write(f"{file},{mod_time}\n")
def validate_folders(raw_folder_path, bronze_folder_path):
    """Check if the specified folders exist."""
    if not os.path.isdir(raw_folder_path) or not os.path.isdir(bronze_folder_path):
        print("Invalid folder path(s)")
        return False
    return True
def process_file(file, raw_folder_path, bronze_folder_path, processed_files):
    """Copy the file to the bronze folder if it's new or modified, and update the log."""
    raw_file_path = os.path.join(raw_folder_path, file)
    bronze_file_path = os.path.join(bronze_folder_path, f'bronze_{file}')
    current_mod_time = os.path.getmtime(raw_file_path)
    if file not in processed_files or processed_files[file] < current_mod_time:
        shutil.copy(raw_file_path, bronze_file_path)
        processed_files[file] = current_mod_time
        print(f"Processed file: {file}")
        return True
    return False
def extract_raw_data(raw_folder_path, bronze_folder_path, log_file):
    """Main function to extract new or modified raw data files to the bronze folder."""
    processed_files = load_log(log_file)
    new_or_modified_files = []
    if not validate_folders(raw_folder_path, bronze_folder_path):
        return "Invalid folder path(s)"
    try:
        files = [file for file in os.listdir(raw_folder_path) if file.endswith('.csv')]
        for file in files:
            if process_file(file, raw_folder_path, bronze_folder_path, processed_files):
                new_or_modified_files.append(file)
        # Save the updated log after processing files
        save_log(log_file, processed_files)
        if not new_or_modified_files:
            print("No new or modified files to process.")
        return new_or_modified_files
    except Exception as ex:
        print(f"Error extracting data from raw folder: {ex}")
        return


if __name__ == "__main__":
    extract_raw_data(
        'C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/raw',
        'C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/bronze',
        'C:/Users/andig/Desktop/Punim Diplome/Heart-Disease-Data-Pipeline/data/processed_files.log'
    )
