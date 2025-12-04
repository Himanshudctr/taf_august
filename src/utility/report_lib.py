import datetime
import os

# Ensure the 'report' directory exists
project_path =  os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
report_dir = os.path.join(project_path,'report')
os.makedirs(report_dir, exist_ok=True)  # this will create the report directory

# Create a single report filename for the session
timestamp = datetime.datetime.now().strftime("%d%m%Y%H%M%S")     # this will give you time
report_filename = os.path.join(report_dir, f"report_{timestamp}.txt")
print(report_filename)


def write_output(validation_type, status, details):
    with open(report_filename, "a") as report:
        report.write(f"{validation_type}: {status} Details: {details}\n ")


# This one use in validation data..like duplicat_check, count_check
  # write_output(
  #           "Duplicate Check",
  #           status,
  #           f"Duplicate Count: {duplicate_count}, Sample Failed Records: {failed_preview}"
  #       )