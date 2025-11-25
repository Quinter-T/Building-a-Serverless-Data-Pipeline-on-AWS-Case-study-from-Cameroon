import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import csv
import io
# NOTE: We need to import 'datetime' for the fix, as it contains 'strptime'
from datetime import date, timedelta, datetime 

# Initialize Glue and Boto3 clients
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# SNS client for notifications
sns_client = boto3.client('sns')

# Define your S3 and SNS variables here
S3_BUCKET = 'vaccine-date-2025888' # secret...
S3_KEY = 'Elegancia District hospital Vaccinatin Data.csv' # Ensure this matches your final CSV name --- snake casing is preferable (elegancia_distric...)
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:710002907483:PRO-Reminders2' # Replace with your ARN

# --- CAMEROON BABY VACCINATION SCHEDULE (0-1 YEAR) ---
# The key is the number of days after birth, and the value is the list of vaccines due.
# DTP-HepB-Hib is the Pentavalent vaccine. PCV is Pneumococcal Conjugate Vaccine.
VACCINATION_SCHEDULE = {
    # 1st Contact (At Birth, Day 0)
    0: 'BCG and OPV 0 (Oral Polio Vaccine dose 0)',
    # 2nd Contact (6 Weeks ≈ 42 Days)
    42: 'DTP-HepB-Hib 1, OPV 1, PCV 1, and ROTA 1 (Rotavirus)',
    # 3rd Contact (10 Weeks ≈ 70 Days)
    70: 'DTP-HepB-Hib 2, OPV 2, PCV 2, and ROTA 2 (Rotavirus)',
    # 4th Contact (14 Weeks ≈ 98 Days)
    98: 'DTP-HepB-Hib 3, OPV 3, PCV 3, and IPV (Inactivated Polio Vaccine)',
    # 5th Contact (9 Months ≈ 270 Days)
    270: 'Measles-Rubella (MR) and Yellow Fever (YF) vaccines',
    # Note: Vitamin A is often given at 6 months and 9 months, but is a supplement, not a vaccine.
}
# --- END SCHEDULE ---

# Read the CSV from S3
try:
    s3_resource = boto3.resource('s3')
    obj = s3_resource.Object(S3_BUCKET, S3_KEY)
    csv_content = obj.get()['Body'].read().decode('utf-8')
except Exception as e:
    print(f"Error reading S3 file: {e}")
    sys.exit(1)

# Process the data
reminders_list = []
reader = csv.reader(io.StringIO(csv_content))
next(reader)  # Skip header

today = date.today()

for row in reader:
    baby_name = row[0]
    date_of_birth_str = row[1]
    phone_number = row[2]
    
    # Assuming the CSV also includes a column for the last received vaccine milestone (e.g., '14 weeks')
    # For this modified script, we'll assume a placeholder for the last completed stage.
    # In a real system, you would read the actual vaccination status (e.g., row[3]).
    # We will simply check ALL upcoming vaccines for a baby less than 1 year old.
    
    try:
        # ------------------------------------------------------------------
        # CORRECTED LINE: Use strptime to handle M/D/YYYY format
        # datetime.strptime returns a datetime object, .date() converts it to a date object
        date_of_birth = datetime.strptime(date_of_birth_str, '%m/%d/%Y').date() 
        # ------------------------------------------------------------------
        baby_age_days = (today - date_of_birth).days
        
        # We only check for babies whose due dates are in the future or today, and are under 1 year old (365 days).
        if baby_age_days < 365:
            
            # Iterate through the schedule to find upcoming vaccines
            for days_due, vaccines_due in VACCINATION_SCHEDULE.items():
                vaccine_due_date = date_of_birth + timedelta(days=days_due)
                
                # Check for reminders: Due today, or due within the next 7 days
                # You may adjust the timedelta(days=7) to change the reminder window
                reminder_window = timedelta(days=7) 
                
                if today <= vaccine_due_date <= (today + reminder_window):
                    reminders_list.append({
                        'BabyName': baby_name,
                        'VaccinationDate': vaccine_due_date.strftime('%Y-%m-%d'),
                        # **The message now specifies the exact vaccine due**
                        'VaccinesDue': vaccines_due,
                        'PhoneNumber': phone_number
                    })
                    # Break after finding the next due vaccine to avoid repeated reminders for one child
                    # if the next one is also due soon (for simplicity here).
                    # A more complex system would track which vaccine doses were received.
                    break 

    except Exception as e:
        # This block will now only catch truly bad data (e.g., missing DOB)
        print(f"Error processing row for {baby_name}: {e}")
        continue

# Format the email content
if reminders_list:
    email_subject = 'DAILY URGENT VACCINATION REMINDERS'
    email_body = (
        "Hello Public Relations Officer,\n\n"
        "The following parents need to be reminded of their baby's **UPCOMING VACCINATIONS** (due within the next 7 days). "
        "Please send an SMS and call each parent, making sure to mention the **SPECIFIC VACCINE(S) DUE**.\n\n"
        "----------------------------------------------------\n"
    )
    for reminder in reminders_list:
        # **The email body now includes the specific vaccines due**
        email_body += (
            f"Baby Name: {reminder['BabyName']}\n"
            f"**Vaccination(s) Due:** {reminder['VaccinesDue']}\n"
            f"Due Date: {reminder['VaccinationDate']}\n"
            f"Mother's Phone Number: {reminder['PhoneNumber']}\n"
            "----------------------------------------------------\n"
        )
else:
    email_subject = 'No Vaccination Reminders for Today'
    email_body = "Hello Public Relations Officer,\n\nThere are no upcoming vaccination reminders for today."

# Publish the message to SNS
try:
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=email_subject,
        Message=email_body
    )
    print("Email notification sent successfully to the SNS topic.")
except Exception as e:
    print(f"Error sending SNS notification: {e}")
    # In a Glue job, you might want a soft exit if SNS fails but processing was okay.
    # For now, we keep the original exit(1).
    # sys.exit(1)

job.commit()