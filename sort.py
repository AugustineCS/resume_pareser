from airflow import DAG    # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime, timedelta
# from airflow.models import Variable 
from pypdf import PdfReader
from dotenv import load_dotenv
import logging
import boto3
from io import BytesIO
import ollama
import os

load_dotenv()
ENDPOINT_URL = os.getenv("ENDPOINT_URL")
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
OBJECT_KEY = os.getenv("OBJECT_KEY")



logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
# default parameter
default_args = {
    'owner': 'Augy',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}


def process_resumes():
    """Takes all resume from s3 bucket and take its content,
    put them as input to llamma3 and extract details like
    name, email etc. Then classify the resume and generate
    a score for that position
    """
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
        )
    except Exception as e:
        print("Error in Connecting",e)

    # List all PDFs in the bucket
    try:
        objects = s3.list_objects_v2(Bucket=BUCKET_NAME)
        pdf_files = []
        if "Contents" in objects:
            for item in objects["Contents"]:
                filename = item["Key"]
                if filename.lower().endswith(".pdf"):
                    pdf_files.append(filename)
        print(pdf_files)

    except Exception as e:
        print("Error listing PDFs:", e)
        pdf_files = []

    for pdf in pdf_files:
            logging.info(f"Processing: {pdf}")
            pdf_bytes = BytesIO()

            try:
                """Try to load pdf to memory without downloading to
                local system
                """
                s3.download_fileobj(BUCKET_NAME, pdf, pdf_bytes)
                print("PDF loaded into memory!")
            except Exception as e:
                print("Error fetching:", e)
                continue
            pdf_bytes.seek(0)
            reader = PdfReader(pdf_bytes)
            print(f"Total pages in the PDF: {len(reader.pages)}")

            # extract content of all pages in resume
            text = ""
            for page in reader.pages:
                extracted = page.extract_text()
                if extracted:
                    text += extracted + "\n"


            prompt = f"""
            You will receive extracted resume text.

            TASK:
            1. Read ONLY the first 80 lines of the resume text and extract:
            - Candidate full name
            - Candidate email have @ search and find word with @, it is the email
            2. Read ONLY the "Experience" and "Projects" sections and guess the most suitable job role.
            Pick one from this list ONLY:
            ["Data Analyst", "Data Engineer", "ML Engineer", "AI Engineer",
            "Backend Developer", "Full Stack Developer", "Frontend Developer",
            "DevOps Engineer", "Cloud Engineer", "Business Analyst",
            "UI/UX Designer", "HR", "Software Engineer"]

            3. Give a suitability score from 0 to 100.

            4. OUTPUT MUST BE VALID JSON ONLY, NOTHING ELSE.
            JSON fields:
            "name", "email", "role", "score"

            RULES:
            - DO NOT invent name or email.
            - DO NOT return placeholder values.
            - If name/email is missing, set it to null.
            - Be concise.
            - No explanation, no markdown, no text outside JSON.

            RESUME TEXT STARTS BELOW:
            ---------------------------------------
            {text}
            ---------------------------------------
            """


            response = ollama.chat(
                model="llama3",
                messages=[{"role": "user", "content": prompt}]
            )

            # extract LLM output
            logging.info(response['message']['content'])


with DAG(
    dag_id='Resume_Parser',
    default_args=default_args,
    description='Resume parses hourly hourly',
    schedule_interval='@hourly',
    start_date=datetime(2025, 11, 21),
    catchup=False,
) as dag:
    # calling process_resumes
    process_resumes = PythonOperator(
        task_id='process_resumes',
        python_callable=process_resumes
    )
    process_resumes

# def name(text):
#     section = text.split("Education")[0]
#     for line in section.split("\n"):
#         line = line.strip()
#         if line:
#             name = line
#             break
#     print(f"Name is {name}")
#     match = re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", section)
#     if match:
#         print(f"Email is {match.group(0)}")
#     return None


# def education(text):
#     sectio = text.split("Experience")[0].split("Education")[1]
#     for line in sectio.split("\n"):
#         # line = line.strip()
#         if line:
#             name = line 
#             break
#     print(f"University is {name}")